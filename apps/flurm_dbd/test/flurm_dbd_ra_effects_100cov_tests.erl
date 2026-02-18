%%%-------------------------------------------------------------------
%%% @doc FLURM DBD Ra Effects 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_dbd_ra_effects module covering all
%%% exported functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_effects_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Mock lager
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Job Recorded Tests
%%====================================================================

job_recorded_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_recorded logs and returns ok", fun() ->
                 LogCalled = ets:new(log_called, [public]),
                 meck:expect(lager, debug, fun(Fmt, Args) ->
                     ets:insert(LogCalled, {called, {Fmt, Args}}),
                     ok
                 end),
                 JobRecord = {job_record, 123, <<"test">>},
                 Result = flurm_dbd_ra_effects:job_recorded(JobRecord),
                 ?assertEqual(ok, Result),
                 %% Verify logging was called
                 [{called, {_, _}}] = ets:lookup(LogCalled, called),
                 ets:delete(LogCalled)
             end},
             {"job_recorded handles various record types", fun() ->
                 Result1 = flurm_dbd_ra_effects:job_recorded(#{job_id => 1}),
                 ?assertEqual(ok, Result1),
                 Result2 = flurm_dbd_ra_effects:job_recorded({1, <<"name">>, <<"user">>}),
                 ?assertEqual(ok, Result2),
                 Result3 = flurm_dbd_ra_effects:job_recorded(simple_atom),
                 ?assertEqual(ok, Result3)
             end}
         ]
     end
    }.

%%====================================================================
%% Became Leader Tests
%%====================================================================

became_leader_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"became_leader logs and returns ok", fun() ->
                 LogCalled = ets:new(log_called, [public]),
                 meck:expect(lager, info, fun(Fmt, Args) ->
                     ets:insert(LogCalled, {called, {Fmt, Args}}),
                     ok
                 end),
                 Result = flurm_dbd_ra_effects:became_leader(node()),
                 ?assertEqual(ok, Result),
                 [{called, {_, [NodeVal]}}] = ets:lookup(LogCalled, called),
                 ?assertEqual(node(), NodeVal),
                 ets:delete(LogCalled)
             end},
             {"became_leader handles different node names", fun() ->
                 Result1 = flurm_dbd_ra_effects:became_leader('node1@localhost'),
                 ?assertEqual(ok, Result1),
                 Result2 = flurm_dbd_ra_effects:became_leader('flurm@192.168.1.100'),
                 ?assertEqual(ok, Result2)
             end}
         ]
     end
    }.

%%====================================================================
%% Became Follower Tests
%%====================================================================

became_follower_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"became_follower logs and returns ok", fun() ->
                 LogCalled = ets:new(log_called, [public]),
                 meck:expect(lager, debug, fun(Fmt, Args) ->
                     ets:insert(LogCalled, {called, {Fmt, Args}}),
                     ok
                 end),
                 Result = flurm_dbd_ra_effects:became_follower(node()),
                 ?assertEqual(ok, Result),
                 [{called, {_, [NodeVal]}}] = ets:lookup(LogCalled, called),
                 ?assertEqual(node(), NodeVal),
                 ets:delete(LogCalled)
             end},
             {"became_follower handles different node names", fun() ->
                 Result1 = flurm_dbd_ra_effects:became_follower('node1@localhost'),
                 ?assertEqual(ok, Result1),
                 Result2 = flurm_dbd_ra_effects:became_follower('dbd@10.0.0.1'),
                 ?assertEqual(ok, Result2)
             end}
         ]
     end
    }.

%%====================================================================
%% Integration Style Tests
%%====================================================================

effect_sequence_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"effects can be called in sequence", fun() ->
                 %% Simulate a leader election followed by job recording
                 ok = flurm_dbd_ra_effects:became_leader(node()),
                 ok = flurm_dbd_ra_effects:job_recorded(#{job_id => 1}),
                 ok = flurm_dbd_ra_effects:job_recorded(#{job_id => 2}),
                 ok = flurm_dbd_ra_effects:became_follower(node()),
                 ?assert(true)
             end},
             {"effects handle rapid state changes", fun() ->
                 %% Rapid leader/follower transitions
                 ok = flurm_dbd_ra_effects:became_leader(node()),
                 ok = flurm_dbd_ra_effects:became_follower(node()),
                 ok = flurm_dbd_ra_effects:became_leader(node()),
                 ok = flurm_dbd_ra_effects:became_follower(node()),
                 ?assert(true)
             end}
         ]
     end
    }.
