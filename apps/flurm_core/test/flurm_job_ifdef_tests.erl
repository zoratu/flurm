%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure internal functions that don't require
%%% a running gen_statem process.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

make_test_job_data() ->
    make_test_job_data(#{}).

make_test_job_data(Overrides) ->
    Defaults = #{
        job_id => 12345,
        user_id => <<"testuser">>,
        group_id => <<"testgroup">>,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        submit_time => {1704, 67200, 0},
        start_time => {1704, 67260, 0},
        end_time => undefined,
        exit_code => undefined,
        priority => 100,
        state_version => ?JOB_STATE_VERSION
    },
    Merged = maps:merge(Defaults, Overrides),
    #job_data{
        job_id = maps:get(job_id, Merged),
        user_id = maps:get(user_id, Merged),
        group_id = maps:get(group_id, Merged),
        partition = maps:get(partition, Merged),
        num_nodes = maps:get(num_nodes, Merged),
        num_cpus = maps:get(num_cpus, Merged),
        time_limit = maps:get(time_limit, Merged),
        script = maps:get(script, Merged),
        allocated_nodes = maps:get(allocated_nodes, Merged),
        submit_time = maps:get(submit_time, Merged),
        start_time = maps:get(start_time, Merged),
        end_time = maps:get(end_time, Merged),
        exit_code = maps:get(exit_code, Merged),
        priority = maps:get(priority, Merged),
        state_version = maps:get(state_version, Merged)
    }.

%%====================================================================
%% build_job_info/2 Tests
%%====================================================================

build_job_info_test_() ->
    {"build_job_info/2 tests", [
        {"builds info map with all fields for pending job",
         fun() ->
             Data = make_test_job_data(),
             Info = flurm_job:build_job_info(pending, Data),

             ?assertEqual(12345, maps:get(job_id, Info)),
             ?assertEqual(<<"testuser">>, maps:get(user_id, Info)),
             ?assertEqual(<<"testgroup">>, maps:get(group_id, Info)),
             ?assertEqual(<<"compute">>, maps:get(partition, Info)),
             ?assertEqual(pending, maps:get(state, Info)),
             ?assertEqual(2, maps:get(num_nodes, Info)),
             ?assertEqual(8, maps:get(num_cpus, Info)),
             ?assertEqual(3600, maps:get(time_limit, Info)),
             ?assertEqual(<<"#!/bin/bash\necho hello">>, maps:get(script, Info)),
             ?assertEqual([<<"node1">>, <<"node2">>], maps:get(allocated_nodes, Info)),
             ?assertEqual(100, maps:get(priority, Info))
         end},

        {"builds info map for running job",
         fun() ->
             Data = make_test_job_data(),
             Info = flurm_job:build_job_info(running, Data),
             ?assertEqual(running, maps:get(state, Info))
         end},

        {"builds info map for completed job with exit code",
         fun() ->
             Data = make_test_job_data(#{
                 exit_code => 0,
                 end_time => {1704, 70800, 0}
             }),
             Info = flurm_job:build_job_info(completed, Data),

             ?assertEqual(completed, maps:get(state, Info)),
             ?assertEqual(0, maps:get(exit_code, Info)),
             ?assertEqual({1704, 70800, 0}, maps:get(end_time, Info))
         end},

        {"builds info map for failed job with non-zero exit code",
         fun() ->
             Data = make_test_job_data(#{
                 exit_code => 1,
                 end_time => {1704, 70800, 0}
             }),
             Info = flurm_job:build_job_info(failed, Data),

             ?assertEqual(failed, maps:get(state, Info)),
             ?assertEqual(1, maps:get(exit_code, Info))
         end},

        {"preserves all timestamps",
         fun() ->
             SubmitTime = {1704, 67200, 0},
             StartTime = {1704, 67260, 0},
             EndTime = {1704, 70800, 0},
             Data = make_test_job_data(#{
                 submit_time => SubmitTime,
                 start_time => StartTime,
                 end_time => EndTime
             }),
             Info = flurm_job:build_job_info(completed, Data),

             ?assertEqual(SubmitTime, maps:get(submit_time, Info)),
             ?assertEqual(StartTime, maps:get(start_time, Info)),
             ?assertEqual(EndTime, maps:get(end_time, Info))
         end},

        {"handles empty allocated nodes",
         fun() ->
             Data = make_test_job_data(#{allocated_nodes => []}),
             Info = flurm_job:build_job_info(pending, Data),
             ?assertEqual([], maps:get(allocated_nodes, Info))
         end},

        {"handles all job states",
         fun() ->
             Data = make_test_job_data(),
             States = [pending, configuring, running, suspended,
                       completing, completed, cancelled, failed,
                       timeout, node_fail],
             lists:foreach(fun(State) ->
                 Info = flurm_job:build_job_info(State, Data),
                 ?assertEqual(State, maps:get(state, Info))
             end, States)
         end}
    ]}.

%%====================================================================
%% maybe_upgrade_state_data/1 Tests
%%====================================================================

maybe_upgrade_state_data_test_() ->
    {"maybe_upgrade_state_data/1 tests", [
        {"returns data unchanged when version matches",
         fun() ->
             Data = make_test_job_data(),
             Result = flurm_job:maybe_upgrade_state_data(Data),
             ?assertEqual(Data, Result)
         end},

        {"upgrades data with older version",
         fun() ->
             %% Create data with older version
             OldData = make_test_job_data(#{state_version => ?JOB_STATE_VERSION - 1}),
             Result = flurm_job:maybe_upgrade_state_data(OldData),
             ?assertEqual(?JOB_STATE_VERSION, Result#job_data.state_version)
         end},

        {"upgrades data from version 0",
         fun() ->
             OldData = make_test_job_data(#{state_version => 0}),
             Result = flurm_job:maybe_upgrade_state_data(OldData),
             ?assertEqual(?JOB_STATE_VERSION, Result#job_data.state_version)
         end},

        {"preserves all other fields during upgrade",
         fun() ->
             OldData = make_test_job_data(#{
                 state_version => ?JOB_STATE_VERSION - 1,
                 job_id => 99999,
                 priority => 500
             }),
             Result = flurm_job:maybe_upgrade_state_data(OldData),

             ?assertEqual(99999, Result#job_data.job_id),
             ?assertEqual(500, Result#job_data.priority),
             ?assertEqual(<<"testuser">>, Result#job_data.user_id)
         end}
    ]}.

%%====================================================================
%% Integration Tests (if applicable)
%%====================================================================

job_info_roundtrip_test_() ->
    {"Job info roundtrip tests", [
        {"job info contains all expected keys",
         fun() ->
             Data = make_test_job_data(),
             Info = flurm_job:build_job_info(running, Data),

             ExpectedKeys = [job_id, user_id, group_id, partition, state,
                            num_nodes, num_cpus, time_limit, script,
                            allocated_nodes, submit_time, start_time,
                            end_time, exit_code, priority],

             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, Info),
                         io_lib:format("Missing key: ~p", [Key]))
             end, ExpectedKeys)
         end},

        {"job info is a proper map",
         fun() ->
             Data = make_test_job_data(),
             Info = flurm_job:build_job_info(pending, Data),
             ?assert(is_map(Info))
         end}
    ]}.
