%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_job_manager module
%%%
%%% These tests directly test the gen_server callbacks without any mocking.
%%% Tests focus on testing the callback functions with controlled state,
%%% verifying state transformations and return values.
%%%
%%% Note: Many code paths in flurm_job_manager require external services
%%% (flurm_db_persist, flurm_license, flurm_limits, flurm_scheduler, etc.).
%%% This pure test module tests internal helper functions and state
%%% transformations that can be exercised without external dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% We need to define the state record to match flurm_job_manager
-record(state, {
    jobs = #{} :: #{job_id() => #job{}},
    job_counter = 1 :: pos_integer(),
    persistence_mode = none :: ra | ets | none
}).

%% Note: array_task record removed to avoid unused record warning
%% If needed in future, define inline where used

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a minimal test state
make_test_state() ->
    #state{
        jobs = #{},
        job_counter = 1,
        persistence_mode = none
    }.

%% Create a test state with some pre-existing jobs
make_state_with_jobs() ->
    Job1 = #job{
        id = 1,
        name = <<"test_job_1">>,
        user = <<"user1">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },
    Job2 = #job{
        id = 2,
        name = <<"test_job_2">>,
        user = <<"user2">>,
        partition = <<"compute">>,
        state = running,
        script = <<"#!/bin/bash\necho run">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 7200,
        priority = 200,
        submit_time = erlang:system_time(second) - 1000,
        start_time = erlang:system_time(second) - 500,
        allocated_nodes = [<<"node1">>, <<"node2">>]
    },
    Job3 = #job{
        id = 3,
        name = <<"test_job_3">>,
        user = <<"user1">>,
        partition = <<"default">>,
        state = held,
        script = <<"#!/bin/bash\necho held">>,
        num_nodes = 1,
        num_cpus = 2,
        memory_mb = 512,
        time_limit = 1800,
        priority = 50,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },
    Job4 = #job{
        id = 4,
        name = <<"completed_job">>,
        user = <<"user3">>,
        partition = <<"default">>,
        state = completed,
        script = <<"#!/bin/bash\necho done">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 100,
        submit_time = erlang:system_time(second) - 2000,
        start_time = erlang:system_time(second) - 1900,
        end_time = erlang:system_time(second) - 1800,
        allocated_nodes = [],
        exit_code = 0
    },
    #state{
        jobs = #{
            1 => Job1,
            2 => Job2,
            3 => Job3,
            4 => Job4
        },
        job_counter = 5,
        persistence_mode = none
    }.

%%====================================================================
%% handle_call/3 Tests - get_job
%%====================================================================

handle_call_get_job_existing_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"get existing job returns job record",
           fun() ->
               {reply, {ok, Job}, NewState} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual(1, Job#job.id),
               ?assertEqual(<<"test_job_1">>, Job#job.name),
               ?assertEqual(<<"user1">>, Job#job.user),
               ?assertEqual(pending, Job#job.state),
               ?assertEqual(State, NewState)
           end},
          {"get job 2 returns correct data",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 2}, {self(), make_ref()}, State),
               ?assertEqual(2, Job#job.id),
               ?assertEqual(<<"test_job_2">>, Job#job.name),
               ?assertEqual(running, Job#job.state),
               ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes)
           end}
         ]
     end}.

handle_call_get_job_not_found_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"get non-existent job returns not_found",
           fun() ->
               {reply, Result, NewState} =
                   flurm_job_manager:handle_call({get_job, 999}, {self(), make_ref()}, State),
               ?assertEqual({error, not_found}, Result),
               ?assertEqual(State, NewState)
           end},
          {"get job with zero ID returns not_found",
           fun() ->
               {reply, Result, _} =
                   flurm_job_manager:handle_call({get_job, 0}, {self(), make_ref()}, State),
               ?assertEqual({error, not_found}, Result)
           end}
         ]
     end}.

%%====================================================================
%% handle_call/3 Tests - list_jobs
%%====================================================================

handle_call_list_jobs_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"list_jobs returns all jobs",
           fun() ->
               {reply, Jobs, NewState} =
                   flurm_job_manager:handle_call(list_jobs, {self(), make_ref()}, State),
               ?assertEqual(4, length(Jobs)),
               ?assertEqual(State, NewState),
               JobIds = [J#job.id || J <- Jobs],
               ?assert(lists:member(1, JobIds)),
               ?assert(lists:member(2, JobIds)),
               ?assert(lists:member(3, JobIds)),
               ?assert(lists:member(4, JobIds))
           end}
         ]
     end}.

handle_call_list_jobs_empty_test_() ->
    {setup,
     fun make_test_state/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"list_jobs on empty state returns empty list",
           fun() ->
               {reply, Jobs, NewState} =
                   flurm_job_manager:handle_call(list_jobs, {self(), make_ref()}, State),
               ?assertEqual([], Jobs),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% handle_call/3 Tests - unknown_request
%%====================================================================

handle_call_unknown_request_test_() ->
    {setup,
     fun make_test_state/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"unknown request returns error",
           fun() ->
               {reply, Result, NewState} =
                   flurm_job_manager:handle_call(unknown_request, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Result),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary tuple returns error",
           fun() ->
               {reply, Result, _} =
                   flurm_job_manager:handle_call({some, complex, request}, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Result)
           end},
          {"atom returns error",
           fun() ->
               {reply, Result, _} =
                   flurm_job_manager:handle_call(random_atom, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Result)
           end}
         ]
     end}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_unknown_test_() ->
    {setup,
     fun make_test_state/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"unknown cast is ignored and state unchanged",
           fun() ->
               {noreply, NewState} = flurm_job_manager:handle_cast(unknown_cast, State),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary message cast is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_manager:handle_cast({some, data, 123}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_unknown_test_() ->
    {setup,
     fun make_test_state/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"unknown info message is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_manager:handle_info(unknown_info, State),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary info message is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_manager:handle_info({random, message}, State),
               ?assertEqual(State, NewState)
           end},
          {"timeout message is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_manager:handle_info(timeout, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {setup,
     fun make_test_state/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"terminate returns ok for normal reason",
           fun() ->
               Result = flurm_job_manager:terminate(normal, State),
               ?assertEqual(ok, Result)
           end},
          {"terminate returns ok for shutdown reason",
           fun() ->
               Result = flurm_job_manager:terminate(shutdown, State),
               ?assertEqual(ok, Result)
           end},
          {"terminate returns ok for error reason",
           fun() ->
               Result = flurm_job_manager:terminate({error, some_error}, State),
               ?assertEqual(ok, Result)
           end},
          {"terminate returns ok for killed reason",
           fun() ->
               Result = flurm_job_manager:terminate(killed, State),
               ?assertEqual(ok, Result)
           end}
         ]
     end}.

terminate_with_jobs_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"terminate with jobs in state returns ok",
           fun() ->
               Result = flurm_job_manager:terminate(normal, State),
               ?assertEqual(ok, Result)
           end}
         ]
     end}.

%%====================================================================
%% State Record Tests
%%====================================================================

state_record_test_() ->
    [
     {"empty state has expected defaults",
      fun() ->
          State = #state{},
          ?assertEqual(#{}, State#state.jobs),
          ?assertEqual(1, State#state.job_counter),
          ?assertEqual(none, State#state.persistence_mode)
      end},
     {"state with custom values",
      fun() ->
          State = #state{
              jobs = #{1 => #job{id = 1, name = <<"test">>}},
              job_counter = 100,
              persistence_mode = ets
          },
          ?assertEqual(1, maps:size(State#state.jobs)),
          ?assertEqual(100, State#state.job_counter),
          ?assertEqual(ets, State#state.persistence_mode)
      end}
    ].

%%====================================================================
%% Job Record Tests
%%====================================================================

job_record_test_() ->
    [
     {"job record defaults",
      fun() ->
          Job = #job{
              id = 1,
              name = <<"test">>,
              user = <<"user">>,
              partition = <<"default">>,
              state = pending,
              script = <<>>,
              num_nodes = 1,
              num_cpus = 1,
              memory_mb = 1024,
              time_limit = 3600,
              priority = 100,
              submit_time = 0,
              allocated_nodes = []
          },
          ?assertEqual(1, Job#job.id),
          ?assertEqual(<<"test">>, Job#job.name),
          ?assertEqual(pending, Job#job.state),
          ?assertEqual(<<"/tmp">>, Job#job.work_dir),
          ?assertEqual(<<>>, Job#job.std_err),
          ?assertEqual(<<>>, Job#job.account),
          ?assertEqual(<<"normal">>, Job#job.qos),
          ?assertEqual([], Job#job.licenses)
      end}
    ].

%%====================================================================
%% Edge Cases - Large Job IDs
%%====================================================================

edge_case_large_job_ids_test_() ->
    {setup,
     fun() ->
         LargeJob = #job{
             id = 999999999,
             name = <<"large_id_job">>,
             user = <<"user">>,
             partition = <<"default">>,
             state = pending,
             script = <<>>,
             num_nodes = 1,
             num_cpus = 1,
             memory_mb = 1024,
             time_limit = 3600,
             priority = 100,
             submit_time = erlang:system_time(second),
             allocated_nodes = []
         },
         #state{
             jobs = #{999999999 => LargeJob},
             job_counter = 1000000000,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"can retrieve job with large ID",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 999999999}, {self(), make_ref()}, State),
               ?assertEqual(999999999, Job#job.id)
           end}
         ]
     end}.

%%====================================================================
%% Edge Cases - Many Jobs
%%====================================================================

edge_case_many_jobs_test_() ->
    {setup,
     fun() ->
         Jobs = lists:foldl(
             fun(N, Acc) ->
                 Job = #job{
                     id = N,
                     name = iolist_to_binary([<<"job_">>, integer_to_binary(N)]),
                     user = <<"user">>,
                     partition = <<"default">>,
                     state = pending,
                     script = <<>>,
                     num_nodes = 1,
                     num_cpus = 1,
                     memory_mb = 1024,
                     time_limit = 3600,
                     priority = 100,
                     submit_time = erlang:system_time(second),
                     allocated_nodes = []
                 },
                 maps:put(N, Job, Acc)
             end,
             #{},
             lists:seq(1, 100)
         ),
         #state{
             jobs = Jobs,
             job_counter = 101,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"list_jobs returns all 100 jobs",
           fun() ->
               {reply, Jobs, _} =
                   flurm_job_manager:handle_call(list_jobs, {self(), make_ref()}, State),
               ?assertEqual(100, length(Jobs))
           end},
          {"can get specific job from large set",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 50}, {self(), make_ref()}, State),
               ?assertEqual(50, Job#job.id),
               ?assertEqual(<<"job_50">>, Job#job.name)
           end}
         ]
     end}.

%%====================================================================
%% Edge Cases - Jobs in Various States
%%====================================================================

jobs_various_states_test_() ->
    {setup,
     fun() ->
         States = [pending, held, configuring, running, completing,
                   completed, cancelled, failed, timeout, node_fail, requeued],
         Jobs = lists:foldl(
             fun({N, JobState}, Acc) ->
                 Job = #job{
                     id = N,
                     name = iolist_to_binary([<<"job_">>, atom_to_binary(JobState, utf8)]),
                     user = <<"user">>,
                     partition = <<"default">>,
                     state = JobState,
                     script = <<>>,
                     num_nodes = 1,
                     num_cpus = 1,
                     memory_mb = 1024,
                     time_limit = 3600,
                     priority = 100,
                     submit_time = erlang:system_time(second),
                     allocated_nodes = []
                 },
                 maps:put(N, Job, Acc)
             end,
             #{},
             lists:zip(lists:seq(1, length(States)), States)
         ),
         #state{
             jobs = Jobs,
             job_counter = length(States) + 1,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"list returns jobs in all states",
           fun() ->
               {reply, Jobs, _} =
                   flurm_job_manager:handle_call(list_jobs, {self(), make_ref()}, State),
               ?assertEqual(11, length(Jobs)),
               JobStates = [J#job.state || J <- Jobs],
               ?assert(lists:member(pending, JobStates)),
               ?assert(lists:member(running, JobStates)),
               ?assert(lists:member(completed, JobStates)),
               ?assert(lists:member(failed, JobStates))
           end},
          {"can get pending job",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual(pending, Job#job.state)
           end},
          {"can get running job",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 4}, {self(), make_ref()}, State),
               ?assertEqual(running, Job#job.state)
           end}
         ]
     end}.

%%====================================================================
%% Persistence Mode Tests
%%====================================================================

persistence_mode_state_test_() ->
    [
     {"state with ra persistence mode",
      fun() ->
          State = #state{persistence_mode = ra},
          ?assertEqual(ra, State#state.persistence_mode)
      end},
     {"state with ets persistence mode",
      fun() ->
          State = #state{persistence_mode = ets},
          ?assertEqual(ets, State#state.persistence_mode)
      end},
     {"state with none persistence mode",
      fun() ->
          State = #state{persistence_mode = none},
          ?assertEqual(none, State#state.persistence_mode)
      end}
    ].

%%====================================================================
%% Job Counter Tests
%%====================================================================

job_counter_test_() ->
    [
     {"job counter starts at 1",
      fun() ->
          State = make_test_state(),
          ?assertEqual(1, State#state.job_counter)
      end},
     {"state with jobs has correct counter",
      fun() ->
          State = make_state_with_jobs(),
          ?assertEqual(5, State#state.job_counter)
      end}
    ].

%%====================================================================
%% From parameter Tests
%%====================================================================

from_parameter_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"handle_call works with valid from tuple",
           fun() ->
               From = {self(), make_ref()},
               {reply, {ok, _Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, From, State)
           end},
          {"handle_call works with different pids",
           fun() ->
               %% Simulate call from different process
               Pid = spawn(fun() -> receive stop -> ok end end),
               From = {Pid, make_ref()},
               {reply, {ok, _Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, From, State),
               Pid ! stop
           end}
         ]
     end}.

%%====================================================================
%% Jobs Map Operations Tests
%%====================================================================

jobs_map_operations_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"jobs map size is correct",
           fun() ->
               ?assertEqual(4, maps:size(State#state.jobs))
           end},
          {"jobs map keys are correct",
           fun() ->
               Keys = maps:keys(State#state.jobs),
               ?assertEqual([1, 2, 3, 4], lists:sort(Keys))
           end},
          {"jobs map values are job records",
           fun() ->
               Values = maps:values(State#state.jobs),
               lists:foreach(fun(V) ->
                   ?assert(is_record(V, job))
               end, Values)
           end}
         ]
     end}.

%%====================================================================
%% Job Field Validation Tests
%%====================================================================

job_field_validation_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"job has valid name binary",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assert(is_binary(Job#job.name))
           end},
          {"job has valid user binary",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assert(is_binary(Job#job.user))
           end},
          {"job has valid partition binary",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assert(is_binary(Job#job.partition))
           end},
          {"job has valid state atom",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assert(is_atom(Job#job.state))
           end},
          {"job has positive integer id",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assert(is_integer(Job#job.id)),
               ?assert(Job#job.id > 0)
           end}
         ]
     end}.

%%====================================================================
%% Running Job with Allocated Nodes Tests
%%====================================================================

running_job_allocated_nodes_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"running job has allocated nodes",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 2}, {self(), make_ref()}, State),
               ?assertEqual(running, Job#job.state),
               ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes),
               ?assert(Job#job.start_time =/= undefined)
           end},
          {"pending job has no allocated nodes",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual(pending, Job#job.state),
               ?assertEqual([], Job#job.allocated_nodes),
               ?assertEqual(undefined, Job#job.start_time)
           end}
         ]
     end}.

%%====================================================================
%% Completed Job Tests
%%====================================================================

completed_job_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"completed job has end_time and exit_code",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 4}, {self(), make_ref()}, State),
               ?assertEqual(completed, Job#job.state),
               ?assert(Job#job.end_time =/= undefined),
               ?assertEqual(0, Job#job.exit_code)
           end}
         ]
     end}.

%%====================================================================
%% Held Job Tests
%%====================================================================

held_job_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"held job can be retrieved",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 3}, {self(), make_ref()}, State),
               ?assertEqual(held, Job#job.state),
               ?assertEqual(<<"test_job_3">>, Job#job.name)
           end}
         ]
     end}.

%%====================================================================
%% Concurrent Access Simulation Tests
%%====================================================================

concurrent_access_test_() ->
    {setup,
     fun make_state_with_jobs/0,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"multiple get calls return consistent data",
           fun() ->
               %% Simulate multiple concurrent reads
               Results = [
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
                   flurm_job_manager:handle_call({get_job, 2}, {self(), make_ref()}, State),
                   flurm_job_manager:handle_call({get_job, 3}, {self(), make_ref()}, State),
                   flurm_job_manager:handle_call({get_job, 4}, {self(), make_ref()}, State)
               ],
               lists:foreach(fun({reply, {ok, _Job}, _}) -> ok end, Results)
           end}
         ]
     end}.

%%====================================================================
%% Binary Data in Jobs Tests
%%====================================================================

binary_data_test_() ->
    {setup,
     fun() ->
         Job = #job{
             id = 1,
             name = <<"job_with_unicode_\x{00E9}\x{00E0}">>,
             user = <<"user\x{00E9}">>,
             partition = <<"partition_\x{00E0}">>,
             state = pending,
             script = <<"#!/bin/bash\necho 'Hello \x{00E9}'\n">>,
             num_nodes = 1,
             num_cpus = 1,
             memory_mb = 1024,
             time_limit = 3600,
             priority = 100,
             submit_time = erlang:system_time(second),
             allocated_nodes = [],
             work_dir = <<"/tmp/test_\x{00E9}">>,
             std_out = <<"/tmp/out_\x{00E0}.log">>,
             account = <<"account_\x{00E9}">>,
             qos = <<"qos_\x{00E0}">>
         },
         #state{
             jobs = #{1 => Job},
             job_counter = 2,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"job with unicode data can be retrieved",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assert(is_binary(Job#job.name)),
               ?assert(is_binary(Job#job.script)),
               ?assert(is_binary(Job#job.work_dir))
           end}
         ]
     end}.

%%====================================================================
%% Empty Script Job Tests
%%====================================================================

empty_script_test_() ->
    {setup,
     fun() ->
         Job = #job{
             id = 1,
             name = <<"empty_script_job">>,
             user = <<"user">>,
             partition = <<"default">>,
             state = pending,
             script = <<>>,
             num_nodes = 1,
             num_cpus = 1,
             memory_mb = 1024,
             time_limit = 3600,
             priority = 100,
             submit_time = erlang:system_time(second),
             allocated_nodes = []
         },
         #state{
             jobs = #{1 => Job},
             job_counter = 2,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"job with empty script can be retrieved",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual(<<>>, Job#job.script)
           end}
         ]
     end}.

%%====================================================================
%% Job with Licenses Tests
%%====================================================================

job_with_licenses_test_() ->
    {setup,
     fun() ->
         Job = #job{
             id = 1,
             name = <<"licensed_job">>,
             user = <<"user">>,
             partition = <<"default">>,
             state = pending,
             script = <<"#!/bin/bash\nmatlab -batch 'exit'">>,
             num_nodes = 1,
             num_cpus = 4,
             memory_mb = 8192,
             time_limit = 7200,
             priority = 100,
             submit_time = erlang:system_time(second),
             allocated_nodes = [],
             licenses = [{<<"matlab">>, 1}, {<<"stata">>, 2}]
         },
         #state{
             jobs = #{1 => Job},
             job_counter = 2,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"job with licenses has correct license list",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual([{<<"matlab">>, 1}, {<<"stata">>, 2}], Job#job.licenses)
           end}
         ]
     end}.

%%====================================================================
%% Job with GRES Tests
%%====================================================================

job_with_gres_test_() ->
    {setup,
     fun() ->
         Job = #job{
             id = 1,
             name = <<"gpu_job">>,
             user = <<"user">>,
             partition = <<"gpu">>,
             state = pending,
             script = <<"#!/bin/bash\npython train.py">>,
             num_nodes = 1,
             num_cpus = 8,
             memory_mb = 32768,
             time_limit = 86400,
             priority = 100,
             submit_time = erlang:system_time(second),
             allocated_nodes = [],
             gres = <<"gpu:a100:4">>,
             gres_per_node = <<"gpu:2">>,
             gpu_type = <<"a100">>,
             gpu_memory_mb = 40960,
             gpu_exclusive = true
         },
         #state{
             jobs = #{1 => Job},
             job_counter = 2,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"job with GRES has correct GRES fields",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual(<<"gpu:a100:4">>, Job#job.gres),
               ?assertEqual(<<"gpu:2">>, Job#job.gres_per_node),
               ?assertEqual(<<"a100">>, Job#job.gpu_type),
               ?assertEqual(40960, Job#job.gpu_memory_mb),
               ?assertEqual(true, Job#job.gpu_exclusive)
           end}
         ]
     end}.

%%====================================================================
%% Job with Reservation Tests
%%====================================================================

job_with_reservation_test_() ->
    {setup,
     fun() ->
         Job = #job{
             id = 1,
             name = <<"reserved_job">>,
             user = <<"user">>,
             partition = <<"default">>,
             state = pending,
             script = <<"#!/bin/bash\necho reserved">>,
             num_nodes = 4,
             num_cpus = 32,
             memory_mb = 65536,
             time_limit = 3600,
             priority = 100,
             submit_time = erlang:system_time(second),
             allocated_nodes = [],
             reservation = <<"maintenance_window">>
         },
         #state{
             jobs = #{1 => Job},
             job_counter = 2,
             persistence_mode = none
         }
     end,
     fun(_) -> ok end,
     fun(State) ->
         [
          {"job with reservation has correct reservation field",
           fun() ->
               {reply, {ok, Job}, _} =
                   flurm_job_manager:handle_call({get_job, 1}, {self(), make_ref()}, State),
               ?assertEqual(<<"maintenance_window">>, Job#job.reservation)
           end}
         ]
     end}.
