%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_job_dispatcher_server module
%%%
%%% Tests the job dispatcher server internal functions via the
%%% exported TEST interface. Tests gen_server callbacks directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_server_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Helper Functions (exported via -ifdef(TEST))
%%====================================================================

%% Test build_job_launch_message function
build_job_launch_message_test_() ->
    [
     {"builds message with minimal job info",
      fun() ->
          JobId = 123,
          JobInfo = #{
              allocated_nodes => [<<"node1">>],
              script => <<"#!/bin/bash\necho test">>
          },
          Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),
          ?assertEqual(job_launch, maps:get(type, Msg)),
          Payload = maps:get(payload, Msg),
          ?assertEqual(123, maps:get(<<"job_id">>, Payload)),
          ?assertEqual(<<"#!/bin/bash\necho test">>, maps:get(<<"script">>, Payload))
      end},
     {"builds message with full job info",
      fun() ->
          JobId = 456,
          JobInfo = #{
              allocated_nodes => [<<"node1">>, <<"node2">>],
              script => <<"#!/bin/bash\necho full">>,
              name => <<"test_job">>,
              num_tasks => 4,
              num_cpus => 8,
              partition => <<"compute">>,
              work_dir => <<"/home/user/work">>,
              memory_mb => 16384,
              time_limit => 7200,
              user_id => 1000,
              group_id => 1000,
              std_out => <<"/tmp/job.out">>,
              std_err => <<"/tmp/job.err">>,
              prolog => <<"/etc/slurm/prolog.sh">>,
              epilog => <<"/etc/slurm/epilog.sh">>
          },
          Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),
          Payload = maps:get(payload, Msg),
          ?assertEqual(456, maps:get(<<"job_id">>, Payload)),
          ?assertEqual(8, maps:get(<<"num_cpus">>, Payload)),
          ?assertEqual(16384, maps:get(<<"memory_mb">>, Payload)),
          ?assertEqual(7200, maps:get(<<"time_limit">>, Payload)),
          ?assertEqual(<<"/tmp/job.out">>, maps:get(<<"std_out">>, Payload)),
          ?assertEqual(<<"/tmp/job.err">>, maps:get(<<"std_err">>, Payload)),
          %% Check environment includes SLURM vars
          Env = maps:get(<<"environment">>, Payload),
          ?assertEqual(<<"456">>, maps:get(<<"SLURM_JOB_ID">>, Env))
      end},
     {"uses default work_dir when not specified",
      fun() ->
          Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
          Payload = maps:get(payload, Msg),
          ?assertEqual(<<"/tmp">>, maps:get(<<"working_dir">>, Payload))
      end},
     {"generates default output file when std_out is empty",
      fun() ->
          JobInfo = #{work_dir => <<"/work">>},
          Msg = flurm_job_dispatcher_server:build_job_launch_message(42, JobInfo),
          Payload = maps:get(payload, Msg),
          ?assertEqual(<<"/work/slurm-42.out">>, maps:get(<<"std_out">>, Payload))
      end}
    ].

%% Test signal_to_name function
signal_to_name_test_() ->
    [
     {"sigterm converts to SIGTERM",
      ?_assertEqual(<<"SIGTERM">>, flurm_job_dispatcher_server:signal_to_name(sigterm))},
     {"sigkill converts to SIGKILL",
      ?_assertEqual(<<"SIGKILL">>, flurm_job_dispatcher_server:signal_to_name(sigkill))},
     {"sigstop converts to SIGSTOP",
      ?_assertEqual(<<"SIGSTOP">>, flurm_job_dispatcher_server:signal_to_name(sigstop))},
     {"sigcont converts to SIGCONT",
      ?_assertEqual(<<"SIGCONT">>, flurm_job_dispatcher_server:signal_to_name(sigcont))},
     {"sighup converts to SIGHUP",
      ?_assertEqual(<<"SIGHUP">>, flurm_job_dispatcher_server:signal_to_name(sighup))},
     {"sigusr1 converts to SIGUSR1",
      ?_assertEqual(<<"SIGUSR1">>, flurm_job_dispatcher_server:signal_to_name(sigusr1))},
     {"sigusr2 converts to SIGUSR2",
      ?_assertEqual(<<"SIGUSR2">>, flurm_job_dispatcher_server:signal_to_name(sigusr2))},
     {"sigint converts to SIGINT",
      ?_assertEqual(<<"SIGINT">>, flurm_job_dispatcher_server:signal_to_name(sigint))},
     {"numeric signal stays numeric",
      ?_assertEqual(<<"15">>, flurm_job_dispatcher_server:signal_to_name(15))},
     {"unknown atom converts to uppercase",
      ?_assertEqual(<<"UNKNOWN">>, flurm_job_dispatcher_server:signal_to_name(unknown))}
    ].

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

%% Test init callback
init_test_() ->
    [
     {"init returns ok with initial state",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          ?assert(is_tuple(State))
      end}
    ].

%% Test handle_call callbacks
handle_call_test_() ->
    [
     {"unknown request returns error",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          {reply, Reply, _NewState} = flurm_job_dispatcher_server:handle_call(unknown_request, {self(), make_ref()}, State),
          ?assertEqual({error, unknown_request}, Reply)
      end},
     {"dispatch_job with no nodes returns error",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          JobInfo = #{allocated_nodes => []},
          {reply, Reply, _NewState} = flurm_job_dispatcher_server:handle_call({dispatch_job, 1, JobInfo}, {self(), make_ref()}, State),
          ?assertEqual({error, no_nodes}, Reply)
      end}
    ].

%% Test handle_cast callbacks
handle_cast_test_() ->
    [
     %% Note: cancel_job test removed because it requires flurm_node_connection_manager
     %% to be running, which is an external dependency
     {"requeue_job removes job from tracking",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          State1 = setelement(2, State, #{42 => [<<"node1">>]}),
          {noreply, NewState} = flurm_job_dispatcher_server:handle_cast({requeue_job, 42}, State1),
          DispatchedJobs = element(2, NewState),
          ?assertNot(maps:is_key(42, DispatchedJobs))
      end},
     {"unknown cast is ignored",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          {noreply, NewState} = flurm_job_dispatcher_server:handle_cast(unknown_cast, State),
          ?assertEqual(State, NewState)
      end}
    ].

%% Test handle_info callback
handle_info_test_() ->
    [
     {"unknown info is ignored",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          {noreply, NewState} = flurm_job_dispatcher_server:handle_info(unknown_info, State),
          ?assertEqual(State, NewState)
      end}
    ].

%% Test terminate callback
terminate_test_() ->
    [
     {"terminate returns ok",
      fun() ->
          {ok, State} = flurm_job_dispatcher_server:init([]),
          ?assertEqual(ok, flurm_job_dispatcher_server:terminate(normal, State))
      end}
    ].

%%====================================================================
%% API Function Export Tests
%%====================================================================

api_exports_test_() ->
    [
     {"start_link/0 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({start_link, 0}, Exports))
      end},
     {"dispatch_job/2 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({dispatch_job, 2}, Exports))
      end},
     {"cancel_job/2 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({cancel_job, 2}, Exports))
      end},
     {"signal_job/3 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({signal_job, 3}, Exports))
      end},
     {"preempt_job/2 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({preempt_job, 2}, Exports))
      end},
     {"requeue_job/1 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({requeue_job, 1}, Exports))
      end},
     {"drain_node/1 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({drain_node, 1}, Exports))
      end},
     {"resume_node/1 is exported",
      fun() ->
          Exports = flurm_job_dispatcher_server:module_info(exports),
          ?assert(lists:member({resume_node, 1}, Exports))
      end}
    ].
