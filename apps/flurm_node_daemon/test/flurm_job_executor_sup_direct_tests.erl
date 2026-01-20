%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_job_executor_sup module
%%%
%%% Tests the job executor supervisor directly without mocking it.
%%% Child module (flurm_job_executor) is mocked to isolate the
%%% supervisor behavior.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_sup_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

executor_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_init_returns_valid_spec/1,
      fun test_init_has_simple_one_for_one_strategy/1,
      fun test_init_child_spec/1,
      fun test_start_link_registers_name/1,
      fun test_start_job/1,
      fun test_start_multiple_jobs/1,
      fun test_stop_job/1
     ]}.

setup() ->
    %% Mock the job executor to avoid real execution
    meck:new(flurm_job_executor, [non_strict]),
    meck:expect(flurm_job_executor, start_link, fun(JobSpec) ->
        %% Spawn a simple process that acts like an executor
        Pid = spawn(fun() ->
            receive
                stop -> ok
            after 60000 ->
                ok
            end
        end),
        {ok, Pid}
    end),

    %% Start lager
    catch application:start(lager),
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    catch gen_server:stop(flurm_job_executor_sup),
    meck:unload(flurm_job_executor),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_init_returns_valid_spec(_) ->
    {"init/1 returns valid supervisor spec",
     fun() ->
         {ok, {SupFlags, Children}} = flurm_job_executor_sup:init([]),

         ?assert(is_map(SupFlags)),
         ?assert(is_list(Children)),
         ?assertEqual(1, length(Children))
     end}.

test_init_has_simple_one_for_one_strategy(_) ->
    {"init/1 uses simple_one_for_one strategy",
     fun() ->
         {ok, {SupFlags, _Children}} = flurm_job_executor_sup:init([]),

         ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
         ?assertEqual(10, maps:get(intensity, SupFlags)),
         ?assertEqual(60, maps:get(period, SupFlags))
     end}.

test_init_child_spec(_) ->
    {"init/1 defines correct child spec for executor",
     fun() ->
         {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),

         ?assertEqual(flurm_job_executor, maps:get(id, ChildSpec)),
         ?assertEqual({flurm_job_executor, start_link, []}, maps:get(start, ChildSpec)),
         ?assertEqual(temporary, maps:get(restart, ChildSpec)),
         ?assertEqual(30000, maps:get(shutdown, ChildSpec)),
         ?assertEqual(worker, maps:get(type, ChildSpec)),
         ?assertEqual([flurm_job_executor], maps:get(modules, ChildSpec))
     end}.

test_start_link_registers_name(_) ->
    {"start_link/0 registers supervisor with local name",
     fun() ->
         {ok, Pid} = flurm_job_executor_sup:start_link(),

         ?assert(is_pid(Pid)),
         ?assertEqual(Pid, whereis(flurm_job_executor_sup)),

         gen_server:stop(Pid)
     end}.

test_start_job(_) ->
    {"start_job/1 starts a job executor",
     fun() ->
         {ok, SupPid} = flurm_job_executor_sup:start_link(),

         JobSpec = #{
             job_id => 5001,
             script => <<"echo test">>,
             working_dir => <<"/tmp">>,
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, JobPid} = flurm_job_executor_sup:start_job(JobSpec),

         ?assert(is_pid(JobPid)),
         ?assert(is_process_alive(JobPid)),

         %% Verify meck was called
         ?assert(meck:called(flurm_job_executor, start_link, [JobSpec])),

         %% Cleanup
         JobPid ! stop,
         gen_server:stop(SupPid)
     end}.

test_start_multiple_jobs(_) ->
    {"start_job/1 can start multiple job executors",
     fun() ->
         {ok, SupPid} = flurm_job_executor_sup:start_link(),

         JobSpec1 = #{job_id => 5002, script => <<"echo 1">>, working_dir => <<"/tmp">>, num_cpus => 1, memory_mb => 512},
         JobSpec2 = #{job_id => 5003, script => <<"echo 2">>, working_dir => <<"/tmp">>, num_cpus => 1, memory_mb => 512},
         JobSpec3 = #{job_id => 5004, script => <<"echo 3">>, working_dir => <<"/tmp">>, num_cpus => 1, memory_mb => 512},

         {ok, Pid1} = flurm_job_executor_sup:start_job(JobSpec1),
         {ok, Pid2} = flurm_job_executor_sup:start_job(JobSpec2),
         {ok, Pid3} = flurm_job_executor_sup:start_job(JobSpec3),

         ?assert(is_pid(Pid1)),
         ?assert(is_pid(Pid2)),
         ?assert(is_pid(Pid3)),

         %% All should be different pids
         ?assertNotEqual(Pid1, Pid2),
         ?assertNotEqual(Pid2, Pid3),
         ?assertNotEqual(Pid1, Pid3),

         %% Cleanup
         Pid1 ! stop,
         Pid2 ! stop,
         Pid3 ! stop,
         gen_server:stop(SupPid)
     end}.

test_stop_job(_) ->
    {"stop_job/1 terminates a running job executor",
     fun() ->
         {ok, SupPid} = flurm_job_executor_sup:start_link(),

         JobSpec = #{
             job_id => 5005,
             script => <<"sleep 100">>,
             working_dir => <<"/tmp">>,
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, JobPid} = flurm_job_executor_sup:start_job(JobSpec),

         ?assert(is_process_alive(JobPid)),

         %% Stop the job
         ok = flurm_job_executor_sup:stop_job(JobPid),

         %% Should be terminated
         flurm_test_utils:wait_for_death(JobPid),
         ?assertNot(is_process_alive(JobPid)),

         gen_server:stop(SupPid)
     end}.

%%====================================================================
%% Integration tests (with real executor)
%%====================================================================

integration_test_() ->
    {setup,
     fun setup_integration/0,
     fun cleanup_integration/1,
     [
      {"supervisor restarts are configured as temporary", fun test_temporary_restart/0}
     ]}.

setup_integration() ->
    catch application:start(lager),
    ok.

cleanup_integration(_) ->
    catch gen_server:stop(flurm_job_executor_sup),
    ok.

test_temporary_restart() ->
    %% Test that temporary restart means children don't restart
    {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
    ?assertEqual(temporary, maps:get(restart, ChildSpec)).

%%====================================================================
%% Supervisor behavior tests
%%====================================================================

supervisor_behavior_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_job_executor, [non_strict]),
         meck:expect(flurm_job_executor, start_link, fun(_) ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),
         catch application:start(lager),
         ok
     end,
     fun(_) ->
         catch gen_server:stop(flurm_job_executor_sup),
         meck:unload(flurm_job_executor),
         ok
     end,
     [
      {"supervisor counts children correctly", fun test_count_children/0},
      {"which_children returns correct info", fun test_which_children/0}
     ]}.

test_count_children() ->
    {ok, SupPid} = flurm_job_executor_sup:start_link(),

    %% Initially no children
    Counts1 = supervisor:count_children(SupPid),
    ?assertEqual(0, proplists:get_value(active, Counts1)),
    ?assertEqual(0, proplists:get_value(workers, Counts1)),

    %% Start a child
    {ok, _Pid1} = flurm_job_executor_sup:start_job(#{job_id => 6001, working_dir => <<"/tmp">>, num_cpus => 1, memory_mb => 512}),

    Counts2 = supervisor:count_children(SupPid),
    ?assertEqual(1, proplists:get_value(active, Counts2)),
    ?assertEqual(1, proplists:get_value(workers, Counts2)),

    gen_server:stop(SupPid).

test_which_children() ->
    {ok, SupPid} = flurm_job_executor_sup:start_link(),

    %% Start some children
    {ok, Pid1} = flurm_job_executor_sup:start_job(#{job_id => 6002, working_dir => <<"/tmp">>, num_cpus => 1, memory_mb => 512}),
    {ok, Pid2} = flurm_job_executor_sup:start_job(#{job_id => 6003, working_dir => <<"/tmp">>, num_cpus => 1, memory_mb => 512}),

    Children = supervisor:which_children(SupPid),

    %% Should have 2 children
    ?assertEqual(2, length(Children)),

    %% Each child should be a worker
    lists:foreach(fun({_Id, Pid, Type, _Modules}) ->
        ?assertEqual(worker, Type),
        ?assert(is_pid(Pid))
    end, Children),

    gen_server:stop(SupPid).
