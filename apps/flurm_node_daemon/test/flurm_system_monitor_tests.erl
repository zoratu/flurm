%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_system_monitor module
%%% Tests system metrics collection and GPU allocation
%%%-------------------------------------------------------------------
-module(flurm_system_monitor_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up any leftover state from previous test runs
    cleanup_all(),
    %% Start lager for logging
    application:ensure_all_started(lager),
    %% Start fresh
    {ok, Pid} = flurm_system_monitor:start_link(),
    {started, Pid}.

cleanup({started, _Pid}) ->
    cleanup_all();
cleanup(_) ->
    cleanup_all().

%% Comprehensive cleanup function to ensure test isolation
cleanup_all() ->
    %% Stop the gen_server if running
    catch gen_server:stop(flurm_system_monitor, normal, 5000),
    %% Also try exit in case stop doesn't work
    case whereis(flurm_system_monitor) of
        undefined -> ok;
        Pid when is_pid(Pid) ->
            catch exit(Pid, kill),
            timer:sleep(10)
    end,
    %% Delete any ETS tables that might have been created
    catch ets:delete(flurm_system_monitor),
    catch ets:delete(flurm_system_monitor_metrics),
    catch ets:delete(flurm_system_monitor_gpus),
    catch ets:delete(flurm_gpu_allocation),
    %% Unload any meck mocks
    catch meck:unload(flurm_system_monitor),
    catch meck:unload(os),
    catch meck:unload(file),
    %% Give time for cleanup to complete
    timer:sleep(20),
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

system_monitor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Server starts correctly", fun test_server_starts/0},
      {"Get metrics", fun test_get_metrics/0},
      {"Get hostname", fun test_get_hostname/0},
      {"Get GPUs", fun test_get_gpus/0},
      {"Get disk usage", fun test_get_disk_usage/0},
      {"Get GPU allocation", fun test_get_gpu_allocation/0},
      {"Allocate GPUs - zero", fun test_allocate_zero_gpus/0},
      {"Allocate GPUs - success", fun test_allocate_gpus_success/0},
      {"Allocate GPUs - not enough", fun test_allocate_gpus_not_enough/0},
      {"Release GPUs", fun test_release_gpus/0},
      {"Release GPUs - nonexistent job", fun test_release_gpus_nonexistent/0},
      {"Handle unknown call", fun test_unknown_call/0},
      {"Handle unknown cast", fun test_unknown_cast/0},
      {"Handle unknown info", fun test_unknown_info/0},
      {"Collect timer fires", fun test_collect_timer/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_server_starts() ->
    Pid = whereis(flurm_system_monitor),
    ?assert(is_pid(Pid)).

test_get_metrics() ->
    Metrics = flurm_system_monitor:get_metrics(),
    ?assert(is_map(Metrics)),

    %% Required keys
    ?assert(maps:is_key(hostname, Metrics)),
    ?assert(maps:is_key(cpus, Metrics)),
    ?assert(maps:is_key(total_memory_mb, Metrics)),
    ?assert(maps:is_key(free_memory_mb, Metrics)),
    ?assert(maps:is_key(available_memory_mb, Metrics)),
    ?assert(maps:is_key(cached_memory_mb, Metrics)),
    ?assert(maps:is_key(load_avg, Metrics)),
    ?assert(maps:is_key(load_avg_5, Metrics)),
    ?assert(maps:is_key(load_avg_15, Metrics)),
    ?assert(maps:is_key(gpu_count, Metrics)),

    %% Verify types
    ?assert(is_binary(maps:get(hostname, Metrics))),
    ?assert(is_integer(maps:get(cpus, Metrics))),
    ?assert(is_integer(maps:get(total_memory_mb, Metrics))).

test_get_hostname() ->
    Hostname = flurm_system_monitor:get_hostname(),
    ?assert(is_binary(Hostname)),
    ?assert(byte_size(Hostname) > 0).

test_get_gpus() ->
    GPUs = flurm_system_monitor:get_gpus(),
    ?assert(is_list(GPUs)),
    %% Each GPU should be a map with required fields
    lists:foreach(fun(GPU) ->
        ?assert(is_map(GPU)),
        ?assert(maps:is_key(index, GPU)),
        ?assert(maps:is_key(name, GPU)),
        ?assert(maps:is_key(type, GPU))
    end, GPUs).

test_get_disk_usage() ->
    DiskUsage = flurm_system_monitor:get_disk_usage(),
    ?assert(is_map(DiskUsage)),
    %% Each mount point entry should have usage info
    maps:foreach(fun(_MountPoint, Info) ->
        ?assert(is_map(Info))
    end, DiskUsage).

test_get_gpu_allocation() ->
    Allocation = flurm_system_monitor:get_gpu_allocation(),
    ?assert(is_map(Allocation)),
    %% Initially should be empty
    ?assertEqual(0, maps:size(Allocation)).

test_allocate_zero_gpus() ->
    JobId = 1001,
    {ok, Indices} = flurm_system_monitor:allocate_gpus(JobId, 0),
    ?assertEqual([], Indices).

test_allocate_gpus_success() ->
    GPUs = flurm_system_monitor:get_gpus(),
    case length(GPUs) of
        0 ->
            %% No GPUs available, skip test
            ok;
        N when N > 0 ->
            JobId = 1002,
            {ok, Indices} = flurm_system_monitor:allocate_gpus(JobId, 1),
            ?assertEqual(1, length(Indices)),
            ?assert(lists:all(fun(I) -> is_integer(I) end, Indices)),

            %% Verify allocation is tracked
            Allocation = flurm_system_monitor:get_gpu_allocation(),
            ?assert(maps:size(Allocation) >= 1)
    end.

test_allocate_gpus_not_enough() ->
    %% Try to allocate more GPUs than available
    JobId = 1003,
    Result = flurm_system_monitor:allocate_gpus(JobId, 1000),
    ?assertEqual({error, not_enough_gpus}, Result).

test_release_gpus() ->
    GPUs = flurm_system_monitor:get_gpus(),
    case length(GPUs) of
        0 ->
            %% No GPUs available, just test the release call
            ok = flurm_system_monitor:release_gpus(2001);
        N when N > 0 ->
            JobId = 2001,
            %% First allocate
            {ok, _} = flurm_system_monitor:allocate_gpus(JobId, 1),

            %% Then release
            ok = flurm_system_monitor:release_gpus(JobId),

            %% Verify GPUs are free
            Allocation = flurm_system_monitor:get_gpu_allocation(),
            AllocatedToJob = [J || {_, J} <- maps:to_list(Allocation), J =:= JobId],
            ?assertEqual([], AllocatedToJob)
    end.

test_release_gpus_nonexistent() ->
    %% Should succeed even if job has no GPU allocations
    ok = flurm_system_monitor:release_gpus(9999).

test_unknown_call() ->
    Result = gen_server:call(flurm_system_monitor, {unknown_request, arg}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    ok = gen_server:cast(flurm_system_monitor, {unknown_cast, arg}),
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_system_monitor))).

test_unknown_info() ->
    flurm_system_monitor ! {unknown_info, arg},
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_system_monitor))).

test_collect_timer() ->
    %% Wait for collect timer to fire (5 second interval)
    %% We'll just verify server is still running after a short wait
    timer:sleep(100),
    ?assert(is_pid(whereis(flurm_system_monitor))).

%%====================================================================
%% Terminate Test - separate fixture since it stops the server
%%====================================================================

terminate_test_() ->
    {setup,
     fun() ->
         cleanup_all(),
         application:ensure_all_started(lager),
         {ok, Pid} = flurm_system_monitor:start_link(),
         Pid
     end,
     fun(_) ->
         cleanup_all()
     end,
     [
      {"Terminate callback", fun() ->
          Pid = whereis(flurm_system_monitor),
          ?assert(is_pid(Pid)),
          gen_server:stop(flurm_system_monitor),
          timer:sleep(50),
          ?assertEqual(undefined, whereis(flurm_system_monitor))
      end}
     ]}.

%%====================================================================
%% GPU Allocation Workflow Tests
%%====================================================================

gpu_workflow_test_() ->
    {setup,
     fun() ->
         cleanup_all(),
         application:ensure_all_started(lager),
         {ok, Pid} = flurm_system_monitor:start_link(),
         Pid
     end,
     fun(_Pid) ->
         cleanup_all()
     end,
     [
      {"Multiple jobs can allocate GPUs", fun() ->
          GPUs = flurm_system_monitor:get_gpus(),
          case length(GPUs) >= 2 of
              false ->
                  %% Not enough GPUs to test
                  ok;
              true ->
                  %% Job 1 allocates
                  {ok, Indices1} = flurm_system_monitor:allocate_gpus(3001, 1),
                  ?assertEqual(1, length(Indices1)),

                  %% Job 2 allocates
                  {ok, Indices2} = flurm_system_monitor:allocate_gpus(3002, 1),
                  ?assertEqual(1, length(Indices2)),

                  %% Should be different GPUs
                  ?assertEqual([], Indices1 -- (Indices1 -- Indices2)),

                  %% Release job 1
                  ok = flurm_system_monitor:release_gpus(3001),

                  %% Job 3 can now allocate
                  {ok, Indices3} = flurm_system_monitor:allocate_gpus(3003, 1),
                  ?assertEqual(1, length(Indices3))
          end
      end}
     ]}.

%%====================================================================
%% Platform Detection Tests
%%====================================================================

platform_test_() ->
    {foreach,
     fun() ->
         cleanup_all(),
         application:ensure_all_started(lager),
         {ok, Pid} = flurm_system_monitor:start_link(),
         Pid
     end,
     fun(_) ->
         cleanup_all()
     end,
     [
      {"CPU count is positive", fun() ->
          Metrics = flurm_system_monitor:get_metrics(),
          CPUs = maps:get(cpus, Metrics),
          ?assert(is_integer(CPUs)),
          ?assert(CPUs > 0)
      end},
      {"Total memory is positive", fun() ->
          Metrics = flurm_system_monitor:get_metrics(),
          TotalMem = maps:get(total_memory_mb, Metrics),
          ?assert(is_integer(TotalMem)),
          ?assert(TotalMem > 0)
      end},
      {"Load average is float", fun() ->
          %% Wait for collect timer
          timer:sleep(5500),
          Metrics = flurm_system_monitor:get_metrics(),
          LoadAvg = maps:get(load_avg, Metrics),
          ?assert(is_float(LoadAvg))
      end}
     ]}.

%%====================================================================
%% Memory Info Tests
%%====================================================================

memory_info_test_() ->
    {setup,
     fun() ->
         cleanup_all(),
         application:ensure_all_started(lager),
         {ok, Pid} = flurm_system_monitor:start_link(),
         Pid
     end,
     fun(_) ->
         cleanup_all()
     end,
     {timeout, 10,
      [
       {"Free memory is reasonable", fun() ->
           timer:sleep(5500), % Wait for collection
           Metrics = flurm_system_monitor:get_metrics(),
           FreeMem = maps:get(free_memory_mb, Metrics),
           TotalMem = maps:get(total_memory_mb, Metrics),
           ?assert(is_integer(FreeMem)),
           ?assert(FreeMem =< TotalMem)
       end}
      ]}}.
