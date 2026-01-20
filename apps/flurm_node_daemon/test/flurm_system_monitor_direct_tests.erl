%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_system_monitor module
%%%
%%% Tests the system monitor gen_server directly without mocking it.
%%% Tests all exported functions and internal callbacks.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_system_monitor_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

system_monitor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_start_link/1,
      fun test_get_metrics/1,
      fun test_get_hostname/1,
      fun test_get_gpus/1,
      fun test_get_disk_usage/1,
      fun test_get_gpu_allocation_empty/1,
      fun test_allocate_gpus_zero/1,
      fun test_allocate_and_release_gpus/1,
      fun test_allocate_gpus_not_enough/1,
      fun test_unknown_call/1,
      fun test_unknown_cast/1,
      fun test_unknown_info/1,
      fun test_collect_message/1,
      fun test_terminate/1
     ]}.

setup() ->
    %% Start lager for logging
    catch application:start(lager),
    ok.

cleanup(_) ->
    %% Stop any running monitor
    catch gen_server:stop(flurm_system_monitor),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_start_link(_) ->
    {"start_link/0 starts and registers the server",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         ?assert(is_pid(Pid)),
         ?assertEqual(Pid, whereis(flurm_system_monitor)),

         gen_server:stop(Pid)
     end}.

test_get_metrics(_) ->
    {"get_metrics/0 returns system metrics map",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         Metrics = flurm_system_monitor:get_metrics(),

         ?assert(is_map(Metrics)),
         ?assert(maps:is_key(hostname, Metrics)),
         ?assert(maps:is_key(cpus, Metrics)),
         ?assert(maps:is_key(total_memory_mb, Metrics)),
         ?assert(maps:is_key(free_memory_mb, Metrics)),
         ?assert(maps:is_key(available_memory_mb, Metrics)),
         ?assert(maps:is_key(load_avg, Metrics)),
         ?assert(maps:is_key(load_avg_5, Metrics)),
         ?assert(maps:is_key(load_avg_15, Metrics)),
         ?assert(maps:is_key(gpu_count, Metrics)),

         %% Verify types
         ?assert(is_binary(maps:get(hostname, Metrics))),
         ?assert(is_integer(maps:get(cpus, Metrics))),
         ?assert(maps:get(cpus, Metrics) > 0),
         ?assert(is_integer(maps:get(total_memory_mb, Metrics))),

         gen_server:stop(Pid)
     end}.

test_get_hostname(_) ->
    {"get_hostname/0 returns binary hostname",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         Hostname = flurm_system_monitor:get_hostname(),

         ?assert(is_binary(Hostname)),
         ?assert(byte_size(Hostname) > 0),

         gen_server:stop(Pid)
     end}.

test_get_gpus(_) ->
    {"get_gpus/0 returns list of GPUs",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         GPUs = flurm_system_monitor:get_gpus(),

         ?assert(is_list(GPUs)),
         %% Each GPU should be a map with required fields if present
         lists:foreach(fun(GPU) ->
             ?assert(is_map(GPU)),
             ?assert(maps:is_key(index, GPU)),
             ?assert(maps:is_key(name, GPU)),
             ?assert(maps:is_key(type, GPU))
         end, GPUs),

         gen_server:stop(Pid)
     end}.

test_get_disk_usage(_) ->
    {"get_disk_usage/0 returns disk usage map",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         DiskUsage = flurm_system_monitor:get_disk_usage(),

         ?assert(is_map(DiskUsage)),
         %% Root should usually be present
         case maps:is_key(<<"/">>, DiskUsage) of
             true ->
                 RootUsage = maps:get(<<"/">>, DiskUsage),
                 ?assert(maps:is_key(total_mb, RootUsage)),
                 ?assert(maps:is_key(used_mb, RootUsage)),
                 ?assert(maps:is_key(available_mb, RootUsage)),
                 ?assert(maps:is_key(percent_used, RootUsage));
             false ->
                 ok  % Might not be available on all systems
         end,

         gen_server:stop(Pid)
     end}.

test_get_gpu_allocation_empty(_) ->
    {"get_gpu_allocation/0 returns empty map initially",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         Allocation = flurm_system_monitor:get_gpu_allocation(),

         ?assertEqual(#{}, Allocation),

         gen_server:stop(Pid)
     end}.

test_allocate_gpus_zero(_) ->
    {"allocate_gpus/2 with 0 GPUs returns empty list",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         Result = flurm_system_monitor:allocate_gpus(1001, 0),

         ?assertEqual({ok, []}, Result),

         gen_server:stop(Pid)
     end}.

test_allocate_and_release_gpus(_) ->
    {"allocate_gpus/2 and release_gpus/1 manage GPU allocation",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         %% Get current GPU count
         GPUs = flurm_system_monitor:get_gpus(),
         NumGPUs = length(GPUs),

         case NumGPUs of
             0 ->
                 %% No GPUs available, test allocation failure
                 Result = flurm_system_monitor:allocate_gpus(1001, 1),
                 ?assertEqual({error, not_enough_gpus}, Result);
             N when N > 0 ->
                 %% Allocate some GPUs
                 {ok, AllocatedIndices} = flurm_system_monitor:allocate_gpus(1001, min(N, 2)),
                 ?assert(is_list(AllocatedIndices)),
                 ?assertEqual(min(N, 2), length(AllocatedIndices)),

                 %% Verify allocation
                 Allocation = flurm_system_monitor:get_gpu_allocation(),
                 ?assert(maps:size(Allocation) > 0),

                 %% Release GPUs
                 ok = flurm_system_monitor:release_gpus(1001),

                 %% Sync with gen_server to ensure cast was processed
                 _ = sys:get_state(Pid),

                 %% Verify release
                 AllocationAfter = flurm_system_monitor:get_gpu_allocation(),
                 ?assertEqual(#{}, AllocationAfter)
         end,

         gen_server:stop(Pid)
     end}.

test_allocate_gpus_not_enough(_) ->
    {"allocate_gpus/2 returns error when not enough GPUs",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         %% Try to allocate more GPUs than available
         Result = flurm_system_monitor:allocate_gpus(1001, 1000),

         ?assertEqual({error, not_enough_gpus}, Result),

         gen_server:stop(Pid)
     end}.

test_unknown_call(_) ->
    {"handle_call returns error for unknown request",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         Result = gen_server:call(Pid, unknown_request),

         ?assertEqual({error, unknown_request}, Result),

         gen_server:stop(Pid)
     end}.

test_unknown_cast(_) ->
    {"handle_cast ignores unknown messages",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         %% Should not crash
         gen_server:cast(Pid, unknown_message),

         %% Verify server is still running
         ?assert(is_process_alive(Pid)),

         gen_server:stop(Pid)
     end}.

test_unknown_info(_) ->
    {"handle_info ignores unknown messages",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         %% Send unknown message
         Pid ! unknown_message,

         %% Sync with gen_server to ensure message was processed
         _ = sys:get_state(Pid),
         ?assert(is_process_alive(Pid)),

         gen_server:stop(Pid)
     end}.

test_collect_message(_) ->
    {"handle_info processes collect message",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         %% Get initial metrics
         MetricsBefore = flurm_system_monitor:get_metrics(),

         %% Trigger a collect manually
         Pid ! collect,

         %% Sync with gen_server to ensure collect was processed
         _ = sys:get_state(Pid),

         %% Get metrics again - should still work
         MetricsAfter = flurm_system_monitor:get_metrics(),

         ?assert(is_map(MetricsAfter)),
         ?assert(maps:is_key(load_avg, MetricsAfter)),

         gen_server:stop(Pid)
     end}.

test_terminate(_) ->
    {"terminate/2 cleans up gracefully",
     fun() ->
         {ok, Pid} = flurm_system_monitor:start_link(),

         %% Stop should succeed
         gen_server:stop(Pid),

         %% Process should be dead - wait_for_death handles the synchronization
         flurm_test_utils:wait_for_death(Pid),
         ?assertNot(is_process_alive(Pid))
     end}.

%%====================================================================
%% Additional coverage tests for internal functions
%%====================================================================

internal_functions_test_() ->
    {setup,
     fun() -> catch application:start(lager), ok end,
     fun(_) -> ok end,
     [
      {"multiple GPU allocations work", fun test_multiple_gpu_allocations/0},
      {"release non-existent job is safe", fun test_release_nonexistent_job/0}
     ]}.

test_multiple_gpu_allocations() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    GPUs = flurm_system_monitor:get_gpus(),
    NumGPUs = length(GPUs),

    case NumGPUs >= 2 of
        true ->
            %% Allocate GPUs to two different jobs
            {ok, Alloc1} = flurm_system_monitor:allocate_gpus(1001, 1),
            {ok, Alloc2} = flurm_system_monitor:allocate_gpus(1002, 1),

            ?assertEqual(1, length(Alloc1)),
            ?assertEqual(1, length(Alloc2)),
            ?assertNotEqual(Alloc1, Alloc2),

            %% Verify both allocations
            Allocation = flurm_system_monitor:get_gpu_allocation(),
            ?assertEqual(2, maps:size(Allocation)),

            %% Release one job
            flurm_system_monitor:release_gpus(1001),
            _ = sys:get_state(flurm_system_monitor),

            AllocationAfter1 = flurm_system_monitor:get_gpu_allocation(),
            ?assertEqual(1, maps:size(AllocationAfter1)),

            %% Release other job
            flurm_system_monitor:release_gpus(1002),
            _ = sys:get_state(flurm_system_monitor),

            AllocationAfter2 = flurm_system_monitor:get_gpu_allocation(),
            ?assertEqual(0, maps:size(AllocationAfter2));
        false ->
            ok  % Not enough GPUs to test
    end,

    gen_server:stop(Pid).

test_release_nonexistent_job() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Should not crash when releasing non-existent job
    flurm_system_monitor:release_gpus(99999),
    _ = sys:get_state(Pid),

    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).
