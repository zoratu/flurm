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

%%====================================================================
%% Platform Detection Tests
%%====================================================================

platform_detection_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> ok end,
     [
      {"detect_platform returns valid atom", fun test_detect_platform/0},
      {"get_system_hostname returns binary", fun test_get_system_hostname/0},
      {"get_cpu_count returns positive integer", fun test_get_cpu_count/0}
     ]}.

test_detect_platform() ->
    Platform = flurm_system_monitor:detect_platform(),
    ?assert(Platform =:= linux orelse Platform =:= darwin orelse Platform =:= other),
    %% Verify it's consistent
    Platform2 = flurm_system_monitor:detect_platform(),
    ?assertEqual(Platform, Platform2).

test_get_system_hostname() ->
    Hostname = flurm_system_monitor:get_system_hostname(),
    ?assert(is_binary(Hostname)),
    ?assert(byte_size(Hostname) > 0),
    %% Should not contain control characters
    ?assertNot(binary:match(Hostname, <<"\n">>) =/= nomatch),
    ?assertNot(binary:match(Hostname, <<"\r">>) =/= nomatch).

test_get_cpu_count() ->
    Count = flurm_system_monitor:get_cpu_count(),
    ?assert(is_integer(Count)),
    ?assert(Count > 0),
    %% Should be less than 10000 (reasonable upper bound)
    ?assert(Count < 10000).

%%====================================================================
%% Memory Function Tests
%%====================================================================

memory_functions_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> ok end,
     [
      {"get_total_memory linux returns positive integer", fun test_get_total_memory_linux/0},
      {"get_total_memory darwin returns positive integer", fun test_get_total_memory_darwin/0},
      {"get_total_memory other returns fallback value", fun test_get_total_memory_other/0},
      {"get_memory_info linux returns tuple", fun test_get_memory_info_linux/0},
      {"get_memory_info darwin returns tuple", fun test_get_memory_info_darwin/0},
      {"get_memory_info other returns zeros", fun test_get_memory_info_other/0},
      {"parse_meminfo_field extracts value", fun test_parse_meminfo_field/0},
      {"parse_meminfo_field handles missing field", fun test_parse_meminfo_field_missing/0},
      {"parse_vm_stat_line extracts value", fun test_parse_vm_stat_line/0},
      {"parse_vm_stat_line handles missing field", fun test_parse_vm_stat_line_missing/0},
      {"erlang_memory_fallback returns positive value", fun test_erlang_memory_fallback/0}
     ]}.

test_get_total_memory_linux() ->
    TotalMem = flurm_system_monitor:get_total_memory(linux),
    ?assert(is_integer(TotalMem)),
    ?assert(TotalMem >= 0).

test_get_total_memory_darwin() ->
    TotalMem = flurm_system_monitor:get_total_memory(darwin),
    ?assert(is_integer(TotalMem)),
    ?assert(TotalMem >= 0).

test_get_total_memory_other() ->
    TotalMem = flurm_system_monitor:get_total_memory(other),
    ?assert(is_integer(TotalMem)),
    ?assert(TotalMem >= 0).

test_get_memory_info_linux() ->
    {FreeMem, CachedMem, AvailMem} = flurm_system_monitor:get_memory_info(linux),
    ?assert(is_integer(FreeMem)),
    ?assert(is_integer(CachedMem)),
    ?assert(is_integer(AvailMem)),
    ?assert(FreeMem >= 0),
    ?assert(CachedMem >= 0),
    ?assert(AvailMem >= 0).

test_get_memory_info_darwin() ->
    {FreeMem, CachedMem, AvailMem} = flurm_system_monitor:get_memory_info(darwin),
    ?assert(is_integer(FreeMem)),
    ?assert(is_integer(CachedMem)),
    ?assert(is_integer(AvailMem)),
    ?assert(FreeMem >= 0),
    ?assert(CachedMem >= 0),
    ?assert(AvailMem >= 0).

test_get_memory_info_other() ->
    {FreeMem, CachedMem, AvailMem} = flurm_system_monitor:get_memory_info(other),
    ?assertEqual(0, FreeMem),
    ?assertEqual(0, CachedMem),
    ?assertEqual(0, AvailMem).

test_parse_meminfo_field() ->
    Content = <<"MemTotal:       16384000 kB\nMemFree:         8192000 kB\nCached:          2048000 kB\n">>,

    MemTotal = flurm_system_monitor:parse_meminfo_field(Content, <<"MemTotal:">>),
    ?assertEqual(16384000, MemTotal),

    MemFree = flurm_system_monitor:parse_meminfo_field(Content, <<"MemFree:">>),
    ?assertEqual(8192000, MemFree),

    Cached = flurm_system_monitor:parse_meminfo_field(Content, <<"Cached:">>),
    ?assertEqual(2048000, Cached).

test_parse_meminfo_field_missing() ->
    Content = <<"MemTotal:       16384000 kB\n">>,

    Missing = flurm_system_monitor:parse_meminfo_field(Content, <<"MemAvailable:">>),
    ?assertEqual(0, Missing),

    %% Test with empty content
    Empty = flurm_system_monitor:parse_meminfo_field(<<>>, <<"MemTotal:">>),
    ?assertEqual(0, Empty).

test_parse_vm_stat_line() ->
    Lines = ["Mach Virtual Memory Statistics: (page size of 4096 bytes)",
             "Pages free:                             1234567.",
             "Pages active:                           2345678.",
             "Pages inactive:                         3456789."],

    Free = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages free"),
    ?assertEqual(1234567, Free),

    Active = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages active"),
    ?assertEqual(2345678, Active),

    Inactive = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages inactive"),
    ?assertEqual(3456789, Inactive).

test_parse_vm_stat_line_missing() ->
    Lines = ["Mach Virtual Memory Statistics: (page size of 4096 bytes)",
             "Pages free:                             1234567."],

    Missing = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages wired"),
    ?assertEqual(0, Missing),

    %% Test with empty list
    Empty = flurm_system_monitor:parse_vm_stat_line([], "Pages free"),
    ?assertEqual(0, Empty).

test_erlang_memory_fallback() ->
    Fallback = flurm_system_monitor:erlang_memory_fallback(),
    ?assert(is_integer(Fallback)),
    ?assert(Fallback > 0).

%%====================================================================
%% Load Average Tests
%%====================================================================

load_average_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> ok end,
     [
      {"get_load_average linux returns tuple of floats", fun test_get_load_average_linux/0},
      {"get_load_average darwin returns tuple of floats", fun test_get_load_average_darwin/0},
      {"get_load_average other returns run queue based values", fun test_get_load_average_other/0},
      {"binary_to_float_safe converts binary to float", fun test_binary_to_float_safe/0},
      {"binary_to_float_safe handles integers", fun test_binary_to_float_safe_integer/0},
      {"binary_to_float_safe handles invalid input", fun test_binary_to_float_safe_invalid/0},
      {"list_to_float_safe converts string to float", fun test_list_to_float_safe/0},
      {"list_to_float_safe handles integers", fun test_list_to_float_safe_integer/0},
      {"list_to_float_safe handles invalid input", fun test_list_to_float_safe_invalid/0},
      {"list_to_float_safe handles whitespace", fun test_list_to_float_safe_whitespace/0}
     ]}.

test_get_load_average_linux() ->
    {L1, L5, L15} = flurm_system_monitor:get_load_average(linux),
    ?assert(is_float(L1)),
    ?assert(is_float(L5)),
    ?assert(is_float(L15)),
    ?assert(L1 >= 0.0),
    ?assert(L5 >= 0.0),
    ?assert(L15 >= 0.0).

test_get_load_average_darwin() ->
    {L1, L5, L15} = flurm_system_monitor:get_load_average(darwin),
    ?assert(is_float(L1)),
    ?assert(is_float(L5)),
    ?assert(is_float(L15)),
    ?assert(L1 >= 0.0),
    ?assert(L5 >= 0.0),
    ?assert(L15 >= 0.0).

test_get_load_average_other() ->
    {L1, L5, L15} = flurm_system_monitor:get_load_average(other),
    ?assert(is_float(L1)),
    ?assert(is_float(L5)),
    ?assert(is_float(L15)),
    ?assert(L1 >= 0.0),
    ?assert(L5 >= 0.0),
    ?assert(L15 >= 0.0),
    %% All three should be equal (run queue based)
    ?assertEqual(L1, L5),
    ?assertEqual(L5, L15).

test_binary_to_float_safe() ->
    ?assertEqual(1.23, flurm_system_monitor:binary_to_float_safe(<<"1.23">>)),
    ?assertEqual(0.5, flurm_system_monitor:binary_to_float_safe(<<"0.5">>)),
    ?assertEqual(10.0, flurm_system_monitor:binary_to_float_safe(<<"10.0">>)).

test_binary_to_float_safe_integer() ->
    ?assertEqual(1.0, flurm_system_monitor:binary_to_float_safe(<<"1">>)),
    ?assertEqual(42.0, flurm_system_monitor:binary_to_float_safe(<<"42">>)),
    ?assertEqual(0.0, flurm_system_monitor:binary_to_float_safe(<<"0">>)).

test_binary_to_float_safe_invalid() ->
    ?assertEqual(0.0, flurm_system_monitor:binary_to_float_safe(<<"abc">>)),
    ?assertEqual(0.0, flurm_system_monitor:binary_to_float_safe(<<>>)),
    ?assertEqual(0.0, flurm_system_monitor:binary_to_float_safe(<<"not a number">>)).

test_list_to_float_safe() ->
    ?assertEqual(1.23, flurm_system_monitor:list_to_float_safe("1.23")),
    ?assertEqual(0.5, flurm_system_monitor:list_to_float_safe("0.5")),
    ?assertEqual(10.0, flurm_system_monitor:list_to_float_safe("10.0")).

test_list_to_float_safe_integer() ->
    ?assertEqual(1.0, flurm_system_monitor:list_to_float_safe("1")),
    ?assertEqual(42.0, flurm_system_monitor:list_to_float_safe("42")),
    ?assertEqual(0.0, flurm_system_monitor:list_to_float_safe("0")).

test_list_to_float_safe_invalid() ->
    ?assertEqual(0.0, flurm_system_monitor:list_to_float_safe("abc")),
    ?assertEqual(0.0, flurm_system_monitor:list_to_float_safe("")),
    ?assertEqual(0.0, flurm_system_monitor:list_to_float_safe("not a number")).

test_list_to_float_safe_whitespace() ->
    ?assertEqual(1.23, flurm_system_monitor:list_to_float_safe("  1.23  ")),
    ?assertEqual(42.0, flurm_system_monitor:list_to_float_safe("\t42\n")),
    ?assertEqual(0.5, flurm_system_monitor:list_to_float_safe(" 0.5 ")).

%%====================================================================
%% GPU Detection Tests
%%====================================================================

gpu_detection_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> ok end,
     [
      {"detect_gpus returns list", fun test_detect_gpus/0},
      {"detect_nvidia_gpus returns list", fun test_detect_nvidia_gpus/0},
      {"parse_nvidia_gpu parses valid line", fun test_parse_nvidia_gpu_valid/0},
      {"parse_nvidia_gpu handles invalid line", fun test_parse_nvidia_gpu_invalid/0},
      {"parse_nvidia_gpu handles empty line", fun test_parse_nvidia_gpu_empty/0},
      {"detect_amd_gpus returns list", fun test_detect_amd_gpus/0},
      {"check_amd_vendor handles non-existent file", fun test_check_amd_vendor_nonexistent/0}
     ]}.

test_detect_gpus() ->
    GPUs = flurm_system_monitor:detect_gpus(),
    ?assert(is_list(GPUs)),
    lists:foreach(fun(GPU) ->
        ?assert(is_map(GPU)),
        ?assert(maps:is_key(index, GPU)),
        ?assert(maps:is_key(name, GPU)),
        ?assert(maps:is_key(type, GPU))
    end, GPUs).

test_detect_nvidia_gpus() ->
    GPUs = flurm_system_monitor:detect_nvidia_gpus(),
    ?assert(is_list(GPUs)),
    lists:foreach(fun(GPU) ->
        ?assert(is_map(GPU)),
        ?assertEqual(nvidia, maps:get(type, GPU)),
        ?assert(maps:is_key(index, GPU)),
        ?assert(maps:is_key(name, GPU)),
        ?assert(maps:is_key(memory_mb, GPU))
    end, GPUs).

test_parse_nvidia_gpu_valid() ->
    %% Valid NVIDIA GPU line format: index, name, memory
    Line = "0, NVIDIA GeForce RTX 3080, 10240",
    Result = flurm_system_monitor:parse_nvidia_gpu(Line),
    ?assertMatch({true, #{index := 0, name := _, type := nvidia, memory_mb := 10240}}, Result),

    %% Another valid line
    Line2 = "1, Tesla V100-SXM2-16GB, 16384",
    {true, GPU2} = flurm_system_monitor:parse_nvidia_gpu(Line2),
    ?assertEqual(1, maps:get(index, GPU2)),
    ?assertEqual(nvidia, maps:get(type, GPU2)),
    ?assertEqual(16384, maps:get(memory_mb, GPU2)).

test_parse_nvidia_gpu_invalid() ->
    %% Invalid format - not enough fields
    ?assertEqual(false, flurm_system_monitor:parse_nvidia_gpu("0, NVIDIA GPU")),
    %% Invalid format - non-numeric index
    ?assertEqual(false, flurm_system_monitor:parse_nvidia_gpu("abc, NVIDIA GPU, 1024")),
    %% Invalid format - non-numeric memory
    ?assertEqual(false, flurm_system_monitor:parse_nvidia_gpu("0, NVIDIA GPU, abc")).

test_parse_nvidia_gpu_empty() ->
    ?assertEqual(false, flurm_system_monitor:parse_nvidia_gpu("")),
    ?assertEqual(false, flurm_system_monitor:parse_nvidia_gpu("   ")),
    ?assertEqual(false, flurm_system_monitor:parse_nvidia_gpu("\n")).

test_detect_amd_gpus() ->
    GPUs = flurm_system_monitor:detect_amd_gpus(),
    ?assert(is_list(GPUs)),
    lists:foreach(fun(GPU) ->
        ?assert(is_map(GPU)),
        ?assertEqual(amd, maps:get(type, GPU)),
        ?assert(maps:is_key(index, GPU)),
        ?assert(maps:is_key(name, GPU))
    end, GPUs).

test_check_amd_vendor_nonexistent() ->
    %% Non-existent file should return false
    Result = flurm_system_monitor:check_amd_vendor("/nonexistent/path/vendor"),
    ?assertEqual(false, Result).

%%====================================================================
%% Disk Usage Tests
%%====================================================================

disk_usage_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> ok end,
     [
      {"get_disk_info returns map", fun test_get_disk_info/0},
      {"get_mount_usage returns usage for root", fun test_get_mount_usage_root/0},
      {"get_mount_usage returns false for nonexistent", fun test_get_mount_usage_nonexistent/0}
     ]}.

test_get_disk_info() ->
    DiskInfo = flurm_system_monitor:get_disk_info(),
    ?assert(is_map(DiskInfo)),
    %% On most systems, root should be present
    maps:foreach(fun(MountPoint, Usage) ->
        ?assert(is_binary(MountPoint)),
        ?assert(is_map(Usage)),
        ?assert(maps:is_key(total_mb, Usage)),
        ?assert(maps:is_key(used_mb, Usage)),
        ?assert(maps:is_key(available_mb, Usage)),
        ?assert(maps:is_key(percent_used, Usage))
    end, DiskInfo).

test_get_mount_usage_root() ->
    Result = flurm_system_monitor:get_mount_usage("/"),
    case Result of
        {true, {<<"/">>, Usage}} ->
            ?assert(is_map(Usage)),
            ?assert(maps:get(total_mb, Usage) > 0),
            ?assert(maps:get(used_mb, Usage) >= 0),
            ?assert(maps:get(available_mb, Usage) >= 0),
            ?assert(maps:get(percent_used, Usage) >= 0),
            ?assert(maps:get(percent_used, Usage) =< 100);
        false ->
            %% Might fail on some systems
            ok
    end.

test_get_mount_usage_nonexistent() ->
    Result = flurm_system_monitor:get_mount_usage("/nonexistent/mount/point/that/does/not/exist"),
    ?assertEqual(false, Result).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> catch gen_server:stop(flurm_system_monitor), ok end,
     [
      {"parse_meminfo_field handles malformed content", fun test_parse_meminfo_malformed/0},
      {"parse_meminfo_field handles no digits after field", fun test_parse_meminfo_no_digits/0},
      {"binary_to_float_safe handles edge cases", fun test_binary_to_float_edge_cases/0},
      {"list_to_float_safe handles edge cases", fun test_list_to_float_edge_cases/0},
      {"get_load_average handles edge platform", fun test_get_load_average_edge/0},
      {"get_total_memory handles edge platform", fun test_get_total_memory_edge/0},
      {"get_memory_info handles edge platform", fun test_get_memory_info_edge/0},
      {"parse_nvidia_gpu handles whitespace variations", fun test_parse_nvidia_gpu_whitespace/0}
     ]}.

test_parse_meminfo_malformed() ->
    %% Field exists but followed by non-numeric content
    Content = <<"MemTotal: abc kB\n">>,
    Result = flurm_system_monitor:parse_meminfo_field(Content, <<"MemTotal:">>),
    ?assertEqual(0, Result).

test_parse_meminfo_no_digits() ->
    %% Field exists but no content after it
    Content = <<"MemTotal:">>,
    Result = flurm_system_monitor:parse_meminfo_field(Content, <<"MemTotal:">>),
    ?assertEqual(0, Result).

test_binary_to_float_edge_cases() ->
    %% Very large numbers
    Large = flurm_system_monitor:binary_to_float_safe(<<"999999999.99">>),
    ?assert(Large > 999999999.0),

    %% Very small numbers
    Small = flurm_system_monitor:binary_to_float_safe(<<"0.0001">>),
    ?assert(Small < 0.001),
    ?assert(Small > 0.0),

    %% Negative numbers
    Neg = flurm_system_monitor:binary_to_float_safe(<<"-1.5">>),
    ?assertEqual(-1.5, Neg).

test_list_to_float_edge_cases() ->
    %% Very large numbers
    Large = flurm_system_monitor:list_to_float_safe("999999999.99"),
    ?assert(Large > 999999999.0),

    %% Very small numbers
    Small = flurm_system_monitor:list_to_float_safe("0.0001"),
    ?assert(Small < 0.001),
    ?assert(Small > 0.0),

    %% Negative numbers
    Neg = flurm_system_monitor:list_to_float_safe("-1.5"),
    ?assertEqual(-1.5, Neg).

test_get_load_average_edge() ->
    %% Test with an atom that's not linux, darwin, or other (should use fallback)
    {L1, L5, L15} = flurm_system_monitor:get_load_average(nonexistent_platform),
    ?assert(is_float(L1)),
    ?assert(is_float(L5)),
    ?assert(is_float(L15)).

test_get_total_memory_edge() ->
    %% Test with unknown platform
    Mem = flurm_system_monitor:get_total_memory(unknown_platform),
    ?assert(is_integer(Mem)),
    ?assert(Mem > 0).

test_get_memory_info_edge() ->
    %% Test with unknown platform
    {Free, Cached, Avail} = flurm_system_monitor:get_memory_info(unknown_platform),
    ?assertEqual(0, Free),
    ?assertEqual(0, Cached),
    ?assertEqual(0, Avail).

test_parse_nvidia_gpu_whitespace() ->
    %% Extra whitespace in fields
    Line1 = "  0  ,  NVIDIA GPU  ,  1024  ",
    Result1 = flurm_system_monitor:parse_nvidia_gpu(Line1),
    ?assertMatch({true, #{index := 0}}, Result1),

    %% Standard spacing with different values
    Line2 = "1, NVIDIA GPU 2, 2048",
    Result2 = flurm_system_monitor:parse_nvidia_gpu(Line2),
    ?assertMatch({true, #{index := 1}}, Result2),

    %% Verify memory is parsed correctly
    {true, GPU} = Result2,
    ?assertEqual(2048, maps:get(memory_mb, GPU)).

%%====================================================================
%% Integration Tests - Full Workflow
%%====================================================================

integration_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> catch gen_server:stop(flurm_system_monitor), ok end,
     [
      {"full metrics collection cycle", fun test_full_metrics_cycle/0},
      {"concurrent access to metrics", fun test_concurrent_access/0},
      {"rapid GPU allocation/release", fun test_rapid_gpu_operations/0},
      {"stress test collect messages", fun test_stress_collect/0}
     ]}.

test_full_metrics_cycle() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Get all types of metrics
    Metrics = flurm_system_monitor:get_metrics(),
    Hostname = flurm_system_monitor:get_hostname(),
    GPUs = flurm_system_monitor:get_gpus(),
    DiskUsage = flurm_system_monitor:get_disk_usage(),
    Allocation = flurm_system_monitor:get_gpu_allocation(),

    %% Verify all returns are valid
    ?assert(is_map(Metrics)),
    ?assert(is_binary(Hostname)),
    ?assert(is_list(GPUs)),
    ?assert(is_map(DiskUsage)),
    ?assert(is_map(Allocation)),

    %% Hostname should match
    ?assertEqual(Hostname, maps:get(hostname, Metrics)),

    %% GPU count should match
    ?assertEqual(length(GPUs), maps:get(gpu_count, Metrics)),

    gen_server:stop(Pid).

test_concurrent_access() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    Self = self(),

    %% Spawn multiple processes to access metrics concurrently
    Pids = [spawn_link(fun() ->
        %% Each process makes multiple requests
        lists:foreach(fun(_) ->
            _Metrics = flurm_system_monitor:get_metrics(),
            _Hostname = flurm_system_monitor:get_hostname(),
            _GPUs = flurm_system_monitor:get_gpus()
        end, lists:seq(1, 10)),
        Self ! {done, self()}
    end) || _ <- lists:seq(1, 5)],

    %% Wait for all to complete
    lists:foreach(fun(P) ->
        receive
            {done, P} -> ok
        after 5000 ->
            ?assert(false)  % Timeout
        end
    end, Pids),

    %% Server should still be alive
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).

test_rapid_gpu_operations() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Rapidly allocate and release GPUs with job 0
    lists:foreach(fun(I) ->
        _Result = flurm_system_monitor:allocate_gpus(I, 0),
        flurm_system_monitor:release_gpus(I)
    end, lists:seq(1, 100)),

    %% Sync with server
    _ = sys:get_state(Pid),

    %% Allocation should be empty
    ?assertEqual(#{}, flurm_system_monitor:get_gpu_allocation()),

    %% Server should still be alive
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).

test_stress_collect() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Send many collect messages rapidly
    lists:foreach(fun(_) ->
        Pid ! collect
    end, lists:seq(1, 50)),

    %% Sync with server
    _ = sys:get_state(Pid),

    %% Should still be able to get metrics
    Metrics = flurm_system_monitor:get_metrics(),
    ?assert(is_map(Metrics)),

    %% Server should still be alive
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).

%%====================================================================
%% State Consistency Tests
%%====================================================================

state_consistency_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> catch gen_server:stop(flurm_system_monitor), ok end,
     [
      {"metrics are consistent", fun test_metrics_consistency/0},
      {"gpu allocation state is consistent", fun test_gpu_allocation_consistency/0},
      {"load average values are reasonable", fun test_load_average_reasonable/0},
      {"memory values are reasonable", fun test_memory_values_reasonable/0}
     ]}.

test_metrics_consistency() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Get metrics multiple times
    Metrics1 = flurm_system_monitor:get_metrics(),
    Metrics2 = flurm_system_monitor:get_metrics(),

    %% Static values should be identical
    ?assertEqual(maps:get(hostname, Metrics1), maps:get(hostname, Metrics2)),
    ?assertEqual(maps:get(cpus, Metrics1), maps:get(cpus, Metrics2)),
    ?assertEqual(maps:get(total_memory_mb, Metrics1), maps:get(total_memory_mb, Metrics2)),
    ?assertEqual(maps:get(gpu_count, Metrics1), maps:get(gpu_count, Metrics2)),

    gen_server:stop(Pid).

test_gpu_allocation_consistency() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    GPUs = flurm_system_monitor:get_gpus(),

    case length(GPUs) of
        0 ->
            %% No GPUs - allocation should always be empty
            ?assertEqual(#{}, flurm_system_monitor:get_gpu_allocation());
        N when N > 0 ->
            %% Allocate one GPU
            {ok, [Idx]} = flurm_system_monitor:allocate_gpus(1001, 1),

            %% Verify allocation is consistent across calls
            Alloc1 = flurm_system_monitor:get_gpu_allocation(),
            Alloc2 = flurm_system_monitor:get_gpu_allocation(),
            ?assertEqual(Alloc1, Alloc2),
            ?assertEqual(1001, maps:get(Idx, Alloc1)),

            %% Release
            flurm_system_monitor:release_gpus(1001),
            _ = sys:get_state(Pid)
    end,

    gen_server:stop(Pid).

test_load_average_reasonable() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Trigger a collect to update values
    Pid ! collect,
    _ = sys:get_state(Pid),

    Metrics = flurm_system_monitor:get_metrics(),

    %% Load averages should be non-negative
    ?assert(maps:get(load_avg, Metrics) >= 0),
    ?assert(maps:get(load_avg_5, Metrics) >= 0),
    ?assert(maps:get(load_avg_15, Metrics) >= 0),

    %% Load averages should be less than 10000 (reasonable upper bound)
    ?assert(maps:get(load_avg, Metrics) < 10000),
    ?assert(maps:get(load_avg_5, Metrics) < 10000),
    ?assert(maps:get(load_avg_15, Metrics) < 10000),

    gen_server:stop(Pid).

test_memory_values_reasonable() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Trigger a collect to update values
    Pid ! collect,
    _ = sys:get_state(Pid),

    Metrics = flurm_system_monitor:get_metrics(),

    TotalMem = maps:get(total_memory_mb, Metrics),
    FreeMem = maps:get(free_memory_mb, Metrics),
    AvailMem = maps:get(available_memory_mb, Metrics),

    %% Total memory should be positive
    ?assert(TotalMem > 0),

    %% Free and available should be non-negative
    ?assert(FreeMem >= 0),
    ?assert(AvailMem >= 0),

    %% Memory values should be reasonable (less than 100 TB)
    ?assert(TotalMem < 100 * 1024 * 1024),

    gen_server:stop(Pid).

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> catch gen_server:stop(flurm_system_monitor), ok end,
     [
      {"handle_call with various unknown requests", fun test_various_unknown_calls/0},
      {"handle_cast with various unknown messages", fun test_various_unknown_casts/0},
      {"handle_info with various unknown messages", fun test_various_unknown_infos/0},
      {"server handles invalid GPU allocation requests", fun test_invalid_gpu_allocation/0}
     ]}.

test_various_unknown_calls() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Various unknown request types
    ?assertEqual({error, unknown_request}, gen_server:call(Pid, foo)),
    ?assertEqual({error, unknown_request}, gen_server:call(Pid, {bar, baz})),
    ?assertEqual({error, unknown_request}, gen_server:call(Pid, [1, 2, 3])),
    ?assertEqual({error, unknown_request}, gen_server:call(Pid, #{key => value})),

    %% Server should still be alive
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).

test_various_unknown_casts() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Various unknown cast types - none should crash the server
    gen_server:cast(Pid, foo),
    gen_server:cast(Pid, {bar, baz}),
    gen_server:cast(Pid, [1, 2, 3]),
    gen_server:cast(Pid, #{key => value}),

    %% Sync and verify server is still alive
    _ = sys:get_state(Pid),
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).

test_various_unknown_infos() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Various unknown info messages - none should crash the server
    Pid ! foo,
    Pid ! {bar, baz},
    Pid ! [1, 2, 3],
    Pid ! #{key => value},
    Pid ! {nodedown, 'fake@node'},
    Pid ! timeout,

    %% Sync and verify server is still alive
    _ = sys:get_state(Pid),
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid).

test_invalid_gpu_allocation() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Try to allocate negative number (should fail gracefully or be handled)
    %% The allocate_gpus call expects non_neg_integer, so -1 might cause issues
    %% But we're testing robustness here

    %% Allocate more than available
    Result1 = flurm_system_monitor:allocate_gpus(1001, 99999),
    ?assertEqual({error, not_enough_gpus}, Result1),

    %% Allocate with job ID 0 (edge case)
    Result2 = flurm_system_monitor:allocate_gpus(0, 0),
    ?assertEqual({ok, []}, Result2),

    gen_server:stop(Pid).

%%====================================================================
%% Cleanup and Teardown Tests
%%====================================================================

cleanup_test_() ->
    {setup,
     fun() -> catch application:ensure_all_started(lager), ok end,
     fun(_) -> ok end,
     [
      {"terminate cleans up properly", fun test_terminate_cleanup/0},
      {"restart after stop works", fun test_restart_after_stop/0}
     ]}.

test_terminate_cleanup() ->
    {ok, Pid} = flurm_system_monitor:start_link(),

    %% Allocate some GPUs if available
    GPUs = flurm_system_monitor:get_gpus(),
    case length(GPUs) of
        0 -> ok;
        _ ->
            _Result = flurm_system_monitor:allocate_gpus(1001, 1)
    end,

    %% Stop the server
    gen_server:stop(Pid),

    %% Wait for death
    timer:sleep(100),
    ?assertNot(is_process_alive(Pid)),

    %% Registered name should be gone
    ?assertEqual(undefined, whereis(flurm_system_monitor)).

test_restart_after_stop() ->
    {ok, Pid1} = flurm_system_monitor:start_link(),
    gen_server:stop(Pid1),
    timer:sleep(50),

    %% Should be able to start again
    {ok, Pid2} = flurm_system_monitor:start_link(),
    ?assert(is_pid(Pid2)),
    ?assertNotEqual(Pid1, Pid2),

    %% Should work correctly
    Metrics = flurm_system_monitor:get_metrics(),
    ?assert(is_map(Metrics)),

    gen_server:stop(Pid2).
