%%%-------------------------------------------------------------------
%%% @doc Tests for -ifdef(TEST) exported functions in flurm_system_monitor
%%%
%%% These tests directly call the internal helper functions that are
%%% exported via -ifdef(TEST) to provide real code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_system_monitor_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% detect_platform/0 Tests
%%====================================================================

detect_platform_test_() ->
    [
        {"returns valid platform atom", fun() ->
            Platform = flurm_system_monitor:detect_platform(),
            ?assert(Platform =:= linux orelse Platform =:= darwin orelse Platform =:= other)
        end},
        {"consistent results", fun() ->
            P1 = flurm_system_monitor:detect_platform(),
            P2 = flurm_system_monitor:detect_platform(),
            ?assertEqual(P1, P2)
        end}
    ].

%%====================================================================
%% get_system_hostname/0 Tests
%%====================================================================

get_system_hostname_test_() ->
    [
        {"returns binary", fun() ->
            Hostname = flurm_system_monitor:get_system_hostname(),
            ?assert(is_binary(Hostname))
        end},
        {"returns non-empty binary", fun() ->
            Hostname = flurm_system_monitor:get_system_hostname(),
            ?assert(byte_size(Hostname) > 0)
        end}
    ].

%%====================================================================
%% get_cpu_count/0 Tests
%%====================================================================

get_cpu_count_test_() ->
    [
        {"returns positive integer", fun() ->
            Count = flurm_system_monitor:get_cpu_count(),
            ?assert(is_integer(Count)),
            ?assert(Count > 0)
        end},
        {"consistent results", fun() ->
            C1 = flurm_system_monitor:get_cpu_count(),
            C2 = flurm_system_monitor:get_cpu_count(),
            ?assertEqual(C1, C2)
        end}
    ].

%%====================================================================
%% get_total_memory/1 Tests
%%====================================================================

get_total_memory_test_() ->
    [
        {"linux platform returns positive integer", fun() ->
            Mem = flurm_system_monitor:get_total_memory(linux),
            ?assert(is_integer(Mem)),
            ?assert(Mem >= 0)
        end},
        {"darwin platform returns positive integer", fun() ->
            Mem = flurm_system_monitor:get_total_memory(darwin),
            ?assert(is_integer(Mem)),
            ?assert(Mem >= 0)
        end},
        {"other platform uses fallback", fun() ->
            Mem = flurm_system_monitor:get_total_memory(other),
            ?assert(is_integer(Mem)),
            ?assert(Mem >= 0)
        end},
        {"current platform returns memory", fun() ->
            Platform = flurm_system_monitor:detect_platform(),
            Mem = flurm_system_monitor:get_total_memory(Platform),
            ?assert(is_integer(Mem)),
            ?assert(Mem > 0)
        end}
    ].

%%====================================================================
%% get_memory_info/1 Tests
%%====================================================================

get_memory_info_test_() ->
    [
        {"returns tuple with three elements", fun() ->
            Platform = flurm_system_monitor:detect_platform(),
            Result = flurm_system_monitor:get_memory_info(Platform),
            ?assertMatch({_, _, _}, Result),
            {FreeMem, CachedMem, AvailMem} = Result,
            ?assert(is_integer(FreeMem)),
            ?assert(is_integer(CachedMem)),
            ?assert(is_integer(AvailMem))
        end},
        {"linux platform returns memory info", fun() ->
            {FreeMem, CachedMem, AvailMem} = flurm_system_monitor:get_memory_info(linux),
            ?assert(is_integer(FreeMem)),
            ?assert(is_integer(CachedMem)),
            ?assert(is_integer(AvailMem))
        end},
        {"darwin platform returns memory info", fun() ->
            {FreeMem, CachedMem, AvailMem} = flurm_system_monitor:get_memory_info(darwin),
            ?assert(is_integer(FreeMem)),
            ?assert(is_integer(CachedMem)),
            ?assert(is_integer(AvailMem))
        end},
        {"other platform returns zeros", fun() ->
            {FreeMem, CachedMem, AvailMem} = flurm_system_monitor:get_memory_info(other),
            ?assertEqual(0, FreeMem),
            ?assertEqual(0, CachedMem),
            ?assertEqual(0, AvailMem)
        end}
    ].

%%====================================================================
%% get_load_average/1 Tests
%%====================================================================

get_load_average_test_() ->
    [
        {"returns tuple with three floats", fun() ->
            Platform = flurm_system_monitor:detect_platform(),
            Result = flurm_system_monitor:get_load_average(Platform),
            ?assertMatch({_, _, _}, Result),
            {L1, L5, L15} = Result,
            ?assert(is_float(L1)),
            ?assert(is_float(L5)),
            ?assert(is_float(L15))
        end},
        {"linux platform returns load average", fun() ->
            {L1, L5, L15} = flurm_system_monitor:get_load_average(linux),
            ?assert(is_float(L1)),
            ?assert(is_float(L5)),
            ?assert(is_float(L15))
        end},
        {"darwin platform returns load average", fun() ->
            {L1, L5, L15} = flurm_system_monitor:get_load_average(darwin),
            ?assert(is_float(L1)),
            ?assert(is_float(L5)),
            ?assert(is_float(L15))
        end},
        {"other platform uses Erlang run queue", fun() ->
            {L1, L5, L15} = flurm_system_monitor:get_load_average(other),
            ?assert(is_float(L1)),
            ?assert(is_float(L5)),
            ?assert(is_float(L15)),
            %% For 'other' platform, all three values are the same (run queue)
            ?assertEqual(L1, L5),
            ?assertEqual(L5, L15)
        end},
        {"load averages are non-negative", fun() ->
            Platform = flurm_system_monitor:detect_platform(),
            {L1, L5, L15} = flurm_system_monitor:get_load_average(Platform),
            ?assert(L1 >= 0.0),
            ?assert(L5 >= 0.0),
            ?assert(L15 >= 0.0)
        end}
    ].

%%====================================================================
%% parse_meminfo_field/2 Tests
%%====================================================================

parse_meminfo_field_test_() ->
    [
        {"parses MemTotal field", fun() ->
            Content = <<"MemTotal:       16384000 kB\nMemFree:         1024000 kB\n">>,
            Result = flurm_system_monitor:parse_meminfo_field(Content, <<"MemTotal:">>),
            ?assertEqual(16384000, Result)
        end},
        {"parses MemFree field", fun() ->
            Content = <<"MemTotal:       16384000 kB\nMemFree:         1024000 kB\n">>,
            Result = flurm_system_monitor:parse_meminfo_field(Content, <<"MemFree:">>),
            ?assertEqual(1024000, Result)
        end},
        {"returns 0 for missing field", fun() ->
            Content = <<"MemTotal:       16384000 kB\n">>,
            Result = flurm_system_monitor:parse_meminfo_field(Content, <<"NonExistent:">>),
            ?assertEqual(0, Result)
        end},
        {"handles field with leading whitespace", fun() ->
            Content = <<"MemAvailable:   8192000 kB\n">>,
            Result = flurm_system_monitor:parse_meminfo_field(Content, <<"MemAvailable:">>),
            ?assertEqual(8192000, Result)
        end},
        {"handles empty content", fun() ->
            Result = flurm_system_monitor:parse_meminfo_field(<<>>, <<"MemTotal:">>),
            ?assertEqual(0, Result)
        end},
        {"parses Cached field", fun() ->
            Content = <<"Cached:         4096000 kB\n">>,
            Result = flurm_system_monitor:parse_meminfo_field(Content, <<"Cached:">>),
            ?assertEqual(4096000, Result)
        end},
        {"parses Buffers field", fun() ->
            Content = <<"Buffers:          512000 kB\n">>,
            Result = flurm_system_monitor:parse_meminfo_field(Content, <<"Buffers:">>),
            ?assertEqual(512000, Result)
        end}
    ].

%%====================================================================
%% parse_vm_stat_line/2 Tests
%%====================================================================

parse_vm_stat_line_test_() ->
    [
        {"parses Pages free", fun() ->
            Lines = ["Mach Virtual Memory Statistics: (page size of 4096 bytes)",
                     "Pages free:                               123456.",
                     "Pages active:                             789012."],
            Result = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages free"),
            ?assertEqual(123456, Result)
        end},
        {"parses Pages inactive", fun() ->
            Lines = ["Pages inactive:                           654321."],
            Result = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages inactive"),
            ?assertEqual(654321, Result)
        end},
        {"returns 0 for missing field", fun() ->
            Lines = ["Pages free:                               123456."],
            Result = flurm_system_monitor:parse_vm_stat_line(Lines, "NonExistent"),
            ?assertEqual(0, Result)
        end},
        {"handles empty list", fun() ->
            Result = flurm_system_monitor:parse_vm_stat_line([], "Pages free"),
            ?assertEqual(0, Result)
        end},
        {"handles malformed line", fun() ->
            Lines = ["Pages free: no number here"],
            Result = flurm_system_monitor:parse_vm_stat_line(Lines, "Pages free"),
            ?assertEqual(0, Result)
        end}
    ].

%%====================================================================
%% erlang_memory_fallback/0 Tests
%%====================================================================

erlang_memory_fallback_test_() ->
    [
        {"returns positive integer", fun() ->
            Mem = flurm_system_monitor:erlang_memory_fallback(),
            ?assert(is_integer(Mem)),
            ?assert(Mem > 0)
        end}
    ].

%%====================================================================
%% binary_to_float_safe/1 Tests
%%====================================================================

binary_to_float_safe_test_() ->
    [
        {"converts float binary", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<"1.23">>),
            ?assertEqual(1.23, Result)
        end},
        {"converts integer binary to float", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<"42">>),
            ?assertEqual(42.0, Result)
        end},
        {"handles binary with whitespace", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<" 3.14 ">>),
            ?assertEqual(3.14, Result)
        end},
        {"handles invalid binary", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<"not_a_number">>),
            ?assertEqual(0.0, Result)
        end},
        {"handles empty binary", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<>>),
            ?assertEqual(0.0, Result)
        end},
        {"handles zero", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<"0.0">>),
            ?assertEqual(0.0, Result)
        end},
        {"handles scientific notation", fun() ->
            Result = flurm_system_monitor:binary_to_float_safe(<<"1.5e2">>),
            ?assertEqual(150.0, Result)
        end}
    ].

%%====================================================================
%% list_to_float_safe/1 Tests
%%====================================================================

list_to_float_safe_test_() ->
    [
        {"converts float string", fun() ->
            Result = flurm_system_monitor:list_to_float_safe("1.23"),
            ?assertEqual(1.23, Result)
        end},
        {"converts integer string to float", fun() ->
            Result = flurm_system_monitor:list_to_float_safe("42"),
            ?assertEqual(42.0, Result)
        end},
        {"handles string with whitespace", fun() ->
            Result = flurm_system_monitor:list_to_float_safe("  3.14  "),
            ?assertEqual(3.14, Result)
        end},
        {"handles invalid string", fun() ->
            Result = flurm_system_monitor:list_to_float_safe("not_a_number"),
            ?assertEqual(0.0, Result)
        end},
        {"handles empty string", fun() ->
            Result = flurm_system_monitor:list_to_float_safe(""),
            ?assertEqual(0.0, Result)
        end},
        {"handles zero", fun() ->
            Result = flurm_system_monitor:list_to_float_safe("0"),
            ?assertEqual(0.0, Result)
        end},
        {"handles negative float", fun() ->
            Result = flurm_system_monitor:list_to_float_safe("-1.5"),
            ?assertEqual(-1.5, Result)
        end}
    ].

%%====================================================================
%% detect_gpus/0 Tests
%%====================================================================

detect_gpus_test_() ->
    [
        {"returns list", fun() ->
            GPUs = flurm_system_monitor:detect_gpus(),
            ?assert(is_list(GPUs))
        end},
        {"each GPU is a map with required fields", fun() ->
            GPUs = flurm_system_monitor:detect_gpus(),
            lists:foreach(fun(GPU) ->
                ?assert(is_map(GPU)),
                ?assert(maps:is_key(index, GPU)),
                ?assert(maps:is_key(name, GPU)),
                ?assert(maps:is_key(type, GPU)),
                ?assert(maps:is_key(memory_mb, GPU))
            end, GPUs)
        end}
    ].

%%====================================================================
%% detect_nvidia_gpus/0 Tests
%%====================================================================

detect_nvidia_gpus_test_() ->
    [
        {"returns list", fun() ->
            GPUs = flurm_system_monitor:detect_nvidia_gpus(),
            ?assert(is_list(GPUs))
        end},
        {"nvidia GPUs have correct type", fun() ->
            GPUs = flurm_system_monitor:detect_nvidia_gpus(),
            lists:foreach(fun(GPU) ->
                ?assertEqual(nvidia, maps:get(type, GPU))
            end, GPUs)
        end}
    ].

%%====================================================================
%% parse_nvidia_gpu/1 Tests
%%====================================================================

parse_nvidia_gpu_test_() ->
    [
        {"parses valid nvidia-smi output line", fun() ->
            Line = "0, NVIDIA GeForce RTX 3090, 24576",
            Result = flurm_system_monitor:parse_nvidia_gpu(Line),
            ?assertMatch({true, #{index := 0, name := _, type := nvidia, memory_mb := 24576}}, Result),
            {true, GPU} = Result,
            ?assertEqual(0, maps:get(index, GPU)),
            ?assertEqual(<<"NVIDIA GeForce RTX 3090">>, maps:get(name, GPU)),
            ?assertEqual(nvidia, maps:get(type, GPU)),
            ?assertEqual(24576, maps:get(memory_mb, GPU))
        end},
        {"parses line with different GPU index", fun() ->
            Line = "2, NVIDIA Tesla V100, 16384",
            Result = flurm_system_monitor:parse_nvidia_gpu(Line),
            ?assertMatch({true, #{index := 2}}, Result)
        end},
        {"returns false for empty line", fun() ->
            Result = flurm_system_monitor:parse_nvidia_gpu(""),
            ?assertEqual(false, Result)
        end},
        {"returns false for whitespace only", fun() ->
            Result = flurm_system_monitor:parse_nvidia_gpu("   "),
            ?assertEqual(false, Result)
        end},
        {"returns false for malformed line", fun() ->
            Result = flurm_system_monitor:parse_nvidia_gpu("invalid, data"),
            ?assertEqual(false, Result)
        end},
        {"returns false for line missing fields", fun() ->
            Result = flurm_system_monitor:parse_nvidia_gpu("0, GPU Name"),
            ?assertEqual(false, Result)
        end},
        {"returns false for non-integer index", fun() ->
            Result = flurm_system_monitor:parse_nvidia_gpu("abc, GPU Name, 1024"),
            ?assertEqual(false, Result)
        end},
        {"returns false for non-integer memory", fun() ->
            Result = flurm_system_monitor:parse_nvidia_gpu("0, GPU Name, notmemory"),
            ?assertEqual(false, Result)
        end}
    ].

%%====================================================================
%% detect_amd_gpus/0 Tests
%%====================================================================

detect_amd_gpus_test_() ->
    [
        {"returns list", fun() ->
            GPUs = flurm_system_monitor:detect_amd_gpus(),
            ?assert(is_list(GPUs))
        end},
        {"amd GPUs have correct type", fun() ->
            GPUs = flurm_system_monitor:detect_amd_gpus(),
            lists:foreach(fun(GPU) ->
                ?assertEqual(amd, maps:get(type, GPU))
            end, GPUs)
        end}
    ].

%%====================================================================
%% check_amd_vendor/1 Tests
%%====================================================================

check_amd_vendor_test_() ->
    [
        {"returns false for non-existent file", fun() ->
            Result = flurm_system_monitor:check_amd_vendor("/nonexistent/path/vendor"),
            ?assertEqual(false, Result)
        end}
    ].

%%====================================================================
%% get_disk_info/0 Tests
%%====================================================================

get_disk_info_test_() ->
    [
        {"returns map", fun() ->
            DiskInfo = flurm_system_monitor:get_disk_info(),
            ?assert(is_map(DiskInfo))
        end},
        {"disk info entries have required fields", fun() ->
            DiskInfo = flurm_system_monitor:get_disk_info(),
            maps:foreach(fun(_MountPoint, Info) ->
                ?assert(is_map(Info)),
                ?assert(maps:is_key(total_mb, Info)),
                ?assert(maps:is_key(used_mb, Info)),
                ?assert(maps:is_key(available_mb, Info)),
                ?assert(maps:is_key(percent_used, Info))
            end, DiskInfo)
        end}
    ].

%%====================================================================
%% get_mount_usage/1 Tests
%%====================================================================

get_mount_usage_test_() ->
    [
        {"returns usage for root mount", fun() ->
            Result = flurm_system_monitor:get_mount_usage("/"),
            ?assertMatch({true, {<<"/"/utf8>>, _}}, Result),
            {true, {_, Info}} = Result,
            ?assert(is_map(Info)),
            ?assert(maps:is_key(total_mb, Info)),
            ?assert(maps:is_key(used_mb, Info)),
            ?assert(maps:is_key(available_mb, Info)),
            ?assert(maps:is_key(percent_used, Info))
        end},
        {"returns false for nonexistent mount", fun() ->
            Result = flurm_system_monitor:get_mount_usage("/nonexistent_mount_xyz123"),
            ?assertEqual(false, Result)
        end},
        {"percent_used is between 0 and 100", fun() ->
            case flurm_system_monitor:get_mount_usage("/") of
                {true, {_, Info}} ->
                    Percent = maps:get(percent_used, Info),
                    ?assert(Percent >= 0),
                    ?assert(Percent =< 100);
                false ->
                    ok
            end
        end}
    ].
