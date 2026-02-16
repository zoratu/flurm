%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_system_monitor module
%%%
%%% Tests the system monitoring functions that gather metrics from
%%% the local system. These tests call real functions without mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_system_monitor_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Helper Functions (exported via -ifdef(TEST))
%%====================================================================

%% Test get_cpu_count function
get_cpu_count_test_() ->
    [
     {"get_cpu_count returns positive integer",
      fun() ->
          Count = flurm_system_monitor:get_cpu_count(),
          ?assert(is_integer(Count)),
          ?assert(Count > 0)
      end}
    ].

%% Test get_memory_mb function
get_memory_mb_test_() ->
    [
     {"get_memory_mb returns positive integer",
      fun() ->
          Memory = flurm_system_monitor:get_memory_mb(),
          ?assert(is_integer(Memory)),
          ?assert(Memory > 0)
      end},
     {"get_memory_mb returns reasonable value (> 100 MB)",
      fun() ->
          Memory = flurm_system_monitor:get_memory_mb(),
          ?assert(Memory > 100)  %% Any real system should have > 100MB
      end}
    ].

%% Test get_free_memory_mb function
get_free_memory_mb_test_() ->
    [
     {"get_free_memory_mb returns non-negative integer",
      fun() ->
          Free = flurm_system_monitor:get_free_memory_mb(),
          ?assert(is_integer(Free)),
          ?assert(Free >= 0)
      end},
     {"free memory is less than or equal to total memory",
      fun() ->
          Total = flurm_system_monitor:get_memory_mb(),
          Free = flurm_system_monitor:get_free_memory_mb(),
          ?assert(Free =< Total)
      end}
    ].

%% Test get_load_avg function
get_load_avg_test_() ->
    [
     {"get_load_avg returns non-negative float",
      fun() ->
          Load = flurm_system_monitor:get_load_avg(),
          ?assert(is_float(Load)),
          ?assert(Load >= 0.0)
      end}
    ].

%% Test get_hostname function
get_hostname_test_() ->
    [
     {"get_hostname returns binary",
      fun() ->
          Hostname = flurm_system_monitor:get_hostname(),
          ?assert(is_binary(Hostname))
      end},
     {"get_hostname returns non-empty",
      fun() ->
          Hostname = flurm_system_monitor:get_hostname(),
          ?assert(byte_size(Hostname) > 0)
      end}
    ].

%% Test get_uptime_seconds function
get_uptime_seconds_test_() ->
    [
     {"get_uptime_seconds returns non-negative integer",
      fun() ->
          Uptime = flurm_system_monitor:get_uptime_seconds(),
          ?assert(is_integer(Uptime)),
          ?assert(Uptime >= 0)
      end}
    ].

%% Test get_disk_usage function
get_disk_usage_test_() ->
    [
     {"get_disk_usage returns a map",
      fun() ->
          Usage = flurm_system_monitor:get_disk_usage(),
          ?assert(is_map(Usage))
      end},
     {"get_disk_usage for /tmp contains expected fields",
      fun() ->
          Usage = flurm_system_monitor:get_disk_usage(<<"/tmp">>),
          ?assert(is_map(Usage)),
          case maps:size(Usage) > 0 of
              true ->
                  %% If path exists, should have total, used, free
                  ?assert(maps:is_key(total, Usage) orelse maps:is_key(error, Usage));
              false ->
                  ok
          end
      end}
    ].

%% Test get_network_interfaces function
get_network_interfaces_test_() ->
    [
     {"get_network_interfaces returns a list",
      fun() ->
          Interfaces = flurm_system_monitor:get_network_interfaces(),
          ?assert(is_list(Interfaces))
      end}
    ].

%% Test build_metrics function
build_metrics_test_() ->
    [
     {"build_metrics returns complete map",
      fun() ->
          Metrics = flurm_system_monitor:build_metrics(),
          ?assert(is_map(Metrics)),
          %% Check required fields exist
          ?assert(maps:is_key(hostname, Metrics)),
          ?assert(maps:is_key(cpus, Metrics)),
          ?assert(maps:is_key(memory_mb, Metrics)),
          ?assert(maps:is_key(free_memory_mb, Metrics)),
          ?assert(maps:is_key(load_avg, Metrics)),
          ?assert(maps:is_key(timestamp, Metrics))
      end},
     {"build_metrics has correct types",
      fun() ->
          Metrics = flurm_system_monitor:build_metrics(),
          ?assert(is_binary(maps:get(hostname, Metrics))),
          ?assert(is_integer(maps:get(cpus, Metrics))),
          ?assert(is_integer(maps:get(memory_mb, Metrics))),
          ?assert(is_integer(maps:get(free_memory_mb, Metrics))),
          ?assert(is_float(maps:get(load_avg, Metrics))),
          ?assert(is_integer(maps:get(timestamp, Metrics)))
      end}
    ].

%% Test parse_meminfo function
parse_meminfo_test_() ->
    [
     {"parse_meminfo handles valid input",
      fun() ->
          Input = "MemTotal:       16384000 kB\nMemFree:         8192000 kB\nMemAvailable:   10240000 kB\n",
          Result = flurm_system_monitor:parse_meminfo(Input),
          ?assert(is_map(Result)),
          ?assertEqual(16384000, maps:get(mem_total_kb, Result)),
          ?assertEqual(8192000, maps:get(mem_free_kb, Result)),
          ?assertEqual(10240000, maps:get(mem_available_kb, Result))
      end},
     {"parse_meminfo handles empty input",
      fun() ->
          Result = flurm_system_monitor:parse_meminfo(""),
          ?assertEqual(#{}, Result)
      end},
     {"parse_meminfo handles malformed input",
      fun() ->
          Result = flurm_system_monitor:parse_meminfo("garbage data"),
          ?assertEqual(#{}, Result)
      end}
    ].

%% Test parse_loadavg function
parse_loadavg_test_() ->
    [
     {"parse_loadavg handles valid input",
      fun() ->
          Input = "0.50 0.75 1.00 2/100 12345",
          Result = flurm_system_monitor:parse_loadavg(Input),
          ?assertEqual(0.50, Result)
      end},
     {"parse_loadavg handles empty input",
      fun() ->
          Result = flurm_system_monitor:parse_loadavg(""),
          ?assertEqual(0.0, Result)
      end},
     {"parse_loadavg handles malformed input",
      fun() ->
          Result = flurm_system_monitor:parse_loadavg("not a number"),
          ?assertEqual(0.0, Result)
      end}
    ].

%%====================================================================
%% Integration Tests with Real Server
%%====================================================================

server_integration_test_() ->
    {setup,
     fun() ->
         %% Start required applications
         application:start(lager),
         %% Start the monitor server
         {ok, Pid} = flurm_system_monitor:start_link(),
         Pid
     end,
     fun(Pid) ->
         %% Stop the server
         gen_server:stop(Pid)
     end,
     [
      {"get_metrics through API returns valid metrics",
       fun() ->
           Metrics = flurm_system_monitor:get_metrics(),
           ?assert(is_map(Metrics)),
           ?assert(maps:is_key(hostname, Metrics)),
           ?assert(maps:is_key(cpus, Metrics))
       end}
     ]}.
