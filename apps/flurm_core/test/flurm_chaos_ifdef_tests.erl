%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_chaos internal functions
%%%
%%% Tests the pure internal functions exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_chaos_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test: increment_stat/2
%%====================================================================

increment_stat_test_() ->
    [
     {"increments new stat to 1",
      fun() ->
          Stats = #{},
          Result = flurm_chaos:increment_stat(kill_random_process, Stats),
          ?assertEqual(1, maps:get(kill_random_process, Result))
      end},

     {"increments existing stat",
      fun() ->
          Stats = #{kill_random_process => 5},
          Result = flurm_chaos:increment_stat(kill_random_process, Stats),
          ?assertEqual(6, maps:get(kill_random_process, Result))
      end},

     {"preserves other stats",
      fun() ->
          Stats = #{trigger_gc => 3, memory_pressure => 2},
          Result = flurm_chaos:increment_stat(cpu_burn, Stats),
          ?assertEqual(3, maps:get(trigger_gc, Result)),
          ?assertEqual(2, maps:get(memory_pressure, Result)),
          ?assertEqual(1, maps:get(cpu_burn, Result))
      end}
    ].

%%====================================================================
%% Test: is_system_process/1
%%====================================================================

is_system_process_test_() ->
    [
     {"identifies init as system process",
      ?_assertEqual(true, flurm_chaos:is_system_process(
                           [{registered_name, init}]))},

     {"identifies code_server as system process",
      ?_assertEqual(true, flurm_chaos:is_system_process(
                           [{registered_name, code_server}]))},

     {"identifies error_logger as system process",
      ?_assertEqual(true, flurm_chaos:is_system_process(
                           [{registered_name, error_logger}]))},

     {"identifies kernel_sup as system process",
      ?_assertEqual(true, flurm_chaos:is_system_process(
                           [{registered_name, kernel_sup}]))},

     {"identifies file_server_2 as system process",
      ?_assertEqual(true, flurm_chaos:is_system_process(
                           [{registered_name, file_server_2}]))},

     {"identifies logger as system process",
      ?_assertEqual(true, flurm_chaos:is_system_process(
                           [{registered_name, logger}]))},

     {"non-system name returns false",
      ?_assertEqual(false, flurm_chaos:is_system_process(
                            [{registered_name, my_process}]))},

     {"unregistered process returns false",
      ?_assertEqual(false, flurm_chaos:is_system_process(
                            [{registered_name, []}]))}
    ].

%%====================================================================
%% Test: is_supervisor/1
%%====================================================================

is_supervisor_test_() ->
    [
     {"identifies supervisor by initial_call",
      ?_assertEqual(true, flurm_chaos:is_supervisor(
                           [{initial_call, {supervisor, kernel, 1}},
                            {dictionary, []}]))},

     {"non-supervisor initial_call returns false",
      ?_assertEqual(false, flurm_chaos:is_supervisor(
                            [{initial_call, {gen_server, init, 1}},
                             {dictionary, []}]))},

     {"process with supervisor dictionary returns true",
      fun() ->
          Info = [{initial_call, {proc_lib, init_p, 5}},
                  {dictionary, [{'$ancestors', [kernel_sup]},
                                {'$initial_call', {supervisor, kernel, 1}}]}],
          ?assertEqual(true, flurm_chaos:is_supervisor(Info))
      end}
    ].

%%====================================================================
%% Test: shuffle_list/1
%%====================================================================

shuffle_list_test_() ->
    [
     {"preserves all elements",
      fun() ->
          List = [1, 2, 3, 4, 5],
          Result = flurm_chaos:shuffle_list(List),
          ?assertEqual(lists:sort(List), lists:sort(Result))
      end},

     {"preserves length",
      fun() ->
          List = lists:seq(1, 100),
          Result = flurm_chaos:shuffle_list(List),
          ?assertEqual(length(List), length(Result))
      end},

     {"empty list returns empty",
      ?_assertEqual([], flurm_chaos:shuffle_list([]))},

     {"single element returns single element",
      ?_assertEqual([1], flurm_chaos:shuffle_list([1]))},

     {"produces different order (probabilistic)",
      fun() ->
          List = lists:seq(1, 20),
          Results = [flurm_chaos:shuffle_list(List) || _ <- lists:seq(1, 10)],
          %% At least some should be different from original
          DifferentCount = length([R || R <- Results, R =/= List]),
          ?assert(DifferentCount > 0)
      end}
    ].

%%====================================================================
%% Test: map_to_delay_config/1
%%====================================================================

map_to_delay_config_test_() ->
    [
     {"creates config with defaults",
      fun() ->
          Result = flurm_chaos:map_to_delay_config(#{}),
          Map = flurm_chaos:delay_config_to_map(Result),
          ?assertEqual(10, maps:get(min_delay_ms, Map)),
          ?assertEqual(500, maps:get(max_delay_ms, Map)),
          ?assertEqual([], maps:get(target_modules, Map))
      end},

     {"creates config with custom values",
      fun() ->
          Input = #{min_delay_ms => 50, max_delay_ms => 1000,
                    target_modules => [my_module]},
          Result = flurm_chaos:map_to_delay_config(Input),
          Map = flurm_chaos:delay_config_to_map(Result),
          ?assertEqual(50, maps:get(min_delay_ms, Map)),
          ?assertEqual(1000, maps:get(max_delay_ms, Map)),
          ?assertEqual([my_module], maps:get(target_modules, Map))
      end}
    ].

%%====================================================================
%% Test: map_to_kill_config/1
%%====================================================================

map_to_kill_config_test_() ->
    [
     {"creates config with defaults",
      fun() ->
          Result = flurm_chaos:map_to_kill_config(#{}),
          Map = flurm_chaos:kill_config_to_map(Result),
          ?assertEqual(1, maps:get(max_kills_per_tick, Map)),
          ?assertEqual(chaos_kill, maps:get(kill_signal, Map)),
          ?assertEqual(true, maps:get(respect_links, Map))
      end},

     {"creates config with custom values",
      fun() ->
          Input = #{max_kills_per_tick => 3, kill_signal => brutal_kill},
          Result = flurm_chaos:map_to_kill_config(Input),
          Map = flurm_chaos:kill_config_to_map(Result),
          ?assertEqual(3, maps:get(max_kills_per_tick, Map)),
          ?assertEqual(brutal_kill, maps:get(kill_signal, Map))
      end}
    ].

%%====================================================================
%% Test: map_to_partition_config/1
%%====================================================================

map_to_partition_config_test_() ->
    [
     {"creates config with defaults",
      fun() ->
          Result = flurm_chaos:map_to_partition_config(#{}),
          Map = flurm_chaos:partition_config_to_map(Result),
          ?assertEqual(5000, maps:get(duration_ms, Map)),
          ?assertEqual(true, maps:get(auto_heal, Map)),
          ?assertEqual(disconnect, maps:get(block_mode, Map))
      end},

     {"creates config with custom values",
      fun() ->
          Input = #{duration_ms => 10000, auto_heal => false, block_mode => filter},
          Result = flurm_chaos:map_to_partition_config(Input),
          Map = flurm_chaos:partition_config_to_map(Result),
          ?assertEqual(10000, maps:get(duration_ms, Map)),
          ?assertEqual(false, maps:get(auto_heal, Map)),
          ?assertEqual(filter, maps:get(block_mode, Map))
      end}
    ].

%%====================================================================
%% Test: map_to_gc_config/1
%%====================================================================

map_to_gc_config_test_() ->
    [
     {"creates config with defaults",
      fun() ->
          Result = flurm_chaos:map_to_gc_config(#{}),
          Map = flurm_chaos:gc_config_to_map(Result),
          ?assertEqual(10, maps:get(max_processes_per_tick, Map)),
          ?assertEqual(undefined, maps:get(target_heap_size, Map)),
          ?assertEqual(false, maps:get(aggressive, Map))
      end},

     {"creates config with custom values",
      fun() ->
          Input = #{max_processes_per_tick => 50, aggressive => true},
          Result = flurm_chaos:map_to_gc_config(Input),
          Map = flurm_chaos:gc_config_to_map(Result),
          ?assertEqual(50, maps:get(max_processes_per_tick, Map)),
          ?assertEqual(true, maps:get(aggressive, Map))
      end}
    ].

%%====================================================================
%% Test: map_to_scheduler_config/1
%%====================================================================

map_to_scheduler_config_test_() ->
    [
     {"creates config with defaults",
      fun() ->
          Result = flurm_chaos:map_to_scheduler_config(#{}),
          Map = flurm_chaos:scheduler_config_to_map(Result),
          ?assertEqual(1, maps:get(min_suspend_ms, Map)),
          ?assertEqual(100, maps:get(max_suspend_ms, Map)),
          ?assertEqual(false, maps:get(affect_dirty_schedulers, Map))
      end},

     {"creates config with custom values",
      fun() ->
          Input = #{min_suspend_ms => 5, max_suspend_ms => 50,
                    affect_dirty_schedulers => true},
          Result = flurm_chaos:map_to_scheduler_config(Input),
          Map = flurm_chaos:scheduler_config_to_map(Result),
          ?assertEqual(5, maps:get(min_suspend_ms, Map)),
          ?assertEqual(50, maps:get(max_suspend_ms, Map)),
          ?assertEqual(true, maps:get(affect_dirty_schedulers, Map))
      end}
    ].

%%====================================================================
%% Test: should_delay_module/2
%%====================================================================

should_delay_module_test_() ->
    [
     {"undefined module always returns true with empty targets",
      fun() ->
          Config = flurm_chaos:map_to_delay_config(#{}),
          ?assertEqual(true, flurm_chaos:should_delay_module(undefined, Config))
      end},

     {"returns true when module not in exclude list",
      fun() ->
          Config = flurm_chaos:map_to_delay_config(#{exclude_modules => [other]}),
          ?assertEqual(true, flurm_chaos:should_delay_module(my_module, Config))
      end},

     {"returns false when module in exclude list",
      fun() ->
          Config = flurm_chaos:map_to_delay_config(#{exclude_modules => [my_module]}),
          ?assertEqual(false, flurm_chaos:should_delay_module(my_module, Config))
      end},

     {"returns true when module in target list",
      fun() ->
          Config = flurm_chaos:map_to_delay_config(#{target_modules => [my_module]}),
          ?assertEqual(true, flurm_chaos:should_delay_module(my_module, Config))
      end},

     {"returns false when module not in target list",
      fun() ->
          Config = flurm_chaos:map_to_delay_config(#{target_modules => [other]}),
          ?assertEqual(false, flurm_chaos:should_delay_module(my_module, Config))
      end},

     {"exclude takes precedence over target",
      fun() ->
          Config = flurm_chaos:map_to_delay_config(
                     #{target_modules => [my_module],
                       exclude_modules => [my_module]}),
          ?assertEqual(false, flurm_chaos:should_delay_module(my_module, Config))
      end}
    ].

%%====================================================================
%% Test: config roundtrip
%%====================================================================

config_roundtrip_test_() ->
    [
     {"delay config roundtrip",
      fun() ->
          Input = #{min_delay_ms => 20, max_delay_ms => 200,
                    target_modules => [a, b], exclude_modules => [c]},
          Config = flurm_chaos:map_to_delay_config(Input),
          Map = flurm_chaos:delay_config_to_map(Config),
          ?assertEqual(20, maps:get(min_delay_ms, Map)),
          ?assertEqual(200, maps:get(max_delay_ms, Map)),
          ?assertEqual([a, b], maps:get(target_modules, Map)),
          ?assertEqual([c], maps:get(exclude_modules, Map))
      end},

     {"kill config roundtrip",
      fun() ->
          Input = #{max_kills_per_tick => 5, kill_signal => test_kill,
                    respect_links => false},
          Config = flurm_chaos:map_to_kill_config(Input),
          Map = flurm_chaos:kill_config_to_map(Config),
          ?assertEqual(5, maps:get(max_kills_per_tick, Map)),
          ?assertEqual(test_kill, maps:get(kill_signal, Map)),
          ?assertEqual(false, maps:get(respect_links, Map))
      end},

     {"partition config roundtrip",
      fun() ->
          Input = #{duration_ms => 15000, auto_heal => false, block_mode => filter},
          Config = flurm_chaos:map_to_partition_config(Input),
          Map = flurm_chaos:partition_config_to_map(Config),
          ?assertEqual(15000, maps:get(duration_ms, Map)),
          ?assertEqual(false, maps:get(auto_heal, Map)),
          ?assertEqual(filter, maps:get(block_mode, Map))
      end},

     {"gc config roundtrip",
      fun() ->
          Input = #{max_processes_per_tick => 25, target_heap_size => 1000000,
                    aggressive => true},
          Config = flurm_chaos:map_to_gc_config(Input),
          Map = flurm_chaos:gc_config_to_map(Config),
          ?assertEqual(25, maps:get(max_processes_per_tick, Map)),
          ?assertEqual(1000000, maps:get(target_heap_size, Map)),
          ?assertEqual(true, maps:get(aggressive, Map))
      end},

     {"scheduler config roundtrip",
      fun() ->
          Input = #{min_suspend_ms => 10, max_suspend_ms => 200,
                    affect_dirty_schedulers => true},
          Config = flurm_chaos:map_to_scheduler_config(Input),
          Map = flurm_chaos:scheduler_config_to_map(Config),
          ?assertEqual(10, maps:get(min_suspend_ms, Map)),
          ?assertEqual(200, maps:get(max_suspend_ms, Map)),
          ?assertEqual(true, maps:get(affect_dirty_schedulers, Map))
      end}
    ].
