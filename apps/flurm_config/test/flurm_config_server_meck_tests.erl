%%%-------------------------------------------------------------------
%%% @doc Comprehensive meck-based tests for flurm_config_server
%%%
%%% Tests all exported functions with proper mocking of:
%%% - File system operations
%%% - Application environment
%%% - External dependencies
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_server_meck_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup for all tests - clean up any lingering mocks
meck_setup() ->
    %% Ensure no mocks are lingering from previous runs
    catch meck:unload(file),
    catch meck:unload(filelib),
    catch meck:unload(application),
    catch meck:unload(error_logger),
    catch meck:unload(flurm_config_slurm),
    catch meck:unload(flurm_config_defaults),
    catch meck:unload(flurm_scheduler),
    %% Stop any lingering server
    case whereis(flurm_config_server) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            exit(Pid, kill),
            flurm_test_utils:wait_for_death(Pid)
    end,
    ok.

meck_cleanup(_) ->
    %% Stop the server if running
    case whereis(flurm_config_server) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, shutdown, 1000)
    end,
    %% Unload all mocks
    catch meck:unload(),
    ok.

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"init with config_file option loads file",
         fun() ->
             setup_basic_mocks(),
             meck:expect(flurm_config_slurm, parse_file, fun("/custom/path.conf") ->
                 {ok, #{clustername => <<"test">>}}
             end),

             {ok, Pid} = flurm_config_server:start_link([{config_file, "/custom/path.conf"}]),
             ?assertEqual(<<"test">>, flurm_config_server:get(clustername)),
             gen_server:stop(Pid)
         end},
        {"init with app env config_file loads file",
         fun() ->
             setup_basic_mocks(),
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_config, config_file) ->
                 {ok, "/app/env/path.conf"}
             end),
             meck:expect(flurm_config_slurm, parse_file, fun("/app/env/path.conf") ->
                 {ok, #{clustername => <<"from_app_env">>}}
             end),

             {ok, Pid} = flurm_config_server:start_link([]),
             ?assertEqual(<<"from_app_env">>, flurm_config_server:get(clustername)),
             gen_server:stop(Pid)
         end},
        {"init without config_file uses defaults",
         fun() ->
             setup_basic_mocks(),
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_config, config_file) -> undefined end),
             meck:expect(filelib, is_file, fun(_) -> false end),

             {ok, Pid} = flurm_config_server:start_link([]),
             ?assertEqual(#{}, flurm_config_server:get_all()),
             gen_server:stop(Pid)
         end},
        {"init handles config file load error gracefully",
         fun() ->
             setup_basic_mocks(),
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {error, {file_error, "/bad", enoent}}
             end),

             {ok, Pid} = flurm_config_server:start_link([{config_file, "/bad/path.conf"}]),
             %% Should start with empty config, not crash
             ?assertEqual(#{}, flurm_config_server:get_all()),
             gen_server:stop(Pid)
         end},
        {"init finds default config file",
         fun() ->
             setup_basic_mocks(),
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_config, config_file) -> undefined end),
             meck:expect(filelib, is_file, fun
                 ("/etc/flurm/flurm.conf") -> false;
                 ("/etc/flurm/slurm.conf") -> false;
                 ("/etc/slurm/slurm.conf") -> true;
                 (_) -> false
             end),
             meck:expect(flurm_config_slurm, parse_file, fun("/etc/slurm/slurm.conf") ->
                 {ok, #{clustername => <<"default_found">>}}
             end),

             {ok, Pid} = flurm_config_server:start_link([]),
             ?assertEqual(<<"default_found">>, flurm_config_server:get(clustername)),
             gen_server:stop(Pid)
         end}
     ]}.

%%====================================================================
%% Get/Set Tests
%%====================================================================

get_set_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"get returns undefined for missing key",
         fun() ->
             ?assertEqual(undefined, flurm_config_server:get(nonexistent_key))
         end},
        {"get returns default for missing key",
         fun() ->
             ?assertEqual(my_default, flurm_config_server:get(nonexistent_key, my_default))
         end},
        {"set stores value",
         fun() ->
             ok = flurm_config_server:set(test_key, test_value),
             ?assertEqual(test_value, flurm_config_server:get(test_key))
         end},
        {"set overwrites existing value",
         fun() ->
             ok = flurm_config_server:set(key, value1),
             ok = flurm_config_server:set(key, value2),
             ?assertEqual(value2, flurm_config_server:get(key))
         end},
        {"get_all returns entire config",
         fun() ->
             ok = flurm_config_server:set(a, 1),
             ok = flurm_config_server:set(b, 2),
             Config = flurm_config_server:get_all(),
             ?assertEqual(1, maps:get(a, Config)),
             ?assertEqual(2, maps:get(b, Config))
         end},
        {"set increments version",
         fun() ->
             V1 = flurm_config_server:get_version(),
             ok = flurm_config_server:set(key, value),
             V2 = flurm_config_server:get_version(),
             ?assertEqual(V1 + 1, V2)
         end}
     ]}.

%%====================================================================
%% Subscription Tests
%%====================================================================

subscription_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"subscribe receives legacy format notifications",
         fun() ->
             ok = flurm_config_server:subscribe(),
             ok = flurm_config_server:set(notify_key, notify_value),
             receive
                 {config_changed, Version, Changes} ->
                     ?assert(is_integer(Version)),
                     ?assert(lists:member({notify_key, notify_value}, Changes))
             after 1000 ->
                 error(no_notification_received)
             end
         end},
        {"duplicate subscribe is idempotent",
         fun() ->
             ok = flurm_config_server:subscribe(),
             ok = flurm_config_server:subscribe(),
             ok = flurm_config_server:set(key, value),
             %% Should only get one notification
             receive
                 {config_changed, _, _} -> ok
             after 500 ->
                 error(no_notification_received)
             end,
             receive
                 {config_changed, _, _} -> error(duplicate_notification)
             after 100 ->
                 ok
             end
         end},
        {"unsubscribe stops notifications",
         fun() ->
             ok = flurm_config_server:subscribe(),
             ok = flurm_config_server:unsubscribe(),
             ok = flurm_config_server:set(key, value),
             receive
                 {config_changed, _, _} -> error(should_not_receive_after_unsubscribe)
             after 100 ->
                 ok
             end
         end},
        {"subscribe_changes with all keys receives all changes",
         fun() ->
             ok = flurm_config_server:subscribe_changes(all),
             ok = flurm_config_server:set(any_key, any_value),
             receive
                 {config_changed, any_key, undefined, any_value} -> ok
             after 1000 ->
                 error(no_notification_received)
             end
         end},
        {"subscribe_changes with specific keys filters notifications",
         fun() ->
             ok = flurm_config_server:subscribe_changes([watched_key]),
             ok = flurm_config_server:set(watched_key, watched_value),
             receive
                 {config_changed, watched_key, undefined, watched_value} -> ok
             after 1000 ->
                 error(no_notification_for_watched_key)
             end,

             ok = flurm_config_server:set(unwatched_key, some_value),
             receive
                 {config_changed, unwatched_key, _, _} -> error(should_not_receive_for_unwatched)
             after 100 ->
                 ok
             end
         end},
        {"unsubscribe_changes stops filtered notifications",
         fun() ->
             ok = flurm_config_server:subscribe_changes(all),
             ok = flurm_config_server:unsubscribe_changes(),
             ok = flurm_config_server:set(key, value),
             receive
                 {config_changed, _, _, _} -> error(should_not_receive_after_unsubscribe)
             after 100 ->
                 ok
             end
         end}
     ]}.

%%====================================================================
%% Reconfigure Tests
%%====================================================================

reconfigure_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"reconfigure without config file returns error",
         fun() ->
             Result = flurm_config_server:reconfigure(),
             ?assertEqual({error, no_config_file}, Result)
         end},
        {"reconfigure with valid .conf file",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun("/test.conf") ->
                 {ok, #{slurmctldport => 6817}}
             end),

             Result = flurm_config_server:reconfigure("/test.conf"),
             ?assertEqual(ok, Result),
             ?assertEqual(6817, flurm_config_server:get(slurmctldport))
         end},
        {"reconfigure with valid .config file",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, consult, fun("/test.config") ->
                 {ok, [#{clustername => <<"erlang_config">>}]}
             end),

             Result = flurm_config_server:reconfigure("/test.config"),
             ?assertEqual(ok, Result),
             ?assertEqual(<<"erlang_config">>, flurm_config_server:get(clustername))
         end},
        {"reconfigure with file error",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {error, {file_error, "/bad", enoent}}
             end),

             Result = flurm_config_server:reconfigure("/bad.conf"),
             ?assertMatch({error, {load_failed, _}}, Result)
         end},
        {"reconfigure with validation error",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{slurmctldport => -1}}
             end),

             Result = flurm_config_server:reconfigure("/invalid.conf"),
             ?assertMatch({error, {validation_failed, _}}, Result)
         end},
        {"reconfigure notifies subscribers of changes",
         fun() ->
             ok = flurm_config_server:subscribe(),

             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{new_key => new_value}}
             end),

             ok = flurm_config_server:reconfigure("/test.conf"),

             receive
                 {config_changed, _, Changes} ->
                     ?assert(lists:member({new_key, new_value}, Changes))
             after 1000 ->
                 error(no_notification_received)
             end
         end},
        {"reload is alias for reconfigure",
         fun() ->
             Result = flurm_config_server:reload(),
             ?assertEqual({error, no_config_file}, Result)
         end},
        {"reload/1 is alias for reconfigure/1",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun("/reload.conf") ->
                 {ok, #{from_reload => true}}
             end),

             ok = flurm_config_server:reload("/reload.conf"),
             ?assertEqual(true, flurm_config_server:get(from_reload))
         end}
     ]}.

%%====================================================================
%% File Loading Edge Cases Tests
%%====================================================================

file_loading_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"load .config file as list terms",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, consult, fun("/list.config") ->
                 {ok, [{key1, value1}, {key2, value2}]}
             end),

             Result = flurm_config_server:reconfigure("/list.config"),
             ?assertEqual(ok, Result),
             ?assertEqual(value1, flurm_config_server:get(key1)),
             ?assertEqual(value2, flurm_config_server:get(key2))
         end},
        {"load file without extension tries slurm format first",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun("/noext") ->
                 {ok, #{from_slurm_parser => true}}
             end),

             Result = flurm_config_server:reconfigure("/noext"),
             ?assertEqual(ok, Result),
             ?assertEqual(true, flurm_config_server:get(from_slurm_parser))
         end},
        {"load file without extension falls back to erlang format",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(flurm_config_slurm, parse_file, fun("/noext_erlang") ->
                 {error, invalid_format}
             end),
             meck:expect(file, consult, fun("/noext_erlang") ->
                 {ok, [#{from_erlang => true}]}
             end),

             Result = flurm_config_server:reconfigure("/noext_erlang"),
             ?assertEqual(ok, Result),
             ?assertEqual(true, flurm_config_server:get(from_erlang))
         end},
        {"load .config file consult error",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, consult, fun("/bad.config") ->
                 {error, {1, erl_parse, "syntax error"}}
             end),

             Result = flurm_config_server:reconfigure("/bad.config"),
             ?assertMatch({error, {load_failed, _}}, Result)
         end}
     ]}.

%%====================================================================
%% Node and Partition Tests
%%====================================================================

node_partition_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"get_nodes returns empty list by default",
         fun() ->
             ?assertEqual([], flurm_config_server:get_nodes())
         end},
        {"get_partitions returns empty list by default",
         fun() ->
             ?assertEqual([], flurm_config_server:get_partitions())
         end},
        {"get_nodes returns stored nodes",
         fun() ->
             Nodes = [#{nodename => <<"node1">>}, #{nodename => <<"node2">>}],
             ok = flurm_config_server:set(nodes, Nodes),
             ?assertEqual(Nodes, flurm_config_server:get_nodes())
         end},
        {"get_partitions returns stored partitions",
         fun() ->
             Parts = [#{partitionname => <<"batch">>}],
             ok = flurm_config_server:set(partitions, Parts),
             ?assertEqual(Parts, flurm_config_server:get_partitions())
         end},
        {"get_node finds node by exact name",
         fun() ->
             meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),
             Nodes = [#{nodename => <<"node1">>, cpus => 8}],
             ok = flurm_config_server:set(nodes, Nodes),
             Node = flurm_config_server:get_node(<<"node1">>),
             ?assertEqual(8, maps:get(cpus, Node))
         end},
        {"get_node finds node from hostlist pattern",
         fun() ->
             meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[1-3]">>) ->
                 [<<"node1">>, <<"node2">>, <<"node3">>]
             end),
             Nodes = [#{nodename => <<"node[1-3]">>, cpus => 16}],
             ok = flurm_config_server:set(nodes, Nodes),
             Node = flurm_config_server:get_node(<<"node2">>),
             ?assertEqual(16, maps:get(cpus, Node))
         end},
        {"get_node returns undefined for missing node",
         fun() ->
             meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),
             Nodes = [#{nodename => <<"node1">>}],
             ok = flurm_config_server:set(nodes, Nodes),
             ?assertEqual(undefined, flurm_config_server:get_node(<<"node99">>))
         end},
        {"get_partition finds partition by name",
         fun() ->
             Parts = [
                 #{partitionname => <<"batch">>, maxtime => 86400},
                 #{partitionname => <<"debug">>, maxtime => 3600}
             ],
             ok = flurm_config_server:set(partitions, Parts),
             Part = flurm_config_server:get_partition(<<"debug">>),
             ?assertEqual(3600, maps:get(maxtime, Part))
         end},
        {"get_partition returns undefined for missing partition",
         fun() ->
             Parts = [#{partitionname => <<"batch">>}],
             ok = flurm_config_server:set(partitions, Parts),
             ?assertEqual(undefined, flurm_config_server:get_partition(<<"nonexistent">>))
         end}
     ]}.

%%====================================================================
%% Version and Last Reload Tests
%%====================================================================

version_reload_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"get_version returns initial version",
         fun() ->
             Version = flurm_config_server:get_version(),
             ?assert(is_integer(Version)),
             ?assert(Version >= 1)
         end},
        {"get_last_reload returns timestamp",
         fun() ->
             LastReload = flurm_config_server:get_last_reload(),
             ?assert(is_integer(LastReload)),
             Now = erlang:system_time(second),
             ?assert(abs(Now - LastReload) < 60)
         end},
        {"reconfigure increments version",
         fun() ->
             V1 = flurm_config_server:get_version(),
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{}}
             end),
             ok = flurm_config_server:reconfigure("/test.conf"),
             V2 = flurm_config_server:get_version(),
             ?assertEqual(V1 + 1, V2)
         end},
        {"reconfigure updates last_reload",
         fun() ->
             LR1 = flurm_config_server:get_last_reload(),
             timer:sleep(1001),  %% Sleep for 1 second to ensure time changes
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{}}
             end),
             ok = flurm_config_server:reconfigure("/test.conf"),
             LR2 = flurm_config_server:get_last_reload(),
             ?assert(LR2 >= LR1)
         end}
     ]}.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"unknown call returns error",
         fun() ->
             Result = gen_server:call(flurm_config_server, {unknown_request, foo}),
             ?assertEqual({error, unknown_request}, Result)
         end},
        {"unknown cast is ignored",
         fun() ->
             ok = gen_server:cast(flurm_config_server, {unknown_cast, bar}),
             _ = sys:get_state(flurm_config_server),
             ?assert(is_process_alive(whereis(flurm_config_server)))
         end},
        {"unknown info message is ignored",
         fun() ->
             whereis(flurm_config_server) ! {unknown_info, baz},
             _ = sys:get_state(flurm_config_server),
             ?assert(is_process_alive(whereis(flurm_config_server)))
         end},
        {"DOWN message removes subscriber",
         fun() ->
             Self = self(),
             Subscriber = spawn(fun() ->
                 ok = flurm_config_server:subscribe(),
                 Self ! subscribed,
                 receive die -> ok end
             end),
             receive subscribed -> ok after 1000 -> error(timeout) end,

             Subscriber ! die,
             flurm_test_utils:wait_for_death(Subscriber),
             _ = sys:get_state(flurm_config_server),

             %% Server should still work
             ok = flurm_config_server:set(key, value),
             ?assertEqual(value, flurm_config_server:get(key))
         end},
        {"DOWN message removes filtered subscriber",
         fun() ->
             Self = self(),
             Subscriber = spawn(fun() ->
                 ok = flurm_config_server:subscribe_changes(all),
                 Self ! subscribed,
                 receive die -> ok end
             end),
             receive subscribed -> ok after 1000 -> error(timeout) end,

             Subscriber ! die,
             flurm_test_utils:wait_for_death(Subscriber),
             _ = sys:get_state(flurm_config_server),

             ?assert(is_process_alive(whereis(flurm_config_server)))
         end}
     ]}.

%%====================================================================
%% Validation Tests
%%====================================================================

validation_test_() ->
    {"validation function tests",
     [
        {"validate_port_numbers accepts valid ports",
         fun() ->
             Config = #{slurmctldport => 6817, slurmdport => 6818},
             ?assertEqual(ok, flurm_config_server:validate_port_numbers(Config))
         end},
        {"validate_port_numbers rejects invalid ports",
         fun() ->
             ?assertMatch({error, {invalid_ports, _}},
                         flurm_config_server:validate_port_numbers(#{slurmctldport => 0})),
             ?assertMatch({error, {invalid_ports, _}},
                         flurm_config_server:validate_port_numbers(#{slurmdport => -1})),
             ?assertMatch({error, {invalid_ports, _}},
                         flurm_config_server:validate_port_numbers(#{slurmdbdport => 70000}))
         end},
        {"validate_node_definitions accepts valid nodes",
         fun() ->
             Config = #{nodes => [#{nodename => <<"node1">>}]},
             ?assertEqual(ok, flurm_config_server:validate_node_definitions(Config))
         end},
        {"validate_node_definitions rejects invalid nodes",
         fun() ->
             Config = #{nodes => [#{cpus => 4}]},
             ?assertMatch({error, {invalid_nodes, _}},
                         flurm_config_server:validate_node_definitions(Config))
         end},
        {"validate_required_keys always returns ok",
         fun() ->
             ?assertEqual(ok, flurm_config_server:validate_required_keys(#{})),
             ?assertEqual(ok, flurm_config_server:validate_required_keys(#{key => val}))
         end},
        {"run_validators with empty list returns ok",
         fun() ->
             ?assertEqual(ok, flurm_config_server:run_validators([], #{}))
         end},
        {"run_validators with all passing returns ok",
         fun() ->
             V1 = fun(_) -> ok end,
             V2 = fun(_) -> ok end,
             ?assertEqual(ok, flurm_config_server:run_validators([V1, V2], #{}))
         end},
        {"run_validators stops on first error",
         fun() ->
             V1 = fun(_) -> ok end,
             V2 = fun(_) -> {error, v2_failed} end,
             V3 = fun(_) -> {error, v3_should_not_run} end,
             ?assertEqual({error, v2_failed},
                         flurm_config_server:run_validators([V1, V2, V3], #{}))
         end}
     ]}.

%%====================================================================
%% Find Changes Tests
%%====================================================================

find_changes_test_() ->
    {"find_changes tests",
     [
        {"find_changes with identical configs returns empty",
         fun() ->
             Config = #{a => 1, b => 2},
             ?assertEqual([], flurm_config_server:find_changes(Config, Config))
         end},
        {"find_changes detects added key",
         fun() ->
             Old = #{a => 1},
             New = #{a => 1, b => 2},
             Changes = flurm_config_server:find_changes(Old, New),
             ?assertEqual([{b, undefined, 2}], Changes)
         end},
        {"find_changes detects removed key",
         fun() ->
             Old = #{a => 1, b => 2},
             New = #{a => 1},
             Changes = flurm_config_server:find_changes(Old, New),
             ?assertEqual([{b, 2, undefined}], Changes)
         end},
        {"find_changes detects changed value",
         fun() ->
             Old = #{a => 1},
             New = #{a => 2},
             Changes = flurm_config_server:find_changes(Old, New),
             ?assertEqual([{a, 1, 2}], Changes)
         end},
        {"find_changes handles empty configs",
         fun() ->
             ?assertEqual([], flurm_config_server:find_changes(#{}, #{}))
         end}
     ]}.

%%====================================================================
%% Find Node/Partition Direct Tests
%%====================================================================

find_node_partition_direct_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks()
     end,
     fun meck_cleanup/1,
     [
        {"find_node with empty list",
         fun() ->
             ?assertEqual(undefined, flurm_config_server:find_node(<<"node1">>, []))
         end},
        {"find_node with missing nodename key",
         fun() ->
             Nodes = [#{cpus => 4}],
             ?assertEqual(undefined, flurm_config_server:find_node(<<"node1">>, Nodes))
         end},
        {"find_node returns first matching node",
         fun() ->
             meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),
             Nodes = [
                 #{nodename => <<"node1">>, order => 1},
                 #{nodename => <<"node1">>, order => 2}
             ],
             Node = flurm_config_server:find_node(<<"node1">>, Nodes),
             ?assertEqual(1, maps:get(order, Node))
         end},
        {"find_partition with empty list",
         fun() ->
             ?assertEqual(undefined, flurm_config_server:find_partition(<<"batch">>, []))
         end},
        {"find_partition with missing partitionname key",
         fun() ->
             Parts = [#{maxtime => 3600}],
             ?assertEqual(undefined, flurm_config_server:find_partition(<<"batch">>, Parts))
         end},
        {"find_partition returns first matching partition",
         fun() ->
             Parts = [
                 #{partitionname => <<"batch">>, order => 1},
                 #{partitionname => <<"batch">>, order => 2}
             ],
             Part = flurm_config_server:find_partition(<<"batch">>, Parts),
             ?assertEqual(1, maps:get(order, Part))
         end}
     ]}.

%%====================================================================
%% Find Default Config Direct Tests
%%====================================================================

find_default_config_direct_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         meck:new(filelib, [passthrough, unstick])
     end,
     fun meck_cleanup/1,
     [
        {"find_default_config finds first existing file",
         fun() ->
             meck:expect(filelib, is_file, fun
                 ("/etc/flurm/flurm.conf") -> false;
                 ("/etc/flurm/slurm.conf") -> true;
                 (_) -> false
             end),
             ?assertEqual("/etc/flurm/slurm.conf", flurm_config_server:find_default_config())
         end},
        {"find_default_config returns undefined when none exist",
         fun() ->
             meck:expect(filelib, is_file, fun(_) -> false end),
             ?assertEqual(undefined, flurm_config_server:find_default_config())
         end}
     ]}.

%%====================================================================
%% Apply Config Changes Tests (with scheduler notification)
%%====================================================================

apply_changes_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         setup_basic_mocks(),
         meck:new(flurm_scheduler, [non_strict]),
         meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"reconfigure with schedulertype change triggers scheduler",
         fun() ->
             ok = flurm_config_server:subscribe(),

             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{schedulertype => <<"sched/backfill">>}}
             end),

             ok = flurm_config_server:reconfigure("/test.conf"),

             %% Verify scheduler was triggered
             ?assert(meck:called(flurm_scheduler, trigger_schedule, []))
         end},
        {"reconfigure with nodes change notifies node registry",
         fun() ->
             %% Register a fake node_registry
             register(flurm_node_registry, self()),

             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{nodes => [#{nodename => <<"node1">>}]}}
             end),

             ok = flurm_config_server:reconfigure("/test.conf"),

             receive
                 {config_reload_nodes, _Nodes} -> ok
             after 1000 ->
                 error(no_node_registry_notification)
             end,

             unregister(flurm_node_registry)
         end},
        {"reconfigure with partitions change notifies partition registry",
         fun() ->
             %% Register a fake partition_registry
             register(flurm_partition_registry, self()),

             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{partitions => [#{partitionname => <<"batch">>}]}}
             end),

             ok = flurm_config_server:reconfigure("/test.conf"),

             receive
                 {config_reload_partitions, _Parts} -> ok
             after 1000 ->
                 error(no_partition_registry_notification)
             end,

             unregister(flurm_partition_registry)
         end}
     ]}.

%%====================================================================
%% Helper Functions
%%====================================================================

setup_basic_mocks() ->
    meck:new(filelib, [passthrough, unstick]),
    meck:new(flurm_config_slurm, [passthrough]),
    meck:new(flurm_config_defaults, [passthrough]),
    meck:new(error_logger, [passthrough, unstick]),

    meck:expect(filelib, is_file, fun(_) -> false end),
    meck:expect(flurm_config_defaults, apply_defaults, fun(C) -> C end),
    meck:expect(error_logger, info_msg, fun(_, _) -> ok end),
    meck:expect(error_logger, warning_msg, fun(_, _) -> ok end),
    meck:expect(error_logger, error_msg, fun(_, _) -> ok end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),
    ok.
