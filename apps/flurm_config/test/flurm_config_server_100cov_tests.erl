%%%-------------------------------------------------------------------
%%% @doc FLURM Config Server 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_config_server module covering:
%%% - Configuration get/set operations
%%% - Hot reload and reconfiguration
%%% - Subscription management
%%% - Validation functions
%%% - Node and partition lookups
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_server_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_config.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(flurm_config_slurm),
    catch meck:unload(flurm_config_defaults),
    catch meck:unload(file),
    catch meck:unload(filelib),
    catch meck:unload(error_logger),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    ok.

%%====================================================================
%% Find Default Config Tests
%%====================================================================

find_default_config_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"find_default_config finds first existing file",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_file, fun
                 ("/etc/flurm/flurm.conf") -> false;
                 ("/etc/flurm/slurm.conf") -> true;
                 (_) -> false
             end),

             Result = flurm_config_server:find_default_config(),
             ?assertEqual("/etc/flurm/slurm.conf", Result)
         end},
        {"find_default_config returns undefined when none exist",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_file, fun(_) -> false end),

             Result = flurm_config_server:find_default_config(),
             ?assertEqual(undefined, Result)
         end},
        {"find_default_config checks all candidates in order",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),

             CheckedFiles = ets:new(checked, [public, bag]),
             meck:expect(filelib, is_file, fun(Path) ->
                 ets:insert(CheckedFiles, {path, Path}),
                 false
             end),

             flurm_config_server:find_default_config(),

             Paths = [P || {path, P} <- ets:lookup(CheckedFiles, path)],
             ets:delete(CheckedFiles),

             %% Should have checked all candidates
             ?assert(lists:member("/etc/flurm/flurm.conf", Paths)),
             ?assert(lists:member("/etc/flurm/slurm.conf", Paths)),
             ?assert(lists:member("/etc/slurm/slurm.conf", Paths)),
             ?assert(lists:member("flurm.conf", Paths)),
             ?assert(lists:member("slurm.conf", Paths))
         end}
     ]}.

%%====================================================================
%% Find Changes Tests
%%====================================================================

find_changes_test_() ->
    {"find_changes tests",
     [
        {"find_changes with identical configs",
         fun() ->
             Config = #{key1 => value1, key2 => value2},
             Result = flurm_config_server:find_changes(Config, Config),
             ?assertEqual([], Result)
         end},
        {"find_changes with added key",
         fun() ->
             Old = #{key1 => value1},
             New = #{key1 => value1, key2 => value2},
             Result = flurm_config_server:find_changes(Old, New),
             ?assertEqual([{key2, undefined, value2}], Result)
         end},
        {"find_changes with removed key",
         fun() ->
             Old = #{key1 => value1, key2 => value2},
             New = #{key1 => value1},
             Result = flurm_config_server:find_changes(Old, New),
             ?assertEqual([{key2, value2, undefined}], Result)
         end},
        {"find_changes with changed value",
         fun() ->
             Old = #{key1 => old_value},
             New = #{key1 => new_value},
             Result = flurm_config_server:find_changes(Old, New),
             ?assertEqual([{key1, old_value, new_value}], Result)
         end},
        {"find_changes with multiple changes",
         fun() ->
             Old = #{a => 1, b => 2, c => 3},
             New = #{a => 1, b => 20, d => 4},
             Result = flurm_config_server:find_changes(Old, New),

             ?assertEqual(3, length(Result)),
             ?assert(lists:member({b, 2, 20}, Result)),
             ?assert(lists:member({c, 3, undefined}, Result)),
             ?assert(lists:member({d, undefined, 4}, Result))
         end},
        {"find_changes with empty configs",
         fun() ->
             ?assertEqual([], flurm_config_server:find_changes(#{}, #{}))
         end}
     ]}.

%%====================================================================
%% Validate Port Numbers Tests
%%====================================================================

validate_port_numbers_test_() ->
    {"validate_port_numbers tests",
     [
        {"validate_port_numbers with valid ports",
         fun() ->
             Config = #{slurmctldport => 6817, slurmdport => 6818, slurmdbdport => 6819},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertEqual(ok, Result)
         end},
        {"validate_port_numbers with missing ports is ok",
         fun() ->
             Config = #{slurmctldport => 6817},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertEqual(ok, Result)
         end},
        {"validate_port_numbers with no ports is ok",
         fun() ->
             Config = #{other_key => value},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertEqual(ok, Result)
         end},
        {"validate_port_numbers with invalid port 0",
         fun() ->
             Config = #{slurmctldport => 0},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertMatch({error, {invalid_ports, [slurmctldport]}}, Result)
         end},
        {"validate_port_numbers with invalid port negative",
         fun() ->
             Config = #{slurmdport => -1},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertMatch({error, {invalid_ports, [slurmdport]}}, Result)
         end},
        {"validate_port_numbers with invalid port too high",
         fun() ->
             Config = #{slurmdbdport => 70000},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertMatch({error, {invalid_ports, [slurmdbdport]}}, Result)
         end},
        {"validate_port_numbers with non-integer port",
         fun() ->
             Config = #{slurmctldport => "6817"},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertMatch({error, {invalid_ports, [slurmctldport]}}, Result)
         end},
        {"validate_port_numbers with multiple invalid ports",
         fun() ->
             Config = #{slurmctldport => 0, slurmdport => -5, slurmdbdport => 6819},
             Result = flurm_config_server:validate_port_numbers(Config),
             ?assertMatch({error, {invalid_ports, _}}, Result),
             {error, {invalid_ports, InvalidPorts}} = Result,
             ?assertEqual(2, length(InvalidPorts))
         end}
     ]}.

%%====================================================================
%% Validate Node Definitions Tests
%%====================================================================

validate_node_definitions_test_() ->
    {"validate_node_definitions tests",
     [
        {"validate_node_definitions with valid nodes",
         fun() ->
             Config = #{nodes => [
                 #{nodename => <<"node1">>, cpus => 4},
                 #{nodename => <<"node2">>, cpus => 8}
             ]},
             Result = flurm_config_server:validate_node_definitions(Config),
             ?assertEqual(ok, Result)
         end},
        {"validate_node_definitions with no nodes is ok",
         fun() ->
             Config = #{other_key => value},
             Result = flurm_config_server:validate_node_definitions(Config),
             ?assertEqual(ok, Result)
         end},
        {"validate_node_definitions with empty nodes list is ok",
         fun() ->
             Config = #{nodes => []},
             Result = flurm_config_server:validate_node_definitions(Config),
             ?assertEqual(ok, Result)
         end},
        {"validate_node_definitions with missing nodename",
         fun() ->
             Config = #{nodes => [
                 #{cpus => 4, memory => 8192}  %% Missing nodename
             ]},
             Result = flurm_config_server:validate_node_definitions(Config),
             ?assertMatch({error, {invalid_nodes, _}}, Result)
         end},
        {"validate_node_definitions with mixed valid/invalid",
         fun() ->
             Config = #{nodes => [
                 #{nodename => <<"good">>, cpus => 4},
                 #{cpus => 8}  %% Missing nodename
             ]},
             Result = flurm_config_server:validate_node_definitions(Config),
             ?assertMatch({error, {invalid_nodes, _}}, Result),
             {error, {invalid_nodes, Invalid}} = Result,
             ?assertEqual(1, length(Invalid))
         end}
     ]}.

%%====================================================================
%% Validate Required Keys Tests
%%====================================================================

validate_required_keys_test_() ->
    {"validate_required_keys tests",
     [
        {"validate_required_keys always returns ok",
         fun() ->
             %% Currently no required keys
             ?assertEqual(ok, flurm_config_server:validate_required_keys(#{})),
             ?assertEqual(ok, flurm_config_server:validate_required_keys(#{key => val}))
         end}
     ]}.

%%====================================================================
%% Run Validators Tests
%%====================================================================

run_validators_test_() ->
    {"run_validators tests",
     [
        {"run_validators with empty list",
         fun() ->
             Result = flurm_config_server:run_validators([], #{}),
             ?assertEqual(ok, Result)
         end},
        {"run_validators with all passing",
         fun() ->
             V1 = fun(_) -> ok end,
             V2 = fun(_) -> ok end,
             Result = flurm_config_server:run_validators([V1, V2], #{}),
             ?assertEqual(ok, Result)
         end},
        {"run_validators stops on first error",
         fun() ->
             V1 = fun(_) -> ok end,
             V2 = fun(_) -> {error, v2_failed} end,
             V3 = fun(_) -> {error, v3_should_not_run} end,

             Result = flurm_config_server:run_validators([V1, V2, V3], #{}),
             ?assertEqual({error, v2_failed}, Result)
         end}
     ]}.

%%====================================================================
%% Find Node Tests
%%====================================================================

find_node_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"find_node with exact match",
         fun() ->
             meck:new(flurm_config_slurm, [passthrough]),
             meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),

             Nodes = [
                 #{nodename => <<"node1">>, cpus => 4},
                 #{nodename => <<"node2">>, cpus => 8}
             ],
             Result = flurm_config_server:find_node(<<"node1">>, Nodes),
             ?assertEqual(#{nodename => <<"node1">>, cpus => 4}, Result)
         end},
        {"find_node with pattern expansion",
         fun() ->
             meck:new(flurm_config_slurm, [passthrough]),
             meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[01-03]">>) ->
                 [<<"node01">>, <<"node02">>, <<"node03">>]
             end),

             Nodes = [#{nodename => <<"node[01-03]">>, cpus => 4}],
             Result = flurm_config_server:find_node(<<"node02">>, Nodes),
             ?assertEqual(#{nodename => <<"node[01-03]">>, cpus => 4}, Result)
         end},
        {"find_node not found",
         fun() ->
             meck:new(flurm_config_slurm, [passthrough]),
             meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),

             Nodes = [#{nodename => <<"node1">>, cpus => 4}],
             Result = flurm_config_server:find_node(<<"node99">>, Nodes),
             ?assertEqual(undefined, Result)
         end},
        {"find_node with empty list",
         fun() ->
             Result = flurm_config_server:find_node(<<"node1">>, []),
             ?assertEqual(undefined, Result)
         end},
        {"find_node with missing nodename key",
         fun() ->
             Nodes = [#{cpus => 4}],  %% No nodename
             Result = flurm_config_server:find_node(<<"node1">>, Nodes),
             ?assertEqual(undefined, Result)
         end}
     ]}.

%%====================================================================
%% Find Partition Tests
%%====================================================================

find_partition_test_() ->
    {"find_partition tests",
     [
        {"find_partition with exact match",
         fun() ->
             Partitions = [
                 #{partitionname => <<"batch">>, nodes => <<"node[1-10]">>},
                 #{partitionname => <<"debug">>, nodes => <<"node[11-12]">>}
             ],
             Result = flurm_config_server:find_partition(<<"batch">>, Partitions),
             ?assertEqual(#{partitionname => <<"batch">>, nodes => <<"node[1-10]">>}, Result)
         end},
        {"find_partition not found",
         fun() ->
             Partitions = [#{partitionname => <<"batch">>}],
             Result = flurm_config_server:find_partition(<<"gpu">>, Partitions),
             ?assertEqual(undefined, Result)
         end},
        {"find_partition with empty list",
         fun() ->
             Result = flurm_config_server:find_partition(<<"batch">>, []),
             ?assertEqual(undefined, Result)
         end},
        {"find_partition with missing partitionname key",
         fun() ->
             Partitions = [#{nodes => <<"node1">>}],
             Result = flurm_config_server:find_partition(<<"batch">>, Partitions),
             ?assertEqual(undefined, Result)
         end},
        {"find_partition returns first match",
         fun() ->
             Partitions = [
                 #{partitionname => <<"batch">>, priority => 1},
                 #{partitionname => <<"batch">>, priority => 2}
             ],
             Result = flurm_config_server:find_partition(<<"batch">>, Partitions),
             ?assertEqual(#{partitionname => <<"batch">>, priority => 1}, Result)
         end}
     ]}.

%%====================================================================
%% Gen Server Integration Tests
%%====================================================================

gen_server_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         meck:new(filelib, [passthrough, unstick]),
         meck:new(flurm_config_slurm, [passthrough]),
         meck:new(flurm_config_defaults, [passthrough]),
         meck:new(error_logger, [passthrough, unstick]),

         meck:expect(filelib, is_file, fun(_) -> false end),
         meck:expect(flurm_config_defaults, apply_defaults, fun(C) -> C end),
         meck:expect(error_logger, info_msg, fun(_, _) -> ok end),
         meck:expect(error_logger, warning_msg, fun(_, _) -> ok end),

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
             Result = flurm_config_server:get(nonexistent_key),
             ?assertEqual(undefined, Result)
         end},
        {"get returns default for missing key",
         fun() ->
             Result = flurm_config_server:get(nonexistent_key, my_default),
             ?assertEqual(my_default, Result)
         end},
        {"set stores value",
         fun() ->
             ok = flurm_config_server:set(test_key, test_value),
             Result = flurm_config_server:get(test_key),
             ?assertEqual(test_value, Result)
         end},
        {"set overwrites existing value",
         fun() ->
             ok = flurm_config_server:set(key, value1),
             ok = flurm_config_server:set(key, value2),
             Result = flurm_config_server:get(key),
             ?assertEqual(value2, Result)
         end},
        {"get_all returns full config",
         fun() ->
             ok = flurm_config_server:set(a, 1),
             ok = flurm_config_server:set(b, 2),
             Config = flurm_config_server:get_all(),
             ?assert(is_map(Config)),
             ?assertEqual(1, maps:get(a, Config)),
             ?assertEqual(2, maps:get(b, Config))
         end},
        {"get_version returns positive integer",
         fun() ->
             Version = flurm_config_server:get_version(),
             ?assert(is_integer(Version)),
             ?assert(Version >= 1)
         end},
        {"get_last_reload returns timestamp",
         fun() ->
             LastReload = flurm_config_server:get_last_reload(),
             ?assert(is_integer(LastReload))
         end},
        {"set increments version",
         fun() ->
             V1 = flurm_config_server:get_version(),
             ok = flurm_config_server:set(key, value),
             V2 = flurm_config_server:get_version(),
             ?assertEqual(V1 + 1, V2)
         end},
        {"get_nodes returns empty list by default",
         fun() ->
             Nodes = flurm_config_server:get_nodes(),
             ?assertEqual([], Nodes)
         end},
        {"get_partitions returns empty list by default",
         fun() ->
             Partitions = flurm_config_server:get_partitions(),
             ?assertEqual([], Partitions)
         end},
        {"get_node returns undefined when not found",
         fun() ->
             Node = flurm_config_server:get_node(<<"unknown">>),
             ?assertEqual(undefined, Node)
         end},
        {"get_partition returns undefined when not found",
         fun() ->
             Part = flurm_config_server:get_partition(<<"unknown">>),
             ?assertEqual(undefined, Part)
         end},
        {"reconfigure without file returns error",
         fun() ->
             Result = flurm_config_server:reconfigure(),
             ?assertEqual({error, no_config_file}, Result)
         end}
     ]}.

%%====================================================================
%% Subscription Tests
%%====================================================================

subscription_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         meck:new(filelib, [passthrough, unstick]),
         meck:new(flurm_config_defaults, [passthrough]),
         meck:new(error_logger, [passthrough, unstick]),

         meck:expect(filelib, is_file, fun(_) -> false end),
         meck:expect(flurm_config_defaults, apply_defaults, fun(C) -> C end),
         meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"subscribe receives notifications",
         fun() ->
             ok = flurm_config_server:subscribe(),
             ok = flurm_config_server:set(notify_key, notify_value),

             receive
                 {config_changed, _Version, Changes} ->
                     ?assert(lists:member({notify_key, notify_value}, Changes))
             after 1000 ->
                 error(no_notification_received)
             end
         end},
        {"unsubscribe stops notifications",
         fun() ->
             ok = flurm_config_server:subscribe(),
             ok = flurm_config_server:unsubscribe(),
             ok = flurm_config_server:set(key, value),

             receive
                 {config_changed, _, _} ->
                     error(should_not_receive_notification)
             after 100 ->
                 ok
             end
         end},
        {"subscribe_changes with all keys",
         fun() ->
             ok = flurm_config_server:subscribe_changes(all),
             ok = flurm_config_server:set(any_key, any_value),

             receive
                 {config_changed, any_key, undefined, any_value} ->
                     ok
             after 1000 ->
                 error(no_notification_received)
             end
         end},
        {"subscribe_changes with specific keys",
         fun() ->
             ok = flurm_config_server:subscribe_changes([watched_key]),
             ok = flurm_config_server:set(watched_key, watched_value),

             receive
                 {config_changed, watched_key, undefined, watched_value} ->
                     ok
             after 1000 ->
                 error(no_notification_received)
             end
         end},
        {"subscribe_changes filters unwatched keys",
         fun() ->
             ok = flurm_config_server:subscribe_changes([watched_key]),
             ok = flurm_config_server:set(unwatched_key, some_value),

             receive
                 {config_changed, unwatched_key, _, _} ->
                     error(should_not_receive_for_unwatched_key)
             after 100 ->
                 ok
             end
         end},
        {"unsubscribe_changes stops notifications",
         fun() ->
             ok = flurm_config_server:subscribe_changes(all),
             ok = flurm_config_server:unsubscribe_changes(),
             ok = flurm_config_server:set(key, value),

             receive
                 {config_changed, _, _, _} ->
                     error(should_not_receive_notification)
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
         meck:new(file, [passthrough, unstick]),
         meck:new(filelib, [passthrough, unstick]),
         meck:new(flurm_config_slurm, [passthrough]),
         meck:new(flurm_config_defaults, [passthrough]),
         meck:new(error_logger, [passthrough, unstick]),

         meck:expect(filelib, is_file, fun(_) -> false end),
         meck:expect(flurm_config_defaults, apply_defaults, fun(C) -> C end),
         meck:expect(error_logger, info_msg, fun(_, _) -> ok end),
         meck:expect(error_logger, warning_msg, fun(_, _) -> ok end),
         meck:expect(error_logger, error_msg, fun(_, _) -> ok end),

         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"reconfigure with valid file",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{slurmctldport => 6817}}
             end),

             Result = flurm_config_server:reconfigure("/etc/slurm.conf"),
             ?assertEqual(ok, Result),

             %% Verify config was loaded
             ?assertEqual(6817, flurm_config_server:get(slurmctldport))
         end},
        {"reconfigure with file error",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {error, {file_error, "/bad", enoent}}
             end),

             Result = flurm_config_server:reconfigure("/bad/path"),
             ?assertMatch({error, {load_failed, _}}, Result)
         end},
        {"reconfigure with validation failure",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{slurmctldport => -1}}  %% Invalid port
             end),

             Result = flurm_config_server:reconfigure("/etc/slurm.conf"),
             ?assertMatch({error, {validation_failed, _}}, Result)
         end},
        {"reload is alias for reconfigure",
         fun() ->
             meck:expect(flurm_config_slurm, parse_file, fun(_) ->
                 {ok, #{test_key => test_value}}
             end),

             Result = flurm_config_server:reload("/etc/slurm.conf"),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         meck:new(filelib, [passthrough, unstick]),
         meck:new(flurm_config_defaults, [passthrough]),
         meck:new(error_logger, [passthrough, unstick]),

         meck:expect(filelib, is_file, fun(_) -> false end),
         meck:expect(flurm_config_defaults, apply_defaults, fun(C) -> C end),
         meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         meck_cleanup(ok)
     end,
     [
        {"DOWN removes subscriber",
         fun() ->
             %% Create a subscriber that we can kill
             Subscriber = spawn(fun() ->
                 receive stop -> ok end
             end),

             %% Make it subscribe through gen_server call simulation
             gen_server:call(flurm_config_server, {subscribe, Subscriber}),

             %% Kill the subscriber
             exit(Subscriber, kill),
             timer:sleep(50),

             %% Set a value - should not crash despite dead subscriber
             ok = flurm_config_server:set(key, value)
         end},
        {"unknown info message is ignored",
         fun() ->
             %% Send unknown message directly
             flurm_config_server ! unknown_message,
             %% Should not crash
             timer:sleep(50),
             ?assert(is_process_alive(whereis(flurm_config_server)))
         end}
     ]}.

%%====================================================================
%% Code Change Tests
%%====================================================================

code_change_test_() ->
    {"code_change returns state unchanged",
     fun() ->
         State = {state, #{}, "/etc/conf", [], #{}, 1234, 5},
         {ok, NewState} = flurm_config_server:code_change("1.0", State, []),
         ?assertEqual(State, NewState)
     end}.

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {"terminate returns ok",
     fun() ->
         State = {state, #{}, undefined, [], #{}, undefined, 1},
         Result = flurm_config_server:terminate(normal, State),
         ?assertEqual(ok, Result)
     end}.
