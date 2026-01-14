%%%-------------------------------------------------------------------
%%% @doc Tests for FLURM Configuration Server
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_server_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: These tests work without lager (fallback to error_logger)

config_server_test_() ->
    {setup,
     fun setup_app/0,
     fun cleanup_app/1,
     {foreach,
      fun setup/0,
      fun cleanup/1,
      [
          {"Get undefined key returns default", fun test_get_default/0},
          {"Set and get value", fun test_set_get/0},
          {"Get all config", fun test_get_all/0},
          {"Subscribe receives updates", fun test_subscribe/0},
          {"Unsubscribe stops updates", fun test_unsubscribe/0},
          {"Load from config file", fun test_load_config/0},
          {"Reconfigure from file", fun test_reconfigure/0}
      ]}}.

setup_app() ->
    %% Create test config file
    TestDir = "/tmp/flurm_config_test",
    file:make_dir(TestDir),
    TestDir.

cleanup_app(TestDir) ->
    file:del_dir_r(TestDir).

setup() ->
    %% Start server without config file
    {ok, Pid} = flurm_config_server:start_link([]),
    Pid.

cleanup(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    timer:sleep(10).

%%====================================================================
%% Basic Get/Set Tests
%%====================================================================

test_get_default() ->
    ?assertEqual(undefined, flurm_config_server:get(nonexistent_key)),
    ?assertEqual(default_value, flurm_config_server:get(nonexistent_key, default_value)).

test_set_get() ->
    ok = flurm_config_server:set(test_key, test_value),
    ?assertEqual(test_value, flurm_config_server:get(test_key)).

test_get_all() ->
    ok = flurm_config_server:set(key1, value1),
    ok = flurm_config_server:set(key2, value2),
    Config = flurm_config_server:get_all(),
    ?assertEqual(value1, maps:get(key1, Config)),
    ?assertEqual(value2, maps:get(key2, Config)).

%%====================================================================
%% Subscription Tests
%%====================================================================

test_subscribe() ->
    ok = flurm_config_server:subscribe(),
    ok = flurm_config_server:set(notify_key, notify_value),
    receive
        {config_changed, _Version, Changes} ->
            ?assert(lists:member({notify_key, notify_value}, Changes))
    after 1000 ->
        ?assert(false, "Did not receive config change notification")
    end.

test_unsubscribe() ->
    ok = flurm_config_server:subscribe(),
    ok = flurm_config_server:unsubscribe(),
    ok = flurm_config_server:set(no_notify_key, no_notify_value),
    receive
        {config_changed, _, _} ->
            ?assert(false, "Should not receive notification after unsubscribe")
    after 100 ->
        ok
    end.

%%====================================================================
%% File Loading Tests
%%====================================================================

test_load_config() ->
    %% Create a test config file in Erlang format
    TestFile = "/tmp/flurm_config_test/test.config",
    ConfigContent = "#{clustername => <<\"testcluster\">>, port => 6817}.\n",
    ok = file:write_file(TestFile, ConfigContent),

    %% Use reconfigure/1 to load the file (server already running from setup)
    ok = flurm_config_server:reconfigure(TestFile),
    ?assertEqual(<<"testcluster">>, flurm_config_server:get(clustername)),
    ?assertEqual(6817, flurm_config_server:get(port)).

test_reconfigure() ->
    %% Create initial config file
    TestFile = "/tmp/flurm_config_test/reconfig.config",
    ok = file:write_file(TestFile, "#{version => 1}.\n"),

    %% Load initial config
    ok = flurm_config_server:reconfigure(TestFile),
    ?assertEqual(1, flurm_config_server:get(version)),

    %% Update config file
    ok = file:write_file(TestFile, "#{version => 2, new_key => new_value}.\n"),

    %% Subscribe to get notification
    ok = flurm_config_server:subscribe(),

    %% Trigger reconfigure
    ok = flurm_config_server:reconfigure(),

    %% Verify new values
    ?assertEqual(2, flurm_config_server:get(version)),
    ?assertEqual(new_value, flurm_config_server:get(new_key)),

    %% Verify we got notification
    receive
        {config_changed, _, Changes} ->
            ?assert(lists:member({version, 2}, Changes))
    after 1000 ->
        ?assert(false, "Did not receive reconfigure notification")
    end.

%%====================================================================
%% Node and Partition Tests
%%====================================================================

nodes_and_partitions_test_() ->
    [
         {"Get nodes returns empty list by default", fun test_get_nodes_empty/0},
         {"Get partitions returns empty list by default", fun test_get_partitions_empty/0},
         {"Get specific node", fun test_get_specific_node/0},
         {"Get specific partition", fun test_get_specific_partition/0}
    ].

test_get_nodes_empty() ->
    {ok, Pid} = flurm_config_server:start_link([]),
    ?assertEqual([], flurm_config_server:get_nodes()),
    unlink(Pid),
    exit(Pid, shutdown).

test_get_partitions_empty() ->
    {ok, Pid} = flurm_config_server:start_link([]),
    ?assertEqual([], flurm_config_server:get_partitions()),
    unlink(Pid),
    exit(Pid, shutdown).

test_get_specific_node() ->
    {ok, Pid} = flurm_config_server:start_link([]),
    Nodes = [
        #{nodename => <<"node001">>, cpus => 64},
        #{nodename => <<"node002">>, cpus => 32}
    ],
    ok = flurm_config_server:set(nodes, Nodes),
    Node = flurm_config_server:get_node(<<"node001">>),
    ?assertEqual(64, maps:get(cpus, Node)),
    ?assertEqual(undefined, flurm_config_server:get_node(<<"nonexistent">>)),
    unlink(Pid),
    exit(Pid, shutdown).

test_get_specific_partition() ->
    {ok, Pid} = flurm_config_server:start_link([]),
    Partitions = [
        #{partitionname => <<"batch">>, default => true},
        #{partitionname => <<"gpu">>, default => false}
    ],
    ok = flurm_config_server:set(partitions, Partitions),
    Part = flurm_config_server:get_partition(<<"batch">>),
    ?assertEqual(true, maps:get(default, Part)),
    ?assertEqual(undefined, flurm_config_server:get_partition(<<"nonexistent">>)),
    unlink(Pid),
    exit(Pid, shutdown).

%%====================================================================
%% Validation Tests
%%====================================================================

validation_test_() ->
    [
         {"Invalid port is rejected", fun test_invalid_port/0},
         {"Invalid node definition is rejected", fun test_invalid_node_def/0}
    ].

test_invalid_port() ->
    %% Create config with invalid port
    TestFile = "/tmp/flurm_config_test/invalid_port.config",
    file:make_dir("/tmp/flurm_config_test"),
    ok = file:write_file(TestFile, "#{slurmctldport => 99999}.\n"),

    {ok, Pid} = flurm_config_server:start_link([{config_file, TestFile}]),

    %% Create new config with invalid port
    ok = file:write_file(TestFile, "#{slurmctldport => -1}.\n"),

    %% Reconfigure should fail validation
    Result = flurm_config_server:reconfigure(),
    ?assertMatch({error, {validation_failed, _}}, Result),

    unlink(Pid),
    exit(Pid, shutdown),
    file:delete(TestFile).

test_invalid_node_def() ->
    %% Create config with invalid node (missing nodename)
    TestFile = "/tmp/flurm_config_test/invalid_node.config",
    file:make_dir("/tmp/flurm_config_test"),
    ok = file:write_file(TestFile, "#{nodes => [#{cpus => 64}]}.\n"),

    {ok, Pid} = flurm_config_server:start_link([]),

    %% Reconfigure should fail validation
    Result = flurm_config_server:reconfigure(TestFile),
    ?assertMatch({error, {validation_failed, _}}, Result),

    unlink(Pid),
    exit(Pid, shutdown),
    file:delete(TestFile).

%%====================================================================
%% Extended API Tests
%%====================================================================

extended_api_test_() ->
    {foreach,
     fun() ->
         file:make_dir("/tmp/flurm_config_test"),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         unlink(Pid),
         exit(Pid, shutdown),
         timer:sleep(10)
     end,
     [
      {"reload/0 is alias for reconfigure/0", fun test_reload_alias/0},
      {"reload/1 is alias for reconfigure/1", fun test_reload_with_file_alias/0},
      {"get_version returns version number", fun test_get_version/0},
      {"get_last_reload returns timestamp", fun test_get_last_reload/0},
      {"subscribe_changes with all keys", fun test_subscribe_changes_all/0},
      {"subscribe_changes with specific keys", fun test_subscribe_changes_filtered/0},
      {"unsubscribe_changes stops filtered updates", fun test_unsubscribe_changes/0},
      {"reconfigure with no config file", fun test_reconfigure_no_config_file/0}
     ]}.

test_reload_alias() ->
    %% Without a config file set, reload should return error (same as reconfigure)
    Result = flurm_config_server:reload(),
    ?assertEqual({error, no_config_file}, Result).

test_reload_with_file_alias() ->
    TestFile = "/tmp/flurm_config_test/reload_test.config",
    ok = file:write_file(TestFile, "#{test_reload_key => reload_value}.\n"),
    Result = flurm_config_server:reload(TestFile),
    ?assertEqual(ok, Result),
    ?assertEqual(reload_value, flurm_config_server:get(test_reload_key)),
    file:delete(TestFile).

test_get_version() ->
    Version1 = flurm_config_server:get_version(),
    ?assert(is_integer(Version1)),
    ?assert(Version1 >= 1),

    %% Set a value should increment version
    ok = flurm_config_server:set(version_test_key, value),
    Version2 = flurm_config_server:get_version(),
    ?assertEqual(Version1 + 1, Version2).

test_get_last_reload() ->
    LastReload = flurm_config_server:get_last_reload(),
    ?assert(is_integer(LastReload) orelse LastReload =:= undefined),
    %% Should be a reasonable timestamp (within last minute)
    case LastReload of
        undefined -> ok;
        TS ->
            Now = erlang:system_time(second),
            ?assert(abs(Now - TS) < 60)
    end.

test_subscribe_changes_all() ->
    ok = flurm_config_server:subscribe_changes(all),
    ok = flurm_config_server:set(filtered_key, filtered_value),
    receive
        {config_changed, filtered_key, undefined, filtered_value} ->
            ok
    after 1000 ->
        ?assert(false, "Did not receive filtered change notification")
    end.

test_subscribe_changes_filtered() ->
    ok = flurm_config_server:subscribe_changes([watched_key]),

    %% Set watched key - should receive notification
    ok = flurm_config_server:set(watched_key, watched_value),
    receive
        {config_changed, watched_key, undefined, watched_value} ->
            ok
    after 1000 ->
        ?assert(false, "Did not receive notification for watched key")
    end,

    %% Set unwatched key - should NOT receive notification
    ok = flurm_config_server:set(unwatched_key, unwatched_value),
    receive
        {config_changed, unwatched_key, _, _} ->
            ?assert(false, "Should not receive notification for unwatched key")
    after 100 ->
        ok
    end.

test_unsubscribe_changes() ->
    ok = flurm_config_server:subscribe_changes(all),
    ok = flurm_config_server:unsubscribe_changes(),
    ok = flurm_config_server:set(unsub_test_key, unsub_test_value),
    receive
        {config_changed, unsub_test_key, _, _} ->
            ?assert(false, "Should not receive notification after unsubscribe_changes")
    after 100 ->
        ok
    end.

test_reconfigure_no_config_file() ->
    Result = flurm_config_server:reconfigure(),
    ?assertEqual({error, no_config_file}, Result).

%%====================================================================
%% Gen_server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {foreach,
     fun() ->
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         unlink(Pid),
         exit(Pid, shutdown),
         timer:sleep(10)
     end,
     [
      {"handle_call with unknown request", fun test_unknown_call/0},
      {"handle_cast with unknown message", fun test_unknown_cast/0},
      {"handle_info with unknown message", fun test_unknown_info/0},
      {"handle_info with DOWN message", fun test_down_message/0},
      {"code_change returns ok", fun test_code_change/0}
     ]}.

test_unknown_call() ->
    %% Send unknown call - should return error
    Result = gen_server:call(flurm_config_server, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    %% Send unknown cast - should be ignored (noreply)
    ok = gen_server:cast(flurm_config_server, {unknown_cast_message, bar}),
    %% Server should still be alive
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_config_server))).

test_unknown_info() ->
    %% Send unknown info message - should be ignored (noreply)
    whereis(flurm_config_server) ! {unknown_info_message, baz},
    timer:sleep(50),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_config_server))).

test_down_message() ->
    %% Subscribe a process
    ok = flurm_config_server:subscribe(),

    %% Spawn a subscriber process that will die
    Subscriber = spawn(fun() ->
        ok = flurm_config_server:subscribe_changes(all),
        receive
            die -> ok
        end
    end),
    timer:sleep(50),

    %% Kill the subscriber
    Subscriber ! die,
    timer:sleep(100),

    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_config_server))),

    %% Setting value should work (subscriber was removed)
    ok = flurm_config_server:set(after_down_key, after_down_value),
    ?assertEqual(after_down_value, flurm_config_server:get(after_down_key)).

test_code_change() ->
    %% code_change/3 should return {ok, State}
    %% We test this by calling the function directly
    State = #{config => #{}, subscribers => [], filtered_subscribers => #{}, version => 1},
    Result = flurm_config_server:code_change("1.0.0", State, []),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% File Loading Edge Cases
%%====================================================================

file_loading_test_() ->
    {foreach,
     fun() ->
         file:make_dir("/tmp/flurm_config_test"),
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         unlink(Pid),
         exit(Pid, shutdown),
         file:del_dir_r("/tmp/flurm_config_test"),
         timer:sleep(10)
     end,
     [
      {"Load .conf file (slurm format)", fun test_load_conf_file/0},
      {"Load file without extension", fun test_load_file_no_extension/0},
      {"Load non-existent file", fun test_load_nonexistent_file/0},
      {"Load malformed config file", fun test_load_malformed_file/0}
     ]}.

test_load_conf_file() ->
    TestFile = "/tmp/flurm_config_test/test.conf",
    ok = file:write_file(TestFile, "ClusterName=testcluster\nSlurmctldPort=6817\n"),
    Result = flurm_config_server:reconfigure(TestFile),
    ?assertEqual(ok, Result),
    ?assertEqual(<<"testcluster">>, flurm_config_server:get(clustername)),
    file:delete(TestFile).

test_load_file_no_extension() ->
    TestFile = "/tmp/flurm_config_test/testconfig",
    %% Try slurm.conf format first
    ok = file:write_file(TestFile, "ClusterName=noextcluster\n"),
    Result = flurm_config_server:reconfigure(TestFile),
    ?assertEqual(ok, Result),
    ?assertEqual(<<"noextcluster">>, flurm_config_server:get(clustername)),
    file:delete(TestFile).

test_load_nonexistent_file() ->
    Result = flurm_config_server:reconfigure("/tmp/flurm_config_test/nonexistent.config"),
    ?assertMatch({error, {load_failed, _}}, Result).

test_load_malformed_file() ->
    TestFile = "/tmp/flurm_config_test/malformed.config",
    ok = file:write_file(TestFile, "this is not valid erlang {"),
    Result = flurm_config_server:reconfigure(TestFile),
    ?assertMatch({error, {load_failed, _}}, Result),
    file:delete(TestFile).

%%====================================================================
%% Node/Partition Lookup Tests
%%====================================================================

node_partition_lookup_test_() ->
    {foreach,
     fun() ->
         {ok, Pid} = flurm_config_server:start_link([]),
         Pid
     end,
     fun(Pid) ->
         unlink(Pid),
         exit(Pid, shutdown),
         timer:sleep(10)
     end,
     [
      {"Find node with hostlist pattern", fun test_find_node_hostlist/0},
      {"Find partition - match", fun test_find_partition_match/0},
      {"Find partition - no match", fun test_find_partition_no_match/0},
      {"Find node - node without nodename", fun test_find_node_without_nodename/0}
     ]}.

test_find_node_hostlist() ->
    %% Nodes with hostlist pattern
    Nodes = [
        #{nodename => <<"node[001-003]">>, cpus => 64}
    ],
    ok = flurm_config_server:set(nodes, Nodes),

    %% Should find node002 even though it's part of a hostlist
    Node = flurm_config_server:get_node(<<"node002">>),
    ?assertMatch(#{cpus := 64}, Node),

    %% Should not find node outside the range
    ?assertEqual(undefined, flurm_config_server:get_node(<<"node005">>)).

test_find_partition_match() ->
    Partitions = [
        #{partitionname => <<"batch">>, maxtime => 86400},
        #{partitionname => <<"debug">>, maxtime => 3600}
    ],
    ok = flurm_config_server:set(partitions, Partitions),

    Part = flurm_config_server:get_partition(<<"debug">>),
    ?assertEqual(3600, maps:get(maxtime, Part)).

test_find_partition_no_match() ->
    Partitions = [
        #{partitionname => <<"batch">>, maxtime => 86400}
    ],
    ok = flurm_config_server:set(partitions, Partitions),
    ?assertEqual(undefined, flurm_config_server:get_partition(<<"nonexistent">>)).

test_find_node_without_nodename() ->
    %% Node without nodename should not match
    Nodes = [
        #{cpus => 64, memory => 128000}  %% Missing nodename
    ],
    ok = flurm_config_server:set(nodes, Nodes),
    ?assertEqual(undefined, flurm_config_server:get_node(<<"anynode">>)).
