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
