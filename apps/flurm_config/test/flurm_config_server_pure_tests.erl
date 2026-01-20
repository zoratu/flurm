%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_config_server module
%%%
%%% These tests exercise the flurm_config_server gen_server without meck.
%%% Tests focus on:
%%% - Direct callback function testing (init, handle_call, handle_cast, etc.)
%%% - State manipulation and transitions
%%% - Subscriber management
%%% - Configuration change detection
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_server_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% State Record (mirroring the server's internal state)
%%====================================================================

-record(state, {
    config :: map(),
    config_file :: string() | undefined,
    subscribers :: [pid()],
    filtered_subscribers :: #{pid() => [atom()] | all},
    last_reload :: integer() | undefined,
    version :: pos_integer()
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a minimal test state
make_test_state() ->
    make_test_state(#{}).

make_test_state(Config) ->
    #state{
        config = Config,
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = erlang:system_time(second),
        version = 1
    }.

%%====================================================================
%% Init Callback Tests
%%====================================================================

init_test_() ->
    [
     {"init with empty options", fun init_empty_options/0},
     {"init with config_file option", fun init_with_config_file/0}
    ].

init_empty_options() ->
    %% Init with no options should start with empty config
    {ok, State} = flurm_config_server:init([]),
    ?assert(is_map(State#state.config)),
    ?assertEqual([], State#state.subscribers),
    ?assertEqual(#{}, State#state.filtered_subscribers),
    ?assertEqual(1, State#state.version),
    ?assert(is_integer(State#state.last_reload)).

init_with_config_file() ->
    %% Create a temp config file
    TempFile = "/tmp/flurm_server_test_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{init_key => init_value}.\n",
    ok = file:write_file(TempFile, Content),

    %% Init with config_file option
    {ok, State} = flurm_config_server:init([{config_file, TempFile}]),
    ?assertEqual(TempFile, State#state.config_file),

    %% Cleanup
    file:delete(TempFile).

%%====================================================================
%% Handle Call Tests - Get/Set Operations
%%====================================================================

handle_call_get_set_test_() ->
    [
     {"handle_call get with existing key", fun handle_call_get_existing/0},
     {"handle_call get with default", fun handle_call_get_default/0},
     {"handle_call set new key", fun handle_call_set_new/0},
     {"handle_call set existing key", fun handle_call_set_existing/0},
     {"handle_call get_all", fun handle_call_get_all/0}
    ].

handle_call_get_existing() ->
    State = make_test_state(#{my_key => my_value}),
    {reply, Reply, NewState} = flurm_config_server:handle_call({get, my_key, undefined}, {self(), ref}, State),
    ?assertEqual(my_value, Reply),
    ?assertEqual(State, NewState).

handle_call_get_default() ->
    State = make_test_state(#{}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get, missing_key, default_val}, {self(), ref}, State),
    ?assertEqual(default_val, Reply).

handle_call_set_new() ->
    State = make_test_state(#{}),
    {reply, ok, NewState} = flurm_config_server:handle_call({set, new_key, new_value}, {self(), ref}, State),
    ?assertEqual(new_value, maps:get(new_key, NewState#state.config)),
    ?assertEqual(2, NewState#state.version).

handle_call_set_existing() ->
    State = make_test_state(#{existing_key => old_value}),
    {reply, ok, NewState} = flurm_config_server:handle_call({set, existing_key, new_value}, {self(), ref}, State),
    ?assertEqual(new_value, maps:get(existing_key, NewState#state.config)),
    ?assertEqual(2, NewState#state.version).

handle_call_get_all() ->
    Config = #{key1 => val1, key2 => val2},
    State = make_test_state(Config),
    {reply, Reply, _NewState} = flurm_config_server:handle_call(get_all, {self(), ref}, State),
    ?assertEqual(Config, Reply).

%%====================================================================
%% Handle Call Tests - Subscribe/Unsubscribe
%%====================================================================

handle_call_subscribe_test_() ->
    [
     {"subscribe adds pid to list", fun subscribe_adds_pid/0},
     {"subscribe is idempotent", fun subscribe_idempotent/0},
     {"unsubscribe removes pid", fun unsubscribe_removes_pid/0},
     {"unsubscribe non-existent pid is safe", fun unsubscribe_nonexistent/0},
     {"subscribe_changes adds to filtered_subscribers", fun subscribe_changes_adds/0},
     {"subscribe_changes with key list", fun subscribe_changes_with_keys/0},
     {"unsubscribe_changes removes from filtered_subscribers", fun unsubscribe_changes_removes/0}
    ].

subscribe_adds_pid() ->
    State = make_test_state(),
    TestPid = spawn(fun() -> receive _ -> ok end end),
    {reply, ok, NewState} = flurm_config_server:handle_call({subscribe, TestPid}, {self(), ref}, State),
    ?assert(lists:member(TestPid, NewState#state.subscribers)),
    exit(TestPid, kill).

subscribe_idempotent() ->
    TestPid = spawn(fun() -> receive _ -> ok end end),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [TestPid],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call({subscribe, TestPid}, {self(), ref}, State),
    %% Should still have only one entry
    ?assertEqual(1, length(NewState#state.subscribers)),
    exit(TestPid, kill).

unsubscribe_removes_pid() ->
    TestPid = spawn(fun() -> receive _ -> ok end end),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [TestPid],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call({unsubscribe, TestPid}, {self(), ref}, State),
    ?assertEqual([], NewState#state.subscribers),
    exit(TestPid, kill).

unsubscribe_nonexistent() ->
    State = make_test_state(),
    NonExistentPid = list_to_pid("<0.99999.0>"),
    {reply, ok, NewState} = flurm_config_server:handle_call({unsubscribe, NonExistentPid}, {self(), ref}, State),
    ?assertEqual([], NewState#state.subscribers).

subscribe_changes_adds() ->
    State = make_test_state(),
    TestPid = spawn(fun() -> receive _ -> ok end end),
    {reply, ok, NewState} = flurm_config_server:handle_call({subscribe_changes, TestPid, all}, {self(), ref}, State),
    ?assertEqual(all, maps:get(TestPid, NewState#state.filtered_subscribers)),
    exit(TestPid, kill).

subscribe_changes_with_keys() ->
    State = make_test_state(),
    TestPid = spawn(fun() -> receive _ -> ok end end),
    Keys = [nodes, partitions],
    {reply, ok, NewState} = flurm_config_server:handle_call({subscribe_changes, TestPid, Keys}, {self(), ref}, State),
    ?assertEqual(Keys, maps:get(TestPid, NewState#state.filtered_subscribers)),
    exit(TestPid, kill).

unsubscribe_changes_removes() ->
    TestPid = spawn(fun() -> receive _ -> ok end end),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{TestPid => all},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call({unsubscribe_changes, TestPid}, {self(), ref}, State),
    ?assertEqual(#{}, NewState#state.filtered_subscribers),
    exit(TestPid, kill).

%%====================================================================
%% Handle Call Tests - Version and Reload Info
%%====================================================================

handle_call_info_test_() ->
    [
     {"get_version returns current version", fun get_version/0},
     {"get_last_reload returns timestamp", fun get_last_reload/0}
    ].

get_version() ->
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 42
    },
    {reply, 42, _NewState} = flurm_config_server:handle_call(get_version, {self(), ref}, State).

get_last_reload() ->
    Timestamp = erlang:system_time(second),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = Timestamp,
        version = 1
    },
    {reply, Reply, _NewState} = flurm_config_server:handle_call(get_last_reload, {self(), ref}, State),
    ?assertEqual(Timestamp, Reply).

%%====================================================================
%% Handle Call Tests - Nodes and Partitions
%%====================================================================

handle_call_nodes_partitions_test_() ->
    [
     {"get_nodes returns node list", fun get_nodes/0},
     {"get_nodes returns empty when no nodes", fun get_nodes_empty/0},
     {"get_partitions returns partition list", fun get_partitions/0},
     {"get_node finds specific node", fun get_node_found/0},
     {"get_node returns undefined when not found", fun get_node_not_found/0},
     {"get_partition finds specific partition", fun get_partition_found/0},
     {"get_partition returns undefined when not found", fun get_partition_not_found/0}
    ].

get_nodes() ->
    Nodes = [#{nodename => <<"node1">>}, #{nodename => <<"node2">>}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call(get_nodes, {self(), ref}, State),
    ?assertEqual(Nodes, Reply).

get_nodes_empty() ->
    State = make_test_state(#{}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call(get_nodes, {self(), ref}, State),
    ?assertEqual([], Reply).

get_partitions() ->
    Partitions = [#{partitionname => <<"batch">>}, #{partitionname => <<"debug">>}],
    State = make_test_state(#{partitions => Partitions}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call(get_partitions, {self(), ref}, State),
    ?assertEqual(Partitions, Reply).

get_node_found() ->
    Nodes = [#{nodename => <<"node1">>, cpus => 8}, #{nodename => <<"node2">>, cpus => 16}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"node1">>}, {self(), ref}, State),
    ?assertEqual(#{nodename => <<"node1">>, cpus => 8}, Reply).

get_node_not_found() ->
    Nodes = [#{nodename => <<"node1">>}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"nonexistent">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

get_partition_found() ->
    Partitions = [#{partitionname => <<"batch">>, default => true}, #{partitionname => <<"debug">>}],
    State = make_test_state(#{partitions => Partitions}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_partition, <<"batch">>}, {self(), ref}, State),
    ?assertEqual(#{partitionname => <<"batch">>, default => true}, Reply).

get_partition_not_found() ->
    Partitions = [#{partitionname => <<"batch">>}],
    State = make_test_state(#{partitions => Partitions}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_partition, <<"nonexistent">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

%%====================================================================
%% Handle Call Tests - Reconfigure
%%====================================================================

handle_call_reconfigure_test_() ->
    [
     {"reconfigure fails without config_file", fun reconfigure_no_file/0},
     {"reconfigure with file that doesn't exist", fun reconfigure_missing_file/0},
     {"reconfigure with valid file", fun reconfigure_valid_file/0},
     {"reconfigure with explicit file path", fun reconfigure_explicit_file/0}
    ].

reconfigure_no_file() ->
    State = make_test_state(),
    {reply, {error, no_config_file}, _NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State).

reconfigure_missing_file() ->
    State = #state{
        config = #{},
        config_file = "/tmp/nonexistent_file_12345.conf",
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, Reply, _NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    ?assertMatch({error, {load_failed, _}}, Reply).

reconfigure_valid_file() ->
    %% Create a temp config file
    TempFile = "/tmp/flurm_reconfig_test_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{reconfig_key => reconfig_value}.\n",
    ok = file:write_file(TempFile, Content),

    State = #state{
        config = #{},
        config_file = TempFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, Reply, NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    ?assertEqual(ok, Reply),
    ?assertEqual(reconfig_value, maps:get(reconfig_key, NewState#state.config)),
    ?assertEqual(2, NewState#state.version),

    %% Cleanup
    file:delete(TempFile).

reconfigure_explicit_file() ->
    %% Create a temp config file
    TempFile = "/tmp/flurm_explicit_test_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{explicit_key => explicit_value}.\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertEqual(ok, Reply),
    ?assertEqual(explicit_value, maps:get(explicit_key, NewState#state.config)),
    ?assertEqual(TempFile, NewState#state.config_file),

    %% Cleanup
    file:delete(TempFile).

%%====================================================================
%% Handle Call Tests - Unknown Request
%%====================================================================

handle_call_unknown_test() ->
    State = make_test_state(),
    {reply, Reply, _NewState} = flurm_config_server:handle_call(unknown_request, {self(), ref}, State),
    ?assertEqual({error, unknown_request}, Reply).

%%====================================================================
%% Handle Cast Tests
%%====================================================================

handle_cast_test() ->
    State = make_test_state(),
    {noreply, NewState} = flurm_config_server:handle_cast(any_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_test_() ->
    [
     {"handle DOWN removes subscriber", fun handle_down_removes_subscriber/0},
     {"handle DOWN removes filtered subscriber", fun handle_down_removes_filtered/0},
     {"handle unknown info", fun handle_unknown_info/0}
    ].

handle_down_removes_subscriber() ->
    TestPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(TestPid), %% Wait for the process to die
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [TestPid],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {noreply, NewState} = flurm_config_server:handle_info({'DOWN', make_ref(), process, TestPid, normal}, State),
    ?assertEqual([], NewState#state.subscribers).

handle_down_removes_filtered() ->
    TestPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(TestPid),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{TestPid => all},
        last_reload = undefined,
        version = 1
    },
    {noreply, NewState} = flurm_config_server:handle_info({'DOWN', make_ref(), process, TestPid, normal}, State),
    ?assertEqual(#{}, NewState#state.filtered_subscribers).

handle_unknown_info() ->
    State = make_test_state(),
    {noreply, NewState} = flurm_config_server:handle_info(unknown_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% Terminate and Code Change Tests
%%====================================================================

terminate_test() ->
    State = make_test_state(),
    ?assertEqual(ok, flurm_config_server:terminate(normal, State)),
    ?assertEqual(ok, flurm_config_server:terminate(shutdown, State)),
    ?assertEqual(ok, flurm_config_server:terminate({error, reason}, State)).

code_change_test() ->
    State = make_test_state(),
    {ok, NewState} = flurm_config_server:code_change("1.0.0", State, []),
    ?assertEqual(State, NewState).

%%====================================================================
%% Validation Tests (via reconfigure with invalid config)
%%====================================================================

validation_test_() ->
    [
     {"valid port numbers pass validation", fun valid_ports/0},
     {"invalid port numbers fail validation", fun invalid_ports/0},
     {"valid node definitions pass validation", fun valid_nodes/0},
     {"invalid node definitions fail validation", fun invalid_nodes/0}
    ].

valid_ports() ->
    %% Create config with valid ports
    TempFile = "/tmp/flurm_valid_ports_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{slurmctldport => 6817, slurmdport => 6818}.\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertEqual(ok, Reply),

    file:delete(TempFile).

invalid_ports() ->
    %% Create config with invalid port
    TempFile = "/tmp/flurm_invalid_ports_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{slurmctldport => 99999}.\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertMatch({error, {validation_failed, {invalid_ports, _}}}, Reply),

    file:delete(TempFile).

valid_nodes() ->
    %% Create config with valid node definitions
    TempFile = "/tmp/flurm_valid_nodes_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{nodes => [#{nodename => <<\"node1\">>}, #{nodename => <<\"node2\">>}]}.\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertEqual(ok, Reply),

    file:delete(TempFile).

invalid_nodes() ->
    %% Create config with invalid node (missing nodename)
    TempFile = "/tmp/flurm_invalid_nodes_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{nodes => [#{cpus => 8}]}.\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertMatch({error, {validation_failed, {invalid_nodes, _}}}, Reply),

    file:delete(TempFile).

%%====================================================================
%% Hostlist Pattern Expansion Tests (via get_node)
%%====================================================================

hostlist_expansion_test_() ->
    [
     {"find node in hostlist range", fun find_node_in_range/0},
     {"find node in hostlist with multiple ranges", fun find_node_multiple_ranges/0},
     {"node not in hostlist range", fun node_not_in_range/0}
    ].

find_node_in_range() ->
    %% Node definitions with hostlist patterns
    Nodes = [#{nodename => <<"node[001-010]">>, cpus => 8}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"node005">>}, {self(), ref}, State),
    ?assertEqual(#{nodename => <<"node[001-010]">>, cpus => 8}, Reply).

find_node_multiple_ranges() ->
    %% Node definitions with multiple ranges
    Nodes = [#{nodename => <<"compute[01-05,10-15]">>, cpus => 16}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"compute12">>}, {self(), ref}, State),
    ?assertEqual(#{nodename => <<"compute[01-05,10-15]">>, cpus => 16}, Reply).

node_not_in_range() ->
    Nodes = [#{nodename => <<"node[001-010]">>, cpus => 8}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"node020">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

%%====================================================================
%% Configuration Change Detection Tests
%%====================================================================

change_detection_test_() ->
    [
     {"detect new key added", fun detect_new_key/0},
     {"detect value changed", fun detect_value_changed/0},
     {"detect key removed", fun detect_key_removed/0},
     {"no changes detected for identical config", fun no_changes_identical/0}
    ].

detect_new_key() ->
    %% Create initial config and then config with new key
    TempFile = "/tmp/flurm_change_new_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{existing_key => value1, new_key => new_value}.\n",
    ok = file:write_file(TempFile, Content),

    State = #state{
        config = #{existing_key => value1},
        config_file = TempFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    ?assertEqual(new_value, maps:get(new_key, NewState#state.config)),

    file:delete(TempFile).

detect_value_changed() ->
    TempFile = "/tmp/flurm_change_val_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{my_key => new_value}.\n",
    ok = file:write_file(TempFile, Content),

    State = #state{
        config = #{my_key => old_value},
        config_file = TempFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    ?assertEqual(new_value, maps:get(my_key, NewState#state.config)),

    file:delete(TempFile).

detect_key_removed() ->
    %% Key was in old config, not in new config (will get defaults applied though)
    TempFile = "/tmp/flurm_change_rem_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{}.\n",
    ok = file:write_file(TempFile, Content),

    State = #state{
        config = #{custom_key => custom_value},
        config_file = TempFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    %% custom_key should not be in new config
    ?assertEqual(undefined, maps:get(custom_key, NewState#state.config, undefined)),

    file:delete(TempFile).

no_changes_identical() ->
    %% Same config should still work (version increments)
    TempFile = "/tmp/flurm_no_change_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{my_key => my_value}.\n",
    ok = file:write_file(TempFile, Content),

    State = #state{
        config = #{my_key => my_value},
        config_file = TempFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },
    {reply, ok, NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    ?assertEqual(2, NewState#state.version),

    file:delete(TempFile).

%%====================================================================
%% SLURM Config File Format Tests
%%====================================================================

slurm_config_format_test_() ->
    [
     {"load slurm.conf format file", fun load_slurm_conf/0},
     {"load config with nodes and partitions", fun load_nodes_partitions/0}
    ].

load_slurm_conf() ->
    %% Create a SLURM-style config file
    TempFile = "/tmp/flurm_slurm_test_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".conf",
    Content = "ClusterName=testcluster\nSlurmctldPort=6817\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertEqual(ok, Reply),
    ?assertEqual(<<"testcluster">>, maps:get(clustername, NewState#state.config)),
    ?assertEqual(6817, maps:get(slurmctldport, NewState#state.config)),

    file:delete(TempFile).

load_nodes_partitions() ->
    TempFile = "/tmp/flurm_np_test_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".conf",
    Content = "ClusterName=test\nNodeName=node01 CPUs=8\nPartitionName=batch Nodes=node01 Default=YES\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(),
    {reply, Reply, NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    ?assertEqual(ok, Reply),

    Nodes = maps:get(nodes, NewState#state.config, []),
    Partitions = maps:get(partitions, NewState#state.config, []),
    ?assert(length(Nodes) > 0),
    ?assert(length(Partitions) > 0),

    file:delete(TempFile).

%%====================================================================
%% Subscriber Notification Tests
%%====================================================================

subscriber_notification_test_() ->
    [
     {"set notifies legacy subscribers", fun set_notifies_legacy/0},
     {"set notifies filtered subscribers with matching key", fun set_notifies_filtered_match/0},
     {"set does not notify filtered subscribers without matching key", fun set_no_notify_filtered_nomatch/0}
    ].

set_notifies_legacy() ->
    %% Create a subscriber process
    Parent = self(),
    Subscriber = spawn(fun() ->
        receive
            Msg -> Parent ! {got_msg, Msg}
        after 1000 ->
            Parent ! {got_msg, timeout}
        end
    end),

    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [Subscriber],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },

    {reply, ok, _NewState} = flurm_config_server:handle_call({set, test_key, test_value}, {self(), ref}, State),

    %% Wait for notification
    receive
        {got_msg, {config_changed, 2, [{test_key, test_value}]}} -> ok;
        {got_msg, Other} -> ?assertEqual(expected, Other)
    after 500 ->
        ?assert(false)
    end.

set_notifies_filtered_match() ->
    Parent = self(),
    Subscriber = spawn(fun() ->
        receive
            Msg -> Parent ! {got_filtered_msg, Msg}
        after 1000 ->
            Parent ! {got_filtered_msg, timeout}
        end
    end),

    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{Subscriber => [nodes, test_key]},
        last_reload = undefined,
        version = 1
    },

    {reply, ok, _NewState} = flurm_config_server:handle_call({set, test_key, test_value}, {self(), ref}, State),

    receive
        {got_filtered_msg, {config_changed, test_key, undefined, test_value}} -> ok;
        {got_filtered_msg, Other} -> ?assertEqual(expected, Other)
    after 500 ->
        ?assert(false)
    end.

set_no_notify_filtered_nomatch() ->
    Parent = self(),
    Subscriber = spawn(fun() ->
        receive
            Msg -> Parent ! {got_nomatch_msg, Msg}
        after 200 ->
            Parent ! {got_nomatch_msg, no_message}
        end
    end),

    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{Subscriber => [nodes, partitions]},  % Does not include test_key
        last_reload = undefined,
        version = 1
    },

    {reply, ok, _NewState} = flurm_config_server:handle_call({set, test_key, test_value}, {self(), ref}, State),

    receive
        {got_nomatch_msg, no_message} -> ok;  % Expected - no notification sent
        {got_nomatch_msg, _Other} -> ?assert(false)  % Should not receive anything
    after 500 ->
        ?assert(false)
    end.

%%====================================================================
%% Node Definition Edge Cases
%%====================================================================

node_edge_cases_test_() ->
    [
     {"node without pattern (exact match)", fun node_exact_match/0},
     {"multiple node definitions", fun multiple_node_defs/0},
     {"node with non-binary nodename", fun node_non_binary_nodename/0}
    ].

node_exact_match() ->
    %% Node with exact name (no hostlist pattern)
    Nodes = [#{nodename => <<"exactnode">>, cpus => 4}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"exactnode">>}, {self(), ref}, State),
    ?assertEqual(#{nodename => <<"exactnode">>, cpus => 4}, Reply).

multiple_node_defs() ->
    %% Multiple node definitions, find from second one
    Nodes = [
        #{nodename => <<"node[001-010]">>, cpus => 8},
        #{nodename => <<"gpu[01-04]">>, cpus => 16, gres => <<"gpu:4">>}
    ],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"gpu02">>}, {self(), ref}, State),
    ?assertEqual(#{nodename => <<"gpu[01-04]">>, cpus => 16, gres => <<"gpu:4">>}, Reply).

node_non_binary_nodename() ->
    %% Node with non-binary nodename should not match
    Nodes = [#{nodename => "string_node", cpus => 4}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"string_node">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

%%====================================================================
%% Partition Edge Cases
%%====================================================================

partition_edge_cases_test_() ->
    [
     {"find partition in multiple partitions", fun find_partition_multiple/0},
     {"partition with extra attributes", fun partition_with_attrs/0}
    ].

find_partition_multiple() ->
    Partitions = [
        #{partitionname => <<"batch">>, default => true},
        #{partitionname => <<"debug">>, maxtime => 3600},
        #{partitionname => <<"gpu">>, nodes => <<"gpu[01-04]">>}
    ],
    State = make_test_state(#{partitions => Partitions}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_partition, <<"debug">>}, {self(), ref}, State),
    ?assertEqual(#{partitionname => <<"debug">>, maxtime => 3600}, Reply).

partition_with_attrs() ->
    Partitions = [#{partitionname => <<"compute">>, maxtime => infinity, priority => 100}],
    State = make_test_state(#{partitions => Partitions}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_partition, <<"compute">>}, {self(), ref}, State),
    ?assertEqual(#{partitionname => <<"compute">>, maxtime => infinity, priority => 100}, Reply).

%%====================================================================
%% Additional Coverage Tests - Reconfigure with Nodes and Partitions
%%====================================================================

reconfigure_with_nodes_partitions_test_() ->
    [
     {"reconfigure triggers node notification", fun reconfigure_node_notification/0},
     {"reconfigure triggers partition notification", fun reconfigure_partition_notification/0},
     {"reconfigure with scheduler type change", fun reconfigure_scheduler_type/0},
     {"reconfigure with port change", fun reconfigure_port_change/0}
    ].

reconfigure_node_notification() ->
    TempFile = "/tmp/flurm_node_notif_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".conf",
    Content = "NodeName=node01 CPUs=8\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(#{}),
    {reply, ok, NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    Nodes = maps:get(nodes, NewState#state.config, []),
    ?assert(length(Nodes) > 0),

    file:delete(TempFile).

reconfigure_partition_notification() ->
    TempFile = "/tmp/flurm_part_notif_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".conf",
    Content = "PartitionName=batch Default=YES\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(#{}),
    {reply, ok, NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),
    Partitions = maps:get(partitions, NewState#state.config, []),
    ?assert(length(Partitions) > 0),

    file:delete(TempFile).

reconfigure_scheduler_type() ->
    TempFile = "/tmp/flurm_sched_type_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".conf",
    Content = "SchedulerType=sched/backfill\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(#{schedulertype => <<"sched/builtin">>}),
    {reply, ok, _NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),

    file:delete(TempFile).

reconfigure_port_change() ->
    TempFile = "/tmp/flurm_port_chg_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".conf",
    Content = "SlurmctldPort=6820\n",
    ok = file:write_file(TempFile, Content),

    State = make_test_state(#{slurmctldport => 6817}),
    {reply, ok, _NewState} = flurm_config_server:handle_call({reconfigure, TempFile}, {self(), ref}, State),

    file:delete(TempFile).

%%====================================================================
%% Additional Coverage - Filtered Subscribers with 'all' key
%%====================================================================

filtered_subscriber_all_test_() ->
    [
     {"subscribe_changes with all receives all changes", fun subscribe_all_receives_all/0},
     {"subscribe_changes updates existing subscription", fun subscribe_changes_updates/0}
    ].

subscribe_all_receives_all() ->
    Parent = self(),
    Subscriber = spawn(fun() ->
        receive
            Msg -> Parent ! {got_all_msg, Msg}
        after 1000 ->
            Parent ! {got_all_msg, timeout}
        end
    end),

    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{Subscriber => all},
        last_reload = undefined,
        version = 1
    },

    {reply, ok, _NewState} = flurm_config_server:handle_call({set, any_key, any_value}, {self(), ref}, State),

    receive
        {got_all_msg, {config_changed, any_key, undefined, any_value}} -> ok;
        {got_all_msg, Other} -> ?assertEqual(expected, Other)
    after 500 ->
        ?assert(false)
    end.

subscribe_changes_updates() ->
    TestPid = spawn(fun() -> receive _ -> ok end end),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [],
        filtered_subscribers = #{TestPid => [old_key]},
        last_reload = undefined,
        version = 1
    },
    %% Update subscription to new keys
    NewKeys = [new_key1, new_key2],
    {reply, ok, NewState} = flurm_config_server:handle_call({subscribe_changes, TestPid, NewKeys}, {self(), ref}, State),
    ?assertEqual(NewKeys, maps:get(TestPid, NewState#state.filtered_subscribers)),
    exit(TestPid, kill).

%%====================================================================
%% Init with Application Environment
%%====================================================================

init_from_app_env_test() ->
    %% Set config_file in application environment
    TempFile = "/tmp/flurm_app_env_init_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{app_env_key => app_env_value}.\n",
    ok = file:write_file(TempFile, Content),

    application:set_env(flurm_config, config_file, TempFile),
    {ok, State} = flurm_config_server:init([]),
    ?assertEqual(TempFile, State#state.config_file),
    application:unset_env(flurm_config, config_file),
    file:delete(TempFile).

%%====================================================================
%% Init with Invalid Config File
%%====================================================================

init_with_invalid_file_test() ->
    %% Init with non-existent config file should still succeed with empty config
    {ok, State} = flurm_config_server:init([{config_file, "/tmp/nonexistent_init_file.conf"}]),
    ?assertEqual("/tmp/nonexistent_init_file.conf", State#state.config_file),
    ?assert(is_map(State#state.config)).

%%====================================================================
%% Edge Cases for Node Search
%%====================================================================

node_search_edge_cases_test_() ->
    [
     {"empty nodes list", fun empty_nodes_list/0},
     {"node with missing nodename key", fun node_missing_nodename/0}
    ].

empty_nodes_list() ->
    State = make_test_state(#{nodes => []}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"anynode">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

node_missing_nodename() ->
    %% Node without nodename key
    Nodes = [#{cpus => 8, memory => 1024}],
    State = make_test_state(#{nodes => Nodes}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_node, <<"anynode">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

%%====================================================================
%% Edge Cases for Partition Search
%%====================================================================

partition_search_edge_cases_test_() ->
    [
     {"empty partitions list", fun empty_partitions_list/0},
     {"partition with missing partitionname key", fun partition_missing_name/0}
    ].

empty_partitions_list() ->
    State = make_test_state(#{partitions => []}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_partition, <<"anypart">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

partition_missing_name() ->
    %% Partition without partitionname key
    Partitions = [#{default => true, maxtime => infinity}],
    State = make_test_state(#{partitions => Partitions}),
    {reply, Reply, _NewState} = flurm_config_server:handle_call({get_partition, <<"anypart">>}, {self(), ref}, State),
    ?assertEqual(undefined, Reply).

%%====================================================================
%% Multiple Subscribers Tests
%%====================================================================

multiple_subscribers_test_() ->
    [
     {"multiple legacy subscribers receive notifications", fun multiple_legacy_subs/0},
     {"both subscriber types receive notifications", fun both_subscriber_types/0}
    ].

multiple_legacy_subs() ->
    Parent = self(),
    Sub1 = spawn(fun() -> receive Msg -> Parent ! {sub1, Msg} after 1000 -> Parent ! {sub1, timeout} end end),
    Sub2 = spawn(fun() -> receive Msg -> Parent ! {sub2, Msg} after 1000 -> Parent ! {sub2, timeout} end end),

    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [Sub1, Sub2],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },

    {reply, ok, _NewState} = flurm_config_server:handle_call({set, multi_key, multi_value}, {self(), ref}, State),

    %% Both subscribers should receive notification
    receive {sub1, {config_changed, 2, [{multi_key, multi_value}]}} -> ok after 500 -> ?assert(false) end,
    receive {sub2, {config_changed, 2, [{multi_key, multi_value}]}} -> ok after 500 -> ?assert(false) end.

both_subscriber_types() ->
    Parent = self(),
    LegacySub = spawn(fun() -> receive Msg -> Parent ! {legacy, Msg} after 1000 -> Parent ! {legacy, timeout} end end),
    FilteredSub = spawn(fun() -> receive Msg -> Parent ! {filtered, Msg} after 1000 -> Parent ! {filtered, timeout} end end),

    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [LegacySub],
        filtered_subscribers = #{FilteredSub => all},
        last_reload = undefined,
        version = 1
    },

    {reply, ok, _NewState} = flurm_config_server:handle_call({set, both_key, both_value}, {self(), ref}, State),

    %% Both subscriber types should receive notification
    receive {legacy, {config_changed, 2, [{both_key, both_value}]}} -> ok after 500 -> ?assert(false) end,
    receive {filtered, {config_changed, both_key, undefined, both_value}} -> ok after 500 -> ?assert(false) end.

%%====================================================================
%% Config Change with Multiple Changes
%%====================================================================

multiple_changes_test() ->
    TempFile = "/tmp/flurm_multi_changes_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "#{key1 => new_val1, key2 => new_val2, key3 => val3}.\n",
    ok = file:write_file(TempFile, Content),

    State = #state{
        config = #{key1 => old_val1, key2 => old_val2},
        config_file = TempFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = undefined,
        version = 1
    },

    {reply, ok, NewState} = flurm_config_server:handle_call(reconfigure, {self(), ref}, State),
    ?assertEqual(new_val1, maps:get(key1, NewState#state.config)),
    ?assertEqual(new_val2, maps:get(key2, NewState#state.config)),
    ?assertEqual(val3, maps:get(key3, NewState#state.config)),

    file:delete(TempFile).

%%====================================================================
%% DOWN Message Handling with Both Subscriber Types
%%====================================================================

down_with_both_subscribers_test() ->
    TestPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(TestPid),
    State = #state{
        config = #{},
        config_file = undefined,
        subscribers = [TestPid],
        filtered_subscribers = #{TestPid => all},
        last_reload = undefined,
        version = 1
    },
    {noreply, NewState} = flurm_config_server:handle_info({'DOWN', make_ref(), process, TestPid, normal}, State),
    ?assertEqual([], NewState#state.subscribers),
    ?assertEqual(#{}, NewState#state.filtered_subscribers).
