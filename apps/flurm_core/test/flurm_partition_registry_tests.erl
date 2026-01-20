%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_partition_registry module
%%%-------------------------------------------------------------------
-module(flurm_partition_registry_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the partition registry server
    case whereis(flurm_partition_registry) of
        undefined ->
            {ok, Pid} = flurm_partition_registry:start_link(),
            %% Unlink immediately to prevent test process from crashing
            %% when gen_server shuts down
            unlink(Pid),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, Pid}) ->
    catch ets:delete(flurm_partitions),
    case is_process_alive(Pid) of
        true ->
            %% Unlink first to prevent test process from being killed
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(flurm_partition_registry, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush])
            end;
        false ->
            ok
    end;
cleanup({existing, _Pid}) ->
    ok.

partition_registry_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"default partition exists", fun test_default_partition_exists/0},
      {"register partition", fun test_register_partition/0},
      {"unregister partition", fun test_unregister_partition/0},
      {"get partition priority", fun test_get_partition_priority/0},
      {"set partition priority", fun test_set_partition_priority/0},
      {"list partitions", fun test_list_partitions/0},
      {"add node to partition", fun test_add_node_to_partition/0},
      {"remove node from partition", fun test_remove_node_from_partition/0},
      {"set default partition", fun test_set_default_partition/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_default_partition_exists() ->
    %% Default partition should exist after init
    {ok, DefaultName} = flurm_partition_registry:get_default_partition(),
    ?assertEqual(<<"default">>, DefaultName),

    %% Should be able to get its info
    {ok, Info} = flurm_partition_registry:get_partition(<<"default">>),
    ?assertEqual(<<"default">>, maps:get(name, Info)).

test_register_partition() ->
    %% Register new partition
    Config = #{
        priority => 2000,
        nodes => [<<"node1">>, <<"node2">>],
        state => up,
        max_time => 7200
    },
    ok = flurm_partition_registry:register_partition(<<"gpu">>, Config),

    %% Should exist now
    {ok, Info} = flurm_partition_registry:get_partition(<<"gpu">>),
    ?assertEqual(<<"gpu">>, maps:get(name, Info)),
    ?assertEqual(2000, maps:get(priority, Info)),
    ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Info)),

    %% Registering same name should fail
    ?assertEqual({error, already_exists},
                 flurm_partition_registry:register_partition(<<"gpu">>, Config)).

test_unregister_partition() ->
    %% Register a partition
    ok = flurm_partition_registry:register_partition(<<"temp">>, #{}),

    %% Should exist
    {ok, _} = flurm_partition_registry:get_partition(<<"temp">>),

    %% Unregister
    ok = flurm_partition_registry:unregister_partition(<<"temp">>),

    %% Should not exist
    ?assertEqual({error, not_found},
                 flurm_partition_registry:get_partition(<<"temp">>)),

    %% Unregistering again should fail
    ?assertEqual({error, not_found},
                 flurm_partition_registry:unregister_partition(<<"temp">>)).

test_get_partition_priority() ->
    %% Default partition priority
    {ok, DefaultPriority} = flurm_partition_registry:get_partition_priority(<<"default">>),
    ?assertEqual(1000, DefaultPriority),

    %% Non-existent partition
    ?assertEqual({error, not_found},
                 flurm_partition_registry:get_partition_priority(<<"nonexistent">>)).

test_set_partition_priority() ->
    %% Set priority
    ok = flurm_partition_registry:set_partition_priority(<<"default">>, 5000),

    %% Verify
    {ok, NewPriority} = flurm_partition_registry:get_partition_priority(<<"default">>),
    ?assertEqual(5000, NewPriority),

    %% Non-existent partition
    ?assertEqual({error, not_found},
                 flurm_partition_registry:set_partition_priority(<<"nonexistent">>, 1000)).

test_list_partitions() ->
    %% Should have default initially
    List1 = flurm_partition_registry:list_partitions(),
    ?assert(lists:member(<<"default">>, List1)),

    %% Add more partitions
    ok = flurm_partition_registry:register_partition(<<"batch">>, #{}),
    ok = flurm_partition_registry:register_partition(<<"debug">>, #{}),

    %% All should be listed
    List2 = flurm_partition_registry:list_partitions(),
    ?assert(lists:member(<<"default">>, List2)),
    ?assert(lists:member(<<"batch">>, List2)),
    ?assert(lists:member(<<"debug">>, List2)).

test_add_node_to_partition() ->
    %% Get initial nodes
    {ok, InitialNodes} = flurm_partition_registry:get_partition_nodes(<<"default">>),
    ?assertEqual([], InitialNodes),

    %% Add node
    ok = flurm_partition_registry:add_node_to_partition(<<"default">>, <<"node001">>),

    %% Verify
    {ok, Nodes1} = flurm_partition_registry:get_partition_nodes(<<"default">>),
    ?assert(lists:member(<<"node001">>, Nodes1)),

    %% Adding same node again should fail
    ?assertEqual({error, already_member},
                 flurm_partition_registry:add_node_to_partition(<<"default">>, <<"node001">>)),

    %% Add to non-existent partition
    ?assertEqual({error, partition_not_found},
                 flurm_partition_registry:add_node_to_partition(<<"nonexistent">>, <<"node001">>)).

test_remove_node_from_partition() ->
    %% Add some nodes first
    ok = flurm_partition_registry:add_node_to_partition(<<"default">>, <<"nodeA">>),
    ok = flurm_partition_registry:add_node_to_partition(<<"default">>, <<"nodeB">>),

    %% Remove one
    ok = flurm_partition_registry:remove_node_from_partition(<<"default">>, <<"nodeA">>),

    %% Verify
    {ok, Nodes} = flurm_partition_registry:get_partition_nodes(<<"default">>),
    ?assertNot(lists:member(<<"nodeA">>, Nodes)),
    ?assert(lists:member(<<"nodeB">>, Nodes)).

test_set_default_partition() ->
    %% Create a new partition
    ok = flurm_partition_registry:register_partition(<<"newdefault">>, #{}),

    %% Set as default
    ok = flurm_partition_registry:set_default_partition(<<"newdefault">>),

    %% Verify
    {ok, DefaultName} = flurm_partition_registry:get_default_partition(),
    ?assertEqual(<<"newdefault">>, DefaultName),

    %% Non-existent partition
    ?assertEqual({error, not_found},
                 flurm_partition_registry:set_default_partition(<<"nonexistent">>)).

%%====================================================================
%% Update Partition Tests
%%====================================================================

update_partition_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"update partition", fun test_update_partition/0},
      {"update partition not found", fun test_update_partition_not_found/0},
      {"update partition multiple fields", fun test_update_partition_multiple/0}
     ]}.

test_update_partition() ->
    %% Register a partition
    ok = flurm_partition_registry:register_partition(<<"updateme">>, #{
        priority => 100,
        max_time => 3600
    }),

    %% Update priority
    ok = flurm_partition_registry:update_partition(<<"updateme">>, #{priority => 500}),

    %% Verify
    {ok, Updated} = flurm_partition_registry:get_partition(<<"updateme">>),
    ?assertEqual(500, maps:get(priority, Updated)).

test_update_partition_not_found() ->
    Result = flurm_partition_registry:update_partition(<<"nonexistent">>, #{priority => 100}),
    ?assertEqual({error, not_found}, Result).

test_update_partition_multiple() ->
    %% Register a partition with defaults
    ok = flurm_partition_registry:register_partition(<<"multi_update">>, #{}),

    %% Update multiple fields
    ok = flurm_partition_registry:update_partition(<<"multi_update">>, #{
        priority => 2000,
        state => drain,
        max_time => 172800,
        default_time => 7200,
        max_nodes => 50,
        allow_root => true,
        nodes => [<<"node1">>, <<"node2">>]
    }),

    %% Verify all updates
    {ok, Info} = flurm_partition_registry:get_partition(<<"multi_update">>),
    ?assertEqual(2000, maps:get(priority, Info)),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(172800, maps:get(max_time, Info)),
    ?assertEqual(7200, maps:get(default_time, Info)),
    ?assertEqual(50, maps:get(max_nodes, Info)),
    ?assertEqual(true, maps:get(allow_root, Info)),
    ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Info)).

%%====================================================================
%% Config Reload Tests
%%====================================================================

config_reload_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"config reload creates new partition", fun test_config_reload_create/0},
      {"config reload updates existing partition", fun test_config_reload_update/0},
      {"config reload with state values", fun test_config_reload_states/0},
      {"config changed handler", fun test_config_changed_handler/0},
      {"config changed for other keys ignored", fun test_config_changed_other_keys/0}
     ]}.

test_config_reload_create() ->
    %% Send config reload to create a new partition
    flurm_partition_registry ! {config_reload_partitions, [
        #{partitionname => <<"config_part">>, prioritytier => 1500, state => <<"UP">>}
    ]},
    _ = sys:get_state(flurm_partition_registry),

    %% Partition should exist
    {ok, Info} = flurm_partition_registry:get_partition(<<"config_part">>),
    ?assertEqual(<<"config_part">>, maps:get(name, Info)),
    ?assertEqual(1500, maps:get(priority, Info)),
    ?assertEqual(up, maps:get(state, Info)).

test_config_reload_update() ->
    %% First register a partition
    ok = flurm_partition_registry:register_partition(<<"reload_part">>, #{
        priority => 100,
        nodes => [<<"old_node">>]
    }),

    %% Send config reload to update it
    flurm_partition_registry ! {config_reload_partitions, [
        #{partitionname => <<"reload_part">>, prioritytier => 3000, nodes => <<"new_node1,new_node2">>}
    ]},
    _ = sys:get_state(flurm_partition_registry),

    %% Verify updates
    {ok, Info} = flurm_partition_registry:get_partition(<<"reload_part">>),
    ?assertEqual(3000, maps:get(priority, Info)).

test_config_reload_states() ->
    %% Test different state string formats
    flurm_partition_registry ! {config_reload_partitions, [
        #{partitionname => <<"state_up">>, state => <<"UP">>},
        #{partitionname => <<"state_down">>, state => <<"DOWN">>},
        #{partitionname => <<"state_drain">>, state => <<"DRAIN">>},
        #{partitionname => <<"state_atom">>, state => down}
    ]},
    _ = sys:get_state(flurm_partition_registry),

    {ok, UpInfo} = flurm_partition_registry:get_partition(<<"state_up">>),
    ?assertEqual(up, maps:get(state, UpInfo)),

    {ok, DownInfo} = flurm_partition_registry:get_partition(<<"state_down">>),
    ?assertEqual(down, maps:get(state, DownInfo)),

    {ok, DrainInfo} = flurm_partition_registry:get_partition(<<"state_drain">>),
    ?assertEqual(drain, maps:get(state, DrainInfo)),

    {ok, AtomInfo} = flurm_partition_registry:get_partition(<<"state_atom">>),
    ?assertEqual(down, maps:get(state, AtomInfo)).

test_config_changed_handler() ->
    %% Send config_changed message for partitions
    flurm_partition_registry ! {config_changed, partitions, [], [
        #{partitionname => <<"changed_part">>, prioritytier => 999}
    ]},
    _ = sys:get_state(flurm_partition_registry),

    %% Partition should be created
    {ok, Info} = flurm_partition_registry:get_partition(<<"changed_part">>),
    ?assertEqual(999, maps:get(priority, Info)).

test_config_changed_other_keys() ->
    %% Count partitions before
    PartsBefore = flurm_partition_registry:list_partitions(),

    %% Send config_changed for a different key (should be ignored)
    flurm_partition_registry ! {config_changed, nodes, [], []},
    flurm_partition_registry ! {config_changed, qos, [], []},
    flurm_partition_registry ! {config_changed, unknown_key, [], []},
    _ = sys:get_state(flurm_partition_registry),

    %% Partition count should be unchanged
    PartsAfter = flurm_partition_registry:list_partitions(),
    ?assertEqual(length(PartsBefore), length(PartsAfter)).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"unknown call returns error", fun test_unknown_call/0},
      {"unknown cast is ignored", fun test_unknown_cast/0},
      {"unknown info is ignored", fun test_unknown_info/0},
      {"code_change succeeds", fun test_code_change/0},
      {"terminate succeeds", fun test_terminate/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_partition_registry, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    %% Unknown cast should not crash the server
    ok = gen_server:cast(flurm_partition_registry, {unknown_cast_message}),
    _ = sys:get_state(flurm_partition_registry),
    ?assert(is_process_alive(whereis(flurm_partition_registry))),
    %% Should still work
    _ = flurm_partition_registry:list_partitions().

test_unknown_info() ->
    %% Unknown info message should not crash the server
    flurm_partition_registry ! {unknown_info_message, foo, bar},
    _ = sys:get_state(flurm_partition_registry),
    ?assert(is_process_alive(whereis(flurm_partition_registry))),
    %% Should still work
    _ = flurm_partition_registry:list_partitions().

test_code_change() ->
    Pid = whereis(flurm_partition_registry),
    sys:suspend(Pid),
    Result = sys:change_code(Pid, flurm_partition_registry, "1.0.0", []),
    ?assertEqual(ok, Result),
    sys:resume(Pid),
    %% Should still work
    _ = flurm_partition_registry:list_partitions().

test_terminate() ->
    %% Start a fresh registry for terminate test
    catch gen_server:stop(flurm_partition_registry),
    %% Process is stopped, start fresh
    {ok, Pid} = flurm_partition_registry:start_link(),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid, normal, 5000),
    %% Process is stopped, verify it's gone
    ?assertNot(is_process_alive(Pid)).

%%====================================================================
%% Register Partition with Default Flag Tests
%%====================================================================

default_flag_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"register partition with default flag sets as default", fun test_register_with_default_flag/0},
      {"unregister default partition clears default", fun test_unregister_clears_default/0}
     ]}.

test_register_with_default_flag() ->
    %% Register a partition marked as default
    ok = flurm_partition_registry:register_partition(<<"new_default_part">>, #{
        default => true,
        priority => 500
    }),

    %% Should be the new default
    {ok, DefaultName} = flurm_partition_registry:get_default_partition(),
    ?assertEqual(<<"new_default_part">>, DefaultName).

test_unregister_clears_default() ->
    %% Register and set as default
    ok = flurm_partition_registry:register_partition(<<"temp_default">>, #{default => true}),
    {ok, <<"temp_default">>} = flurm_partition_registry:get_default_partition(),

    %% Unregister it
    ok = flurm_partition_registry:unregister_partition(<<"temp_default">>),

    %% Default should be cleared (returns error or different partition)
    Result = flurm_partition_registry:get_default_partition(),
    %% Either error or a different partition name
    case Result of
        {error, not_set} -> ok;
        {ok, Name} when Name =/= <<"temp_default">> -> ok;
        _ -> ?assert(false)
    end.
