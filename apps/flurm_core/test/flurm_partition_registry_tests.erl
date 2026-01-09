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
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    catch ets:delete(flurm_partitions),
    gen_server:stop(flurm_partition_registry);
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
