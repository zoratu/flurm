%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_partition module
%%%
%%% Tests cover:
%%% - Partition process creation
%%% - Node management (add/remove)
%%% - State management (up/down/drain)
%%% - Information retrieval
%%% - API variants (pid vs name)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

%% Create a partition spec for testing
make_partition_spec(Name) ->
    #partition_spec{
        name = Name,
        nodes = [],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100
    }.

make_partition_spec(Name, Nodes) ->
    #partition_spec{
        name = Name,
        nodes = Nodes,
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100
    }.

setup() ->
    %% Start a partition process for testing
    Spec = make_partition_spec(<<"test_partition">>),
    {ok, Pid} = flurm_partition:start_link(Spec),
    {started, Pid}.

setup_with_nodes() ->
    %% Start with some initial nodes
    Spec = make_partition_spec(<<"nodes_partition">>, [<<"node1">>, <<"node2">>]),
    {ok, Pid} = flurm_partition:start_link(Spec),
    {started, Pid}.

cleanup({started, Pid}) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid);
        false -> ok
    end.

%%====================================================================
%% Test Fixtures
%%====================================================================

partition_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Basic operations
      {"get nodes - empty", fun test_get_nodes_empty/0},
      {"get nodes by name", fun test_get_nodes_by_name/0},
      {"add node", fun test_add_node/0},
      {"add node by name", fun test_add_node_by_name/0},
      {"add node already member", fun test_add_node_already_member/0},
      {"remove node", fun test_remove_node/0},
      {"remove node by name", fun test_remove_node_by_name/0},
      {"remove node not member", fun test_remove_node_not_member/0},

      %% State management
      {"get state default", fun test_get_state_default/0},
      {"get state by name", fun test_get_state_by_name/0},
      {"set state up", fun test_set_state_up/0},
      {"set state down", fun test_set_state_down/0},
      {"set state drain", fun test_set_state_drain/0},
      {"set state by name", fun test_set_state_by_name/0},
      {"set state invalid", fun test_set_state_invalid/0},

      %% Info retrieval
      {"get info", fun test_get_info/0},
      {"get info by name", fun test_get_info_by_name/0}
     ]}.

partition_with_nodes_test_() ->
    {foreach,
     fun setup_with_nodes/0,
     fun cleanup/1,
     [
      {"get nodes with initial nodes", fun test_get_nodes_with_initial/0},
      {"max nodes limit", fun test_max_nodes_limit/0}
     ]}.

%%====================================================================
%% Node Management Tests
%%====================================================================

test_get_nodes_empty() ->
    %% Should return empty list for newly created partition
    {ok, Nodes} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assertEqual([], Nodes).

test_get_nodes_by_name() ->
    %% Use binary name to get nodes
    {ok, Nodes} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assertEqual([], Nodes).

test_add_node() ->
    %% Add a node to the partition
    ok = flurm_partition:add_node(<<"test_partition">>, <<"new_node">>),

    %% Verify node was added
    {ok, Nodes} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assert(lists:member(<<"new_node">>, Nodes)).

test_add_node_by_name() ->
    %% Add node using partition name
    ok = flurm_partition:add_node(<<"test_partition">>, <<"node_a">>),
    ok = flurm_partition:add_node(<<"test_partition">>, <<"node_b">>),

    {ok, Nodes} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"node_a">>, Nodes)),
    ?assert(lists:member(<<"node_b">>, Nodes)).

test_add_node_already_member() ->
    %% Add same node twice
    ok = flurm_partition:add_node(<<"test_partition">>, <<"dup_node">>),
    Result = flurm_partition:add_node(<<"test_partition">>, <<"dup_node">>),
    ?assertEqual({error, already_member}, Result).

test_remove_node() ->
    %% Add then remove
    ok = flurm_partition:add_node(<<"test_partition">>, <<"to_remove">>),
    {ok, Nodes1} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assert(lists:member(<<"to_remove">>, Nodes1)),

    ok = flurm_partition:remove_node(<<"test_partition">>, <<"to_remove">>),
    {ok, Nodes2} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assertNot(lists:member(<<"to_remove">>, Nodes2)).

test_remove_node_by_name() ->
    ok = flurm_partition:add_node(<<"test_partition">>, <<"nodeX">>),
    ok = flurm_partition:remove_node(<<"test_partition">>, <<"nodeX">>),

    {ok, Nodes} = flurm_partition:get_nodes(<<"test_partition">>),
    ?assertNot(lists:member(<<"nodeX">>, Nodes)).

test_remove_node_not_member() ->
    Result = flurm_partition:remove_node(<<"test_partition">>, <<"nonexistent">>),
    ?assertEqual({error, not_member}, Result).

test_get_nodes_with_initial() ->
    {ok, Nodes} = flurm_partition:get_nodes(<<"nodes_partition">>),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"node1">>, Nodes)),
    ?assert(lists:member(<<"node2">>, Nodes)).

test_max_nodes_limit() ->
    %% Partition has max_nodes = 10, already has 2
    %% Add 8 more to fill it
    lists:foreach(fun(I) ->
        NodeName = iolist_to_binary([<<"extra">>, integer_to_binary(I)]),
        ok = flurm_partition:add_node(<<"nodes_partition">>, NodeName)
    end, lists:seq(1, 8)),

    %% Should now have 10 nodes
    {ok, Nodes} = flurm_partition:get_nodes(<<"nodes_partition">>),
    ?assertEqual(10, length(Nodes)),

    %% Adding one more should fail
    Result = flurm_partition:add_node(<<"nodes_partition">>, <<"overflow">>),
    ?assertEqual({error, max_nodes_reached}, Result).

%%====================================================================
%% State Management Tests
%%====================================================================

test_get_state_default() ->
    {ok, State} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(up, State).

test_get_state_by_name() ->
    {ok, State} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(up, State).

test_set_state_up() ->
    ok = flurm_partition:set_state(<<"test_partition">>, up),
    {ok, State} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(up, State).

test_set_state_down() ->
    ok = flurm_partition:set_state(<<"test_partition">>, down),
    {ok, State} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(down, State).

test_set_state_drain() ->
    ok = flurm_partition:set_state(<<"test_partition">>, drain),
    {ok, State} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(drain, State).

test_set_state_by_name() ->
    ok = flurm_partition:set_state(<<"test_partition">>, down),
    {ok, State} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(down, State),

    ok = flurm_partition:set_state(<<"test_partition">>, up),
    {ok, State2} = flurm_partition:get_state(<<"test_partition">>),
    ?assertEqual(up, State2).

test_set_state_invalid() ->
    Result = flurm_partition:set_state(<<"test_partition">>, invalid_state),
    ?assertEqual({error, invalid_state}, Result).

%%====================================================================
%% Info Retrieval Tests
%%====================================================================

test_get_info() ->
    {ok, Info} = flurm_partition:get_info(<<"test_partition">>),

    ?assert(is_map(Info)),
    ?assertEqual(<<"test_partition">>, maps:get(name, Info)),
    ?assertEqual([], maps:get(nodes, Info)),
    ?assertEqual(0, maps:get(node_count, Info)),
    ?assertEqual(86400, maps:get(max_time, Info)),
    ?assertEqual(3600, maps:get(default_time, Info)),
    ?assertEqual(10, maps:get(max_nodes, Info)),
    ?assertEqual(100, maps:get(priority, Info)),
    ?assertEqual(up, maps:get(state, Info)).

test_get_info_by_name() ->
    %% Add some nodes first
    ok = flurm_partition:add_node(<<"test_partition">>, <<"info_node1">>),
    ok = flurm_partition:add_node(<<"test_partition">>, <<"info_node2">>),

    {ok, Info} = flurm_partition:get_info(<<"test_partition">>),

    ?assertEqual(2, maps:get(node_count, Info)),
    ?assert(lists:member(<<"info_node1">>, maps:get(nodes, Info))),
    ?assert(lists:member(<<"info_node2">>, maps:get(nodes, Info))).

%%====================================================================
%% PID-based API Tests
%%====================================================================

pid_api_test_() ->
    {foreach,
     fun() ->
         Spec = make_partition_spec(<<"pid_test_partition">>),
         {ok, Pid} = flurm_partition:start_link(Spec),
         {started, Pid}
     end,
     fun cleanup/1,
     [
      {"get nodes by pid", fun test_get_nodes_pid/0},
      {"add node by pid", fun test_add_node_pid/0},
      {"remove node by pid", fun test_remove_node_pid/0},
      {"get state by pid", fun test_get_state_pid/0},
      {"set state by pid", fun test_set_state_pid/0},
      {"get info by pid", fun test_get_info_pid/0}
     ]}.

get_pid() ->
    whereis(flurm_partition_pid_test_partition).

test_get_nodes_pid() ->
    Pid = get_pid(),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assertEqual([], Nodes).

test_add_node_pid() ->
    Pid = get_pid(),
    ok = flurm_partition:add_node(Pid, <<"pid_node">>),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assert(lists:member(<<"pid_node">>, Nodes)).

test_remove_node_pid() ->
    Pid = get_pid(),
    ok = flurm_partition:add_node(Pid, <<"to_remove_pid">>),
    ok = flurm_partition:remove_node(Pid, <<"to_remove_pid">>),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assertNot(lists:member(<<"to_remove_pid">>, Nodes)).

test_get_state_pid() ->
    Pid = get_pid(),
    {ok, State} = flurm_partition:get_state(Pid),
    ?assertEqual(up, State).

test_set_state_pid() ->
    Pid = get_pid(),
    ok = flurm_partition:set_state(Pid, drain),
    {ok, State} = flurm_partition:get_state(Pid),
    ?assertEqual(drain, State).

test_get_info_pid() ->
    Pid = get_pid(),
    {ok, Info} = flurm_partition:get_info(Pid),
    ?assert(is_map(Info)),
    ?assertEqual(<<"pid_test_partition">>, maps:get(name, Info)).

%%====================================================================
%% Multiple Partition Tests
%%====================================================================

multiple_partitions_test_() ->
    {foreach,
     fun() ->
         %% Start multiple partitions
         Spec1 = make_partition_spec(<<"multi_part_a">>),
         Spec2 = make_partition_spec(<<"multi_part_b">>),
         Spec3 = #partition_spec{
             name = <<"multi_part_c">>,
             nodes = [],
             max_time = 172800,
             default_time = 7200,
             max_nodes = 20,
             priority = 200
         },
         {ok, Pid1} = flurm_partition:start_link(Spec1),
         {ok, Pid2} = flurm_partition:start_link(Spec2),
         {ok, Pid3} = flurm_partition:start_link(Spec3),
         {started, [Pid1, Pid2, Pid3]}
     end,
     fun({started, Pids}) ->
         lists:foreach(fun(Pid) ->
             case is_process_alive(Pid) of
                 true -> gen_server:stop(Pid);
                 false -> ok
             end
         end, Pids)
     end,
     [
      {"partitions are independent", fun test_partitions_independent/0},
      {"different partition configs", fun test_different_configs/0}
     ]}.

test_partitions_independent() ->
    %% Add nodes to different partitions
    ok = flurm_partition:add_node(<<"multi_part_a">>, <<"nodeA">>),
    ok = flurm_partition:add_node(<<"multi_part_b">>, <<"nodeB">>),

    %% Verify independence
    {ok, NodesA} = flurm_partition:get_nodes(<<"multi_part_a">>),
    {ok, NodesB} = flurm_partition:get_nodes(<<"multi_part_b">>),

    ?assert(lists:member(<<"nodeA">>, NodesA)),
    ?assertNot(lists:member(<<"nodeB">>, NodesA)),

    ?assert(lists:member(<<"nodeB">>, NodesB)),
    ?assertNot(lists:member(<<"nodeA">>, NodesB)),

    %% Set different states
    ok = flurm_partition:set_state(<<"multi_part_a">>, down),
    ok = flurm_partition:set_state(<<"multi_part_b">>, drain),

    {ok, StateA} = flurm_partition:get_state(<<"multi_part_a">>),
    {ok, StateB} = flurm_partition:get_state(<<"multi_part_b">>),

    ?assertEqual(down, StateA),
    ?assertEqual(drain, StateB).

test_different_configs() ->
    {ok, InfoA} = flurm_partition:get_info(<<"multi_part_a">>),
    {ok, InfoC} = flurm_partition:get_info(<<"multi_part_c">>),

    %% Part A has default config
    ?assertEqual(86400, maps:get(max_time, InfoA)),
    ?assertEqual(3600, maps:get(default_time, InfoA)),
    ?assertEqual(10, maps:get(max_nodes, InfoA)),
    ?assertEqual(100, maps:get(priority, InfoA)),

    %% Part C has custom config
    ?assertEqual(172800, maps:get(max_time, InfoC)),
    ?assertEqual(7200, maps:get(default_time, InfoC)),
    ?assertEqual(20, maps:get(max_nodes, InfoC)),
    ?assertEqual(200, maps:get(priority, InfoC)).

%%====================================================================
%% Unknown Request Tests
%%====================================================================

unknown_request_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle unknown call", fun test_unknown_call/0},
      {"handle unknown cast", fun test_unknown_cast/0},
      {"handle unknown info", fun test_unknown_info/0},
      {"handle code_change", fun test_code_change/0}
     ]}.

test_unknown_call() ->
    Pid = whereis(flurm_partition_test_partition),
    Result = gen_server:call(Pid, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    Pid = whereis(flurm_partition_test_partition),
    %% Unknown cast should be ignored without crashing
    ok = gen_server:cast(Pid, {unknown_cast_message, bar}),
    timer:sleep(50),
    ?assert(is_process_alive(Pid)),
    %% Process should still work
    {ok, State} = flurm_partition:get_state(Pid),
    ?assertEqual(up, State).

test_unknown_info() ->
    Pid = whereis(flurm_partition_test_partition),
    %% Unknown info message should be ignored without crashing
    Pid ! {unknown_info_message, baz},
    timer:sleep(50),
    ?assert(is_process_alive(Pid)),
    %% Process should still work
    {ok, State} = flurm_partition:get_state(Pid),
    ?assertEqual(up, State).

test_code_change() ->
    Pid = whereis(flurm_partition_test_partition),
    %% Test code_change callback via sys module
    sys:suspend(Pid),
    Result = sys:change_code(Pid, flurm_partition, "1.0.0", []),
    ?assertEqual(ok, Result),
    sys:resume(Pid),
    %% Verify process still works after code change
    {ok, Info} = flurm_partition:get_info(Pid),
    ?assertEqual(<<"test_partition">>, maps:get(name, Info)).

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {"partition terminates cleanly", fun test_terminate_clean/0}.

test_terminate_clean() ->
    %% Start a fresh partition for terminate test
    Spec = make_partition_spec(<<"term_test_partition">>),
    {ok, Pid} = flurm_partition:start_link(Spec),
    ?assert(is_process_alive(Pid)),
    %% Stop the partition
    gen_server:stop(Pid, normal, 5000),
    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)).
