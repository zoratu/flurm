%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_partition module
%%%
%%% These tests directly test the gen_server callbacks without any
%%% mocking (no meck). Each test creates proper state structures and
%%% calls the callback functions directly.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Helper to create a partition spec for testing
make_partition_spec() ->
    #partition_spec{
        name = <<"test_partition">>,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100
    }.

make_partition_spec(Name, Nodes, MaxNodes) ->
    #partition_spec{
        name = Name,
        nodes = Nodes,
        max_time = 3600,
        default_time = 1800,
        max_nodes = MaxNodes,
        priority = 100
    }.

%% Helper to create a partition state for testing
make_partition_state() ->
    #partition_state{
        name = <<"test_partition">>,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        state = up
    }.

make_partition_state(Name, Nodes, MaxNodes, PartState) ->
    #partition_state{
        name = Name,
        nodes = Nodes,
        max_time = 3600,
        default_time = 1800,
        max_nodes = MaxNodes,
        priority = 100,
        state = PartState
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 tests",
     [
      {"init creates correct state from partition spec",
       fun() ->
           Spec = make_partition_spec(),
           {ok, State} = flurm_partition:init([Spec]),
           ?assertEqual(<<"test_partition">>, State#partition_state.name),
           ?assertEqual([<<"node1">>, <<"node2">>], State#partition_state.nodes),
           ?assertEqual(3600, State#partition_state.max_time),
           ?assertEqual(1800, State#partition_state.default_time),
           ?assertEqual(10, State#partition_state.max_nodes),
           ?assertEqual(100, State#partition_state.priority),
           ?assertEqual(up, State#partition_state.state)
       end},

      {"init with empty nodes list",
       fun() ->
           Spec = make_partition_spec(<<"empty_partition">>, [], 5),
           {ok, State} = flurm_partition:init([Spec]),
           ?assertEqual(<<"empty_partition">>, State#partition_state.name),
           ?assertEqual([], State#partition_state.nodes),
           ?assertEqual(up, State#partition_state.state)
       end},

      {"init with single node",
       fun() ->
           Spec = make_partition_spec(<<"single_node_part">>, [<<"nodeA">>], 1),
           {ok, State} = flurm_partition:init([Spec]),
           ?assertEqual([<<"nodeA">>], State#partition_state.nodes),
           ?assertEqual(1, State#partition_state.max_nodes)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - get_nodes
%%====================================================================

handle_call_get_nodes_test_() ->
    {"handle_call get_nodes tests",
     [
      {"get_nodes returns node list",
       fun() ->
           State = make_partition_state(),
           {reply, {ok, Nodes}, NewState} =
               flurm_partition:handle_call(get_nodes, {self(), make_ref()}, State),
           ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
           ?assertEqual(State, NewState)
       end},

      {"get_nodes with empty node list",
       fun() ->
           State = make_partition_state(<<"test">>, [], 5, up),
           {reply, {ok, Nodes}, _} =
               flurm_partition:handle_call(get_nodes, {self(), make_ref()}, State),
           ?assertEqual([], Nodes)
       end},

      {"get_nodes with many nodes",
       fun() ->
           ManyNodes = [list_to_binary("node" ++ integer_to_list(N)) || N <- lists:seq(1, 100)],
           State = make_partition_state(<<"large_part">>, ManyNodes, 200, up),
           {reply, {ok, Nodes}, _} =
               flurm_partition:handle_call(get_nodes, {self(), make_ref()}, State),
           ?assertEqual(100, length(Nodes)),
           ?assertEqual(ManyNodes, Nodes)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - add_node
%%====================================================================

handle_call_add_node_test_() ->
    {"handle_call add_node tests",
     [
      {"add_node successfully adds new node",
       fun() ->
           State = make_partition_state(),
           {reply, ok, NewState} =
               flurm_partition:handle_call({add_node, <<"node3">>}, {self(), make_ref()}, State),
           ?assertEqual([<<"node3">>, <<"node1">>, <<"node2">>], NewState#partition_state.nodes)
       end},

      {"add_node fails for existing node",
       fun() ->
           State = make_partition_state(),
           {reply, {error, already_member}, NewState} =
               flurm_partition:handle_call({add_node, <<"node1">>}, {self(), make_ref()}, State),
           ?assertEqual(State, NewState)
       end},

      {"add_node fails when max_nodes reached",
       fun() ->
           State = make_partition_state(<<"full_part">>, [<<"n1">>, <<"n2">>], 2, up),
           {reply, {error, max_nodes_reached}, NewState} =
               flurm_partition:handle_call({add_node, <<"n3">>}, {self(), make_ref()}, State),
           ?assertEqual(State, NewState)
       end},

      {"add_node to empty partition",
       fun() ->
           State = make_partition_state(<<"empty">>, [], 5, up),
           {reply, ok, NewState} =
               flurm_partition:handle_call({add_node, <<"first_node">>}, {self(), make_ref()}, State),
           ?assertEqual([<<"first_node">>], NewState#partition_state.nodes)
       end},

      {"add_node at capacity boundary",
       fun() ->
           %% State with 9 nodes, max is 10
           Nodes = [list_to_binary("n" ++ integer_to_list(N)) || N <- lists:seq(1, 9)],
           State = make_partition_state(<<"boundary">>, Nodes, 10, up),
           %% Can add one more
           {reply, ok, NewState} =
               flurm_partition:handle_call({add_node, <<"n10">>}, {self(), make_ref()}, State),
           ?assertEqual(10, length(NewState#partition_state.nodes)),
           %% Now full, can't add another
           {reply, {error, max_nodes_reached}, _} =
               flurm_partition:handle_call({add_node, <<"n11">>}, {self(), make_ref()}, NewState)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - remove_node
%%====================================================================

handle_call_remove_node_test_() ->
    {"handle_call remove_node tests",
     [
      {"remove_node successfully removes existing node",
       fun() ->
           State = make_partition_state(),
           {reply, ok, NewState} =
               flurm_partition:handle_call({remove_node, <<"node1">>}, {self(), make_ref()}, State),
           ?assertEqual([<<"node2">>], NewState#partition_state.nodes)
       end},

      {"remove_node fails for non-existent node",
       fun() ->
           State = make_partition_state(),
           {reply, {error, not_member}, NewState} =
               flurm_partition:handle_call({remove_node, <<"nonexistent">>}, {self(), make_ref()}, State),
           ?assertEqual(State, NewState)
       end},

      {"remove_node from single node partition",
       fun() ->
           State = make_partition_state(<<"single">>, [<<"only_node">>], 5, up),
           {reply, ok, NewState} =
               flurm_partition:handle_call({remove_node, <<"only_node">>}, {self(), make_ref()}, State),
           ?assertEqual([], NewState#partition_state.nodes)
       end},

      {"remove_node from empty partition fails",
       fun() ->
           State = make_partition_state(<<"empty">>, [], 5, up),
           {reply, {error, not_member}, NewState} =
               flurm_partition:handle_call({remove_node, <<"any_node">>}, {self(), make_ref()}, State),
           ?assertEqual(State, NewState)
       end},

      {"remove all nodes sequentially",
       fun() ->
           State0 = make_partition_state(<<"multi">>, [<<"a">>, <<"b">>, <<"c">>], 10, up),
           {reply, ok, State1} =
               flurm_partition:handle_call({remove_node, <<"a">>}, {self(), make_ref()}, State0),
           {reply, ok, State2} =
               flurm_partition:handle_call({remove_node, <<"b">>}, {self(), make_ref()}, State1),
           {reply, ok, State3} =
               flurm_partition:handle_call({remove_node, <<"c">>}, {self(), make_ref()}, State2),
           ?assertEqual([], State3#partition_state.nodes)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - get_info
%%====================================================================

handle_call_get_info_test_() ->
    {"handle_call get_info tests",
     [
      {"get_info returns correct partition info map",
       fun() ->
           State = make_partition_state(),
           {reply, {ok, Info}, NewState} =
               flurm_partition:handle_call(get_info, {self(), make_ref()}, State),
           ?assertEqual(State, NewState),
           ?assertEqual(<<"test_partition">>, maps:get(name, Info)),
           ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Info)),
           ?assertEqual(2, maps:get(node_count, Info)),
           ?assertEqual(3600, maps:get(max_time, Info)),
           ?assertEqual(1800, maps:get(default_time, Info)),
           ?assertEqual(10, maps:get(max_nodes, Info)),
           ?assertEqual(100, maps:get(priority, Info)),
           ?assertEqual(up, maps:get(state, Info))
       end},

      {"get_info with empty nodes",
       fun() ->
           State = make_partition_state(<<"empty">>, [], 5, down),
           {reply, {ok, Info}, _} =
               flurm_partition:handle_call(get_info, {self(), make_ref()}, State),
           ?assertEqual([], maps:get(nodes, Info)),
           ?assertEqual(0, maps:get(node_count, Info)),
           ?assertEqual(down, maps:get(state, Info))
       end},

      {"get_info reflects drain state",
       fun() ->
           State = make_partition_state(<<"draining">>, [<<"x">>], 5, drain),
           {reply, {ok, Info}, _} =
               flurm_partition:handle_call(get_info, {self(), make_ref()}, State),
           ?assertEqual(drain, maps:get(state, Info))
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - set_state
%%====================================================================

handle_call_set_state_test_() ->
    {"handle_call set_state tests",
     [
      {"set_state to up",
       fun() ->
           State = make_partition_state(<<"test">>, [], 5, down),
           {reply, ok, NewState} =
               flurm_partition:handle_call({set_state, up}, {self(), make_ref()}, State),
           ?assertEqual(up, NewState#partition_state.state)
       end},

      {"set_state to down",
       fun() ->
           State = make_partition_state(),
           {reply, ok, NewState} =
               flurm_partition:handle_call({set_state, down}, {self(), make_ref()}, State),
           ?assertEqual(down, NewState#partition_state.state)
       end},

      {"set_state to drain",
       fun() ->
           State = make_partition_state(),
           {reply, ok, NewState} =
               flurm_partition:handle_call({set_state, drain}, {self(), make_ref()}, State),
           ?assertEqual(drain, NewState#partition_state.state)
       end},

      {"set_state with invalid state",
       fun() ->
           State = make_partition_state(),
           {reply, {error, invalid_state}, NewState} =
               flurm_partition:handle_call({set_state, invalid}, {self(), make_ref()}, State),
           ?assertEqual(State, NewState),
           ?assertEqual(up, NewState#partition_state.state)
       end},

      {"set_state with atom that's not a valid state",
       fun() ->
           State = make_partition_state(),
           {reply, {error, invalid_state}, _} =
               flurm_partition:handle_call({set_state, running}, {self(), make_ref()}, State)
       end},

      {"set_state preserves other state fields",
       fun() ->
           State = make_partition_state(),
           {reply, ok, NewState} =
               flurm_partition:handle_call({set_state, drain}, {self(), make_ref()}, State),
           ?assertEqual(State#partition_state.name, NewState#partition_state.name),
           ?assertEqual(State#partition_state.nodes, NewState#partition_state.nodes),
           ?assertEqual(State#partition_state.max_time, NewState#partition_state.max_time),
           ?assertEqual(State#partition_state.max_nodes, NewState#partition_state.max_nodes),
           ?assertEqual(State#partition_state.priority, NewState#partition_state.priority)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - get_state
%%====================================================================

handle_call_get_state_test_() ->
    {"handle_call get_state tests",
     [
      {"get_state returns up",
       fun() ->
           State = make_partition_state(<<"test">>, [], 5, up),
           {reply, {ok, PartState}, NewState} =
               flurm_partition:handle_call(get_state, {self(), make_ref()}, State),
           ?assertEqual(up, PartState),
           ?assertEqual(State, NewState)
       end},

      {"get_state returns down",
       fun() ->
           State = make_partition_state(<<"test">>, [], 5, down),
           {reply, {ok, PartState}, _} =
               flurm_partition:handle_call(get_state, {self(), make_ref()}, State),
           ?assertEqual(down, PartState)
       end},

      {"get_state returns drain",
       fun() ->
           State = make_partition_state(<<"test">>, [], 5, drain),
           {reply, {ok, PartState}, _} =
               flurm_partition:handle_call(get_state, {self(), make_ref()}, State),
           ?assertEqual(drain, PartState)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

handle_call_unknown_test_() ->
    {"handle_call unknown request tests",
     [
      {"unknown request returns error",
       fun() ->
           State = make_partition_state(),
           {reply, {error, unknown_request}, NewState} =
               flurm_partition:handle_call(unknown_command, {self(), make_ref()}, State),
           ?assertEqual(State, NewState)
       end},

      {"random tuple request returns error",
       fun() ->
           State = make_partition_state(),
           {reply, {error, unknown_request}, _} =
               flurm_partition:handle_call({random, stuff}, {self(), make_ref()}, State)
       end}
     ]}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    {"handle_cast tests",
     [
      {"handle_cast ignores any message",
       fun() ->
           State = make_partition_state(),
           {noreply, NewState} = flurm_partition:handle_cast(any_message, State),
           ?assertEqual(State, NewState)
       end},

      {"handle_cast with tuple message",
       fun() ->
           State = make_partition_state(),
           {noreply, NewState} = flurm_partition:handle_cast({some, tuple}, State),
           ?assertEqual(State, NewState)
       end}
     ]}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests",
     [
      {"handle_info ignores any message",
       fun() ->
           State = make_partition_state(),
           {noreply, NewState} = flurm_partition:handle_info(any_info, State),
           ?assertEqual(State, NewState)
       end},

      {"handle_info with complex message",
       fun() ->
           State = make_partition_state(),
           {noreply, NewState} = flurm_partition:handle_info({timeout, make_ref(), check}, State),
           ?assertEqual(State, NewState)
       end}
     ]}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests",
     [
      {"terminate returns ok for normal shutdown",
       fun() ->
           State = make_partition_state(),
           ?assertEqual(ok, flurm_partition:terminate(normal, State))
       end},

      {"terminate returns ok for shutdown reason",
       fun() ->
           State = make_partition_state(),
           ?assertEqual(ok, flurm_partition:terminate(shutdown, State))
       end},

      {"terminate returns ok for error reason",
       fun() ->
           State = make_partition_state(),
           ?assertEqual(ok, flurm_partition:terminate({error, some_reason}, State))
       end}
     ]}.

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    {"code_change tests",
     [
      {"code_change returns state unchanged",
       fun() ->
           State = make_partition_state(),
           {ok, NewState} = flurm_partition:code_change("1.0", State, []),
           ?assertEqual(State, NewState)
       end},

      {"code_change with extra data",
       fun() ->
           State = make_partition_state(),
           {ok, NewState} = flurm_partition:code_change("2.0", State, {extra, data}),
           ?assertEqual(State, NewState)
       end}
     ]}.

%%====================================================================
%% Integration-style Tests (still pure, no mocking)
%%====================================================================

workflow_test_() ->
    {"workflow tests - simulating real usage patterns",
     [
      {"full partition lifecycle",
       fun() ->
           %% Create partition
           Spec = make_partition_spec(<<"lifecycle_test">>, [], 5),
           {ok, State0} = flurm_partition:init([Spec]),
           ?assertEqual([], State0#partition_state.nodes),
           ?assertEqual(up, State0#partition_state.state),

           %% Add nodes
           {reply, ok, State1} =
               flurm_partition:handle_call({add_node, <<"node_a">>}, {self(), make_ref()}, State0),
           {reply, ok, State2} =
               flurm_partition:handle_call({add_node, <<"node_b">>}, {self(), make_ref()}, State1),
           ?assertEqual(2, length(State2#partition_state.nodes)),

           %% Get info
           {reply, {ok, Info}, _} =
               flurm_partition:handle_call(get_info, {self(), make_ref()}, State2),
           ?assertEqual(2, maps:get(node_count, Info)),

           %% Drain partition
           {reply, ok, State3} =
               flurm_partition:handle_call({set_state, drain}, {self(), make_ref()}, State2),
           {reply, {ok, drain}, _} =
               flurm_partition:handle_call(get_state, {self(), make_ref()}, State3),

           %% Remove nodes
           {reply, ok, State4} =
               flurm_partition:handle_call({remove_node, <<"node_a">>}, {self(), make_ref()}, State3),
           {reply, ok, State5} =
               flurm_partition:handle_call({remove_node, <<"node_b">>}, {self(), make_ref()}, State4),
           ?assertEqual([], State5#partition_state.nodes),

           %% Terminate
           ?assertEqual(ok, flurm_partition:terminate(shutdown, State5))
       end},

      {"state transitions",
       fun() ->
           State0 = make_partition_state(<<"state_test">>, [<<"n1">>], 10, up),

           %% up -> down
           {reply, ok, State1} =
               flurm_partition:handle_call({set_state, down}, {self(), make_ref()}, State0),
           ?assertEqual(down, State1#partition_state.state),

           %% down -> drain
           {reply, ok, State2} =
               flurm_partition:handle_call({set_state, drain}, {self(), make_ref()}, State1),
           ?assertEqual(drain, State2#partition_state.state),

           %% drain -> up
           {reply, ok, State3} =
               flurm_partition:handle_call({set_state, up}, {self(), make_ref()}, State2),
           ?assertEqual(up, State3#partition_state.state)
       end},

      {"node management under capacity constraints",
       fun() ->
           %% Start with partition allowing max 3 nodes
           State0 = make_partition_state(<<"capacity_test">>, [], 3, up),

           %% Add 3 nodes
           {reply, ok, State1} =
               flurm_partition:handle_call({add_node, <<"n1">>}, {self(), make_ref()}, State0),
           {reply, ok, State2} =
               flurm_partition:handle_call({add_node, <<"n2">>}, {self(), make_ref()}, State1),
           {reply, ok, State3} =
               flurm_partition:handle_call({add_node, <<"n3">>}, {self(), make_ref()}, State2),

           %% Verify full
           {reply, {error, max_nodes_reached}, _} =
               flurm_partition:handle_call({add_node, <<"n4">>}, {self(), make_ref()}, State3),

           %% Remove one
           {reply, ok, State4} =
               flurm_partition:handle_call({remove_node, <<"n2">>}, {self(), make_ref()}, State3),

           %% Now can add
           {reply, ok, State5} =
               flurm_partition:handle_call({add_node, <<"n4">>}, {self(), make_ref()}, State4),
           ?assertEqual(3, length(State5#partition_state.nodes))
       end}
     ]}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {"edge case tests",
     [
      {"partition with unicode name",
       fun() ->
           Spec = #partition_spec{
               name = <<"partition_">>,
               nodes = [],
               max_time = 1000,
               default_time = 500,
               max_nodes = 10,
               priority = 50
           },
           {ok, State} = flurm_partition:init([Spec]),
           ?assertEqual(<<"partition_">>, State#partition_state.name)
       end},

      {"partition with very long node names",
       fun() ->
           LongName = list_to_binary(lists:duplicate(1000, $x)),
           State0 = make_partition_state(<<"long_test">>, [], 5, up),
           {reply, ok, State1} =
               flurm_partition:handle_call({add_node, LongName}, {self(), make_ref()}, State0),
           {reply, {ok, Nodes}, _} =
               flurm_partition:handle_call(get_nodes, {self(), make_ref()}, State1),
           ?assertEqual([LongName], Nodes)
       end},

      {"partition with max_nodes = 1",
       fun() ->
           State0 = make_partition_state(<<"single_max">>, [], 1, up),
           {reply, ok, State1} =
               flurm_partition:handle_call({add_node, <<"only">>}, {self(), make_ref()}, State0),
           {reply, {error, max_nodes_reached}, _} =
               flurm_partition:handle_call({add_node, <<"second">>}, {self(), make_ref()}, State1)
       end},

      {"duplicate add followed by remove",
       fun() ->
           State0 = make_partition_state(<<"dup_test">>, [<<"existing">>], 5, up),
           %% Try to add existing node
           {reply, {error, already_member}, _} =
               flurm_partition:handle_call({add_node, <<"existing">>}, {self(), make_ref()}, State0),
           %% Now remove it
           {reply, ok, State1} =
               flurm_partition:handle_call({remove_node, <<"existing">>}, {self(), make_ref()}, State0),
           %% Now can add it
           {reply, ok, _} =
               flurm_partition:handle_call({add_node, <<"existing">>}, {self(), make_ref()}, State1)
       end}
     ]}.

%%====================================================================
%% API Tests - Testing with actual gen_server process
%%====================================================================

%% Generate unique partition names to avoid conflicts
unique_partition_name() ->
    Unique = integer_to_binary(erlang:unique_integer([positive])),
    <<"test_api_partition_", Unique/binary>>.

api_start_link_test_() ->
    {"start_link API tests",
     [
      {"start_link creates gen_server process",
       fun() ->
           Name = unique_partition_name(),
           Spec = #partition_spec{
               name = Name,
               nodes = [<<"n1">>],
               max_time = 3600,
               default_time = 1800,
               max_nodes = 10,
               priority = 100
           },
           {ok, Pid} = flurm_partition:start_link(Spec),
           ?assert(is_pid(Pid)),
           ?assert(is_process_alive(Pid)),
           %% Clean up
           gen_server:stop(Pid)
       end}
     ]}.

api_get_nodes_test_() ->
    {"get_nodes API tests",
     {foreach,
      fun() ->
          Name = unique_partition_name(),
          Spec = #partition_spec{
              name = Name,
              nodes = [<<"node1">>, <<"node2">>],
              max_time = 3600,
              default_time = 1800,
              max_nodes = 10,
              priority = 100
          },
          {ok, Pid} = flurm_partition:start_link(Spec),
          {Pid, Name}
      end,
      fun({Pid, _Name}) ->
          gen_server:stop(Pid)
      end,
      [
       fun({Pid, _Name}) ->
           {"get_nodes with pid",
            fun() ->
                {ok, Nodes} = flurm_partition:get_nodes(Pid),
                ?assertEqual([<<"node1">>, <<"node2">>], Nodes)
            end}
       end,
       fun({_Pid, Name}) ->
           {"get_nodes with name",
            fun() ->
                {ok, Nodes} = flurm_partition:get_nodes(Name),
                ?assertEqual([<<"node1">>, <<"node2">>], Nodes)
            end}
       end
      ]}}.

api_add_node_test_() ->
    {"add_node API tests",
     {foreach,
      fun() ->
          Name = unique_partition_name(),
          Spec = #partition_spec{
              name = Name,
              nodes = [],
              max_time = 3600,
              default_time = 1800,
              max_nodes = 5,
              priority = 100
          },
          {ok, Pid} = flurm_partition:start_link(Spec),
          {Pid, Name}
      end,
      fun({Pid, _Name}) ->
          gen_server:stop(Pid)
      end,
      [
       fun({Pid, _Name}) ->
           {"add_node with pid",
            fun() ->
                ok = flurm_partition:add_node(Pid, <<"new_node">>),
                {ok, Nodes} = flurm_partition:get_nodes(Pid),
                ?assert(lists:member(<<"new_node">>, Nodes))
            end}
       end,
       fun({_Pid, Name}) ->
           {"add_node with name",
            fun() ->
                ok = flurm_partition:add_node(Name, <<"another_node">>),
                {ok, Nodes} = flurm_partition:get_nodes(Name),
                ?assert(lists:member(<<"another_node">>, Nodes))
            end}
       end
      ]}}.

api_remove_node_test_() ->
    {"remove_node API tests",
     {foreach,
      fun() ->
          Name = unique_partition_name(),
          Spec = #partition_spec{
              name = Name,
              nodes = [<<"nodeA">>, <<"nodeB">>],
              max_time = 3600,
              default_time = 1800,
              max_nodes = 5,
              priority = 100
          },
          {ok, Pid} = flurm_partition:start_link(Spec),
          {Pid, Name}
      end,
      fun({Pid, _Name}) ->
          gen_server:stop(Pid)
      end,
      [
       fun({Pid, _Name}) ->
           {"remove_node with pid",
            fun() ->
                ok = flurm_partition:remove_node(Pid, <<"nodeA">>),
                {ok, Nodes} = flurm_partition:get_nodes(Pid),
                ?assertNot(lists:member(<<"nodeA">>, Nodes))
            end}
       end,
       fun({_Pid, Name}) ->
           {"remove_node with name",
            fun() ->
                ok = flurm_partition:remove_node(Name, <<"nodeB">>),
                {ok, Nodes} = flurm_partition:get_nodes(Name),
                ?assertNot(lists:member(<<"nodeB">>, Nodes))
            end}
       end
      ]}}.

api_get_info_test_() ->
    {"get_info API tests",
     {foreach,
      fun() ->
          Name = unique_partition_name(),
          Spec = #partition_spec{
              name = Name,
              nodes = [<<"info_node">>],
              max_time = 7200,
              default_time = 900,
              max_nodes = 20,
              priority = 50
          },
          {ok, Pid} = flurm_partition:start_link(Spec),
          {Pid, Name}
      end,
      fun({Pid, _Name}) ->
          gen_server:stop(Pid)
      end,
      [
       fun({Pid, Name}) ->
           {"get_info with pid",
            fun() ->
                {ok, Info} = flurm_partition:get_info(Pid),
                ?assertEqual(Name, maps:get(name, Info)),
                ?assertEqual([<<"info_node">>], maps:get(nodes, Info)),
                ?assertEqual(1, maps:get(node_count, Info)),
                ?assertEqual(7200, maps:get(max_time, Info)),
                ?assertEqual(900, maps:get(default_time, Info)),
                ?assertEqual(20, maps:get(max_nodes, Info)),
                ?assertEqual(50, maps:get(priority, Info)),
                ?assertEqual(up, maps:get(state, Info))
            end}
       end,
       fun({_Pid, Name}) ->
           {"get_info with name",
            fun() ->
                {ok, Info} = flurm_partition:get_info(Name),
                ?assertEqual(Name, maps:get(name, Info))
            end}
       end
      ]}}.

api_set_state_test_() ->
    {"set_state API tests",
     {foreach,
      fun() ->
          Name = unique_partition_name(),
          Spec = #partition_spec{
              name = Name,
              nodes = [],
              max_time = 3600,
              default_time = 1800,
              max_nodes = 5,
              priority = 100
          },
          {ok, Pid} = flurm_partition:start_link(Spec),
          {Pid, Name}
      end,
      fun({Pid, _Name}) ->
          gen_server:stop(Pid)
      end,
      [
       fun({Pid, _Name}) ->
           {"set_state with pid",
            fun() ->
                ok = flurm_partition:set_state(Pid, down),
                {ok, State} = flurm_partition:get_state(Pid),
                ?assertEqual(down, State)
            end}
       end,
       fun({_Pid, Name}) ->
           {"set_state with name",
            fun() ->
                ok = flurm_partition:set_state(Name, drain),
                {ok, State} = flurm_partition:get_state(Name),
                ?assertEqual(drain, State)
            end}
       end
      ]}}.

api_get_state_test_() ->
    {"get_state API tests",
     {foreach,
      fun() ->
          Name = unique_partition_name(),
          Spec = #partition_spec{
              name = Name,
              nodes = [],
              max_time = 3600,
              default_time = 1800,
              max_nodes = 5,
              priority = 100
          },
          {ok, Pid} = flurm_partition:start_link(Spec),
          {Pid, Name}
      end,
      fun({Pid, _Name}) ->
          gen_server:stop(Pid)
      end,
      [
       fun({Pid, _Name}) ->
           {"get_state with pid",
            fun() ->
                {ok, State} = flurm_partition:get_state(Pid),
                ?assertEqual(up, State)
            end}
       end,
       fun({_Pid, Name}) ->
           {"get_state with name",
            fun() ->
                {ok, State} = flurm_partition:get_state(Name),
                ?assertEqual(up, State)
            end}
       end
      ]}}.
