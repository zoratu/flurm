%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_partition gen_server callbacks
%%%
%%% Tests the callback functions directly without starting the process.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_callback_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

make_partition_spec() ->
    make_partition_spec(<<"batch">>).

make_partition_spec(Name) ->
    #partition_spec{
        name = Name,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100
    }.

make_partition_state() ->
    make_partition_state(<<"batch">>).

make_partition_state(Name) ->
    #partition_state{
        name = Name,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100,
        state = up
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_creates_state_from_spec_test() ->
    Spec = make_partition_spec(),
    {ok, State} = flurm_partition:init([Spec]),
    ?assertEqual(<<"batch">>, State#partition_state.name),
    ?assertEqual([<<"node1">>, <<"node2">>], State#partition_state.nodes),
    ?assertEqual(86400, State#partition_state.max_time),
    ?assertEqual(3600, State#partition_state.default_time),
    ?assertEqual(10, State#partition_state.max_nodes),
    ?assertEqual(100, State#partition_state.priority),
    ?assertEqual(up, State#partition_state.state).

init_with_empty_nodes_test() ->
    Spec = #partition_spec{
        name = <<"empty">>,
        nodes = [],
        max_time = 7200,
        default_time = 1800,
        max_nodes = 5,
        priority = 50
    },
    {ok, State} = flurm_partition:init([Spec]),
    ?assertEqual([], State#partition_state.nodes),
    ?assertEqual(5, State#partition_state.max_nodes).

%%====================================================================
%% handle_call/3 Tests - get_nodes
%%====================================================================

handle_call_get_nodes_test() ->
    State = make_partition_state(),
    {reply, {ok, Nodes}, NewState} = flurm_partition:handle_call(get_nodes, {self(), make_ref()}, State),
    ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
    ?assertEqual(State, NewState).

handle_call_get_nodes_empty_test() ->
    State = make_partition_state(),
    EmptyState = State#partition_state{nodes = []},
    {reply, {ok, Nodes}, _} = flurm_partition:handle_call(get_nodes, {self(), make_ref()}, EmptyState),
    ?assertEqual([], Nodes).

%%====================================================================
%% handle_call/3 Tests - add_node
%%====================================================================

handle_call_add_node_success_test() ->
    State = make_partition_state(),
    {reply, ok, NewState} = flurm_partition:handle_call({add_node, <<"node3">>}, {self(), make_ref()}, State),
    ?assert(lists:member(<<"node3">>, NewState#partition_state.nodes)),
    ?assertEqual(3, length(NewState#partition_state.nodes)).

handle_call_add_node_already_member_test() ->
    State = make_partition_state(),
    {reply, {error, already_member}, NewState} = flurm_partition:handle_call({add_node, <<"node1">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

handle_call_add_node_max_reached_test() ->
    State = make_partition_state(),
    %% Set max_nodes to current count so no more can be added
    FullState = State#partition_state{max_nodes = 2},
    {reply, {error, max_nodes_reached}, NewState} = flurm_partition:handle_call({add_node, <<"node3">>}, {self(), make_ref()}, FullState),
    ?assertEqual(FullState, NewState).

%%====================================================================
%% handle_call/3 Tests - remove_node
%%====================================================================

handle_call_remove_node_success_test() ->
    State = make_partition_state(),
    {reply, ok, NewState} = flurm_partition:handle_call({remove_node, <<"node1">>}, {self(), make_ref()}, State),
    ?assertNot(lists:member(<<"node1">>, NewState#partition_state.nodes)),
    ?assertEqual(1, length(NewState#partition_state.nodes)).

handle_call_remove_node_not_member_test() ->
    State = make_partition_state(),
    {reply, {error, not_member}, NewState} = flurm_partition:handle_call({remove_node, <<"nonexistent">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - get_info
%%====================================================================

handle_call_get_info_test() ->
    State = make_partition_state(),
    {reply, {ok, Info}, NewState} = flurm_partition:handle_call(get_info, {self(), make_ref()}, State),
    ?assertEqual(State, NewState),
    ?assertEqual(<<"batch">>, maps:get(name, Info)),
    ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Info)),
    ?assertEqual(2, maps:get(node_count, Info)),
    ?assertEqual(86400, maps:get(max_time, Info)),
    ?assertEqual(3600, maps:get(default_time, Info)),
    ?assertEqual(10, maps:get(max_nodes, Info)),
    ?assertEqual(100, maps:get(priority, Info)),
    ?assertEqual(up, maps:get(state, Info)).

%%====================================================================
%% handle_call/3 Tests - set_state
%%====================================================================

handle_call_set_state_up_test() ->
    State = make_partition_state(),
    DownState = State#partition_state{state = down},
    {reply, ok, NewState} = flurm_partition:handle_call({set_state, up}, {self(), make_ref()}, DownState),
    ?assertEqual(up, NewState#partition_state.state).

handle_call_set_state_down_test() ->
    State = make_partition_state(),
    {reply, ok, NewState} = flurm_partition:handle_call({set_state, down}, {self(), make_ref()}, State),
    ?assertEqual(down, NewState#partition_state.state).

handle_call_set_state_drain_test() ->
    State = make_partition_state(),
    {reply, ok, NewState} = flurm_partition:handle_call({set_state, drain}, {self(), make_ref()}, State),
    ?assertEqual(drain, NewState#partition_state.state).

handle_call_set_state_invalid_test() ->
    State = make_partition_state(),
    {reply, {error, invalid_state}, NewState} = flurm_partition:handle_call({set_state, bogus}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - get_state
%%====================================================================

handle_call_get_state_up_test() ->
    State = make_partition_state(),
    {reply, {ok, up}, NewState} = flurm_partition:handle_call(get_state, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

handle_call_get_state_down_test() ->
    State = make_partition_state(),
    DownState = State#partition_state{state = down},
    {reply, {ok, down}, _} = flurm_partition:handle_call(get_state, {self(), make_ref()}, DownState).

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

handle_call_unknown_request_test() ->
    State = make_partition_state(),
    {reply, {error, unknown_request}, NewState} = flurm_partition:handle_call({unknown, request}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_ignores_messages_test() ->
    State = make_partition_state(),
    {noreply, NewState} = flurm_partition:handle_cast(any_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_ignores_messages_test() ->
    State = make_partition_state(),
    {noreply, NewState} = flurm_partition:handle_info(any_info, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_returns_ok_test() ->
    State = make_partition_state(),
    ?assertEqual(ok, flurm_partition:terminate(normal, State)),
    ?assertEqual(ok, flurm_partition:terminate(shutdown, State)),
    ?assertEqual(ok, flurm_partition:terminate({error, reason}, State)).

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_returns_state_test() ->
    State = make_partition_state(),
    {ok, NewState} = flurm_partition:code_change(old_vsn, State, extra),
    ?assertEqual(State, NewState).

%%====================================================================
%% Internal Helper Tests
%%====================================================================

partition_name_converts_binary_to_atom_test() ->
    AtomName = flurm_partition:partition_name(<<"test">>),
    ?assertEqual(flurm_partition_test, AtomName).

partition_name_with_special_chars_test() ->
    AtomName = flurm_partition:partition_name(<<"gpu-high">>),
    ?assertEqual('flurm_partition_gpu-high', AtomName).

build_partition_info_test() ->
    State = make_partition_state(),
    Info = flurm_partition:build_partition_info(State),
    ?assert(is_map(Info)),
    ?assertEqual(<<"batch">>, maps:get(name, Info)),
    ?assertEqual(2, maps:get(node_count, Info)).

validate_partition_state_test() ->
    ?assert(flurm_partition:validate_partition_state(up)),
    ?assert(flurm_partition:validate_partition_state(down)),
    ?assert(flurm_partition:validate_partition_state(drain)),
    ?assertNot(flurm_partition:validate_partition_state(invalid)),
    ?assertNot(flurm_partition:validate_partition_state("up")),
    ?assertNot(flurm_partition:validate_partition_state(1)).
