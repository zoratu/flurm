%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_partition_manager gen_server callbacks
%%%
%%% Tests the callback functions directly without starting the process.
%%% Uses meck to mock flurm_core and lager dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_manager_callback_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% State record copied from module (internal)
-record(state, {
    partitions = #{} :: #{binary() => #partition{}}
}).

make_partition(Name) ->
    #partition{
        name = Name,
        state = up,
        nodes = [],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 1000,
        priority = 100,
        allow_root = false
    }.

make_state() ->
    DefaultPartition = make_partition(<<"default">>),
    #state{partitions = #{<<"default">> => DefaultPartition}}.

make_state_with_partitions(Partitions) ->
    PartitionMap = maps:from_list([{P#partition.name, P} || P <- Partitions]),
    #state{partitions = PartitionMap}.

setup() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:new(flurm_core, [passthrough, no_link]),
    meck:expect(flurm_core, new_partition, fun(Spec) ->
        #partition{
            name = maps:get(name, Spec, <<"unnamed">>),
            state = maps:get(state, Spec, up),
            nodes = maps:get(nodes, Spec, []),
            max_time = maps:get(max_time, Spec, 86400),
            default_time = maps:get(default_time, Spec, 3600),
            max_nodes = maps:get(max_nodes, Spec, 1000),
            priority = maps:get(priority, Spec, 100),
            allow_root = maps:get(allow_root, Spec, false)
        }
    end),
    meck:expect(flurm_core, partition_name, fun(#partition{name = Name}) -> Name end),
    meck:expect(flurm_core, add_node_to_partition, fun(#partition{nodes = Nodes} = P, NodeName) ->
        P#partition{nodes = [NodeName | Nodes]}
    end),
    ok.

cleanup(_) ->
    meck:unload(lager),
    meck:unload(flurm_core),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

partition_manager_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates default partition", fun init_creates_default_partition/0},
      {"create_partition success", fun handle_call_create_partition_success/0},
      {"create_partition already exists", fun handle_call_create_partition_already_exists/0},
      {"update_partition success", fun handle_call_update_partition_success/0},
      {"update_partition not found", fun handle_call_update_partition_not_found/0},
      {"delete_partition success", fun handle_call_delete_partition_success/0},
      {"delete_partition cannot delete default", fun handle_call_delete_partition_cannot_delete_default/0},
      {"delete_partition not found", fun handle_call_delete_partition_not_found/0},
      {"get_partition success", fun handle_call_get_partition_success/0},
      {"get_partition not found", fun handle_call_get_partition_not_found/0},
      {"list_partitions", fun handle_call_list_partitions/0},
      {"add_node success", fun handle_call_add_node_success/0},
      {"add_node partition not found", fun handle_call_add_node_partition_not_found/0},
      {"remove_node success", fun handle_call_remove_node_success/0},
      {"remove_node partition not found", fun handle_call_remove_node_partition_not_found/0},
      {"unknown request", fun handle_call_unknown_request/0},
      {"handle_cast ignores messages", fun handle_cast_ignores_messages/0},
      {"handle_info ignores messages", fun handle_info_ignores_messages/0},
      {"terminate returns ok", fun terminate_returns_ok/0},
      {"apply_partition_updates", fun apply_partition_updates_test/0}
     ]}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_creates_default_partition() ->
    {ok, State} = flurm_partition_manager:init([]),
    ?assert(maps:is_key(<<"default">>, State#state.partitions)),
    DefaultP = maps:get(<<"default">>, State#state.partitions),
    ?assertEqual(<<"default">>, DefaultP#partition.name).

%%====================================================================
%% handle_call/3 Tests - create_partition
%%====================================================================

handle_call_create_partition_success() ->
    State = make_state(),
    PartitionSpec = #{name => <<"batch">>, max_time => 7200},
    {reply, ok, NewState} = flurm_partition_manager:handle_call({create_partition, PartitionSpec}, {self(), make_ref()}, State),
    ?assert(maps:is_key(<<"batch">>, NewState#state.partitions)).

handle_call_create_partition_already_exists() ->
    State = make_state(),
    PartitionSpec = #{name => <<"default">>},
    {reply, {error, already_exists}, NewState} = flurm_partition_manager:handle_call({create_partition, PartitionSpec}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - update_partition
%%====================================================================

handle_call_update_partition_success() ->
    State = make_state(),
    Updates = #{max_time => 43200, priority => 200},
    {reply, ok, NewState} = flurm_partition_manager:handle_call({update_partition, <<"default">>, Updates}, {self(), make_ref()}, State),
    UpdatedP = maps:get(<<"default">>, NewState#state.partitions),
    ?assertEqual(43200, UpdatedP#partition.max_time),
    ?assertEqual(200, UpdatedP#partition.priority).

handle_call_update_partition_not_found() ->
    State = make_state(),
    Updates = #{max_time => 43200},
    {reply, {error, not_found}, NewState} = flurm_partition_manager:handle_call({update_partition, <<"nonexistent">>, Updates}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - delete_partition
%%====================================================================

handle_call_delete_partition_success() ->
    BatchPartition = make_partition(<<"batch">>),
    State = make_state_with_partitions([make_partition(<<"default">>), BatchPartition]),
    {reply, ok, NewState} = flurm_partition_manager:handle_call({delete_partition, <<"batch">>}, {self(), make_ref()}, State),
    ?assertNot(maps:is_key(<<"batch">>, NewState#state.partitions)),
    ?assert(maps:is_key(<<"default">>, NewState#state.partitions)).

handle_call_delete_partition_cannot_delete_default() ->
    State = make_state(),
    {reply, {error, cannot_delete_default}, NewState} = flurm_partition_manager:handle_call({delete_partition, <<"default">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

handle_call_delete_partition_not_found() ->
    State = make_state(),
    {reply, {error, not_found}, NewState} = flurm_partition_manager:handle_call({delete_partition, <<"nonexistent">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - get_partition
%%====================================================================

handle_call_get_partition_success() ->
    State = make_state(),
    {reply, {ok, Partition}, NewState} = flurm_partition_manager:handle_call({get_partition, <<"default">>}, {self(), make_ref()}, State),
    ?assertEqual(<<"default">>, Partition#partition.name),
    ?assertEqual(State, NewState).

handle_call_get_partition_not_found() ->
    State = make_state(),
    {reply, {error, not_found}, NewState} = flurm_partition_manager:handle_call({get_partition, <<"nonexistent">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - list_partitions
%%====================================================================

handle_call_list_partitions() ->
    BatchPartition = make_partition(<<"batch">>),
    State = make_state_with_partitions([make_partition(<<"default">>), BatchPartition]),
    {reply, Partitions, NewState} = flurm_partition_manager:handle_call(list_partitions, {self(), make_ref()}, State),
    ?assertEqual(2, length(Partitions)),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - add_node
%%====================================================================

handle_call_add_node_success() ->
    State = make_state(),
    {reply, ok, NewState} = flurm_partition_manager:handle_call({add_node, <<"default">>, <<"node1">>}, {self(), make_ref()}, State),
    UpdatedP = maps:get(<<"default">>, NewState#state.partitions),
    ?assert(lists:member(<<"node1">>, UpdatedP#partition.nodes)).

handle_call_add_node_partition_not_found() ->
    State = make_state(),
    {reply, {error, partition_not_found}, NewState} = flurm_partition_manager:handle_call({add_node, <<"nonexistent">>, <<"node1">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - remove_node
%%====================================================================

handle_call_remove_node_success() ->
    DefaultP = (make_partition(<<"default">>))#partition{nodes = [<<"node1">>, <<"node2">>]},
    State = #state{partitions = #{<<"default">> => DefaultP}},
    {reply, ok, NewState} = flurm_partition_manager:handle_call({remove_node, <<"default">>, <<"node1">>}, {self(), make_ref()}, State),
    UpdatedP = maps:get(<<"default">>, NewState#state.partitions),
    ?assertNot(lists:member(<<"node1">>, UpdatedP#partition.nodes)),
    ?assert(lists:member(<<"node2">>, UpdatedP#partition.nodes)).

handle_call_remove_node_partition_not_found() ->
    State = make_state(),
    {reply, {error, partition_not_found}, NewState} = flurm_partition_manager:handle_call({remove_node, <<"nonexistent">>, <<"node1">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

handle_call_unknown_request() ->
    State = make_state(),
    {reply, {error, unknown_request}, NewState} = flurm_partition_manager:handle_call({unknown, request}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_ignores_messages() ->
    State = make_state(),
    {noreply, NewState} = flurm_partition_manager:handle_cast(any_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_ignores_messages() ->
    State = make_state(),
    {noreply, NewState} = flurm_partition_manager:handle_info(any_info, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_returns_ok() ->
    State = make_state(),
    ?assertEqual(ok, flurm_partition_manager:terminate(normal, State)),
    ?assertEqual(ok, flurm_partition_manager:terminate(shutdown, State)).

%%====================================================================
%% Internal Helper Tests
%%====================================================================

apply_partition_updates_test() ->
    Partition = make_partition(<<"test">>),
    Updates = #{
        state => down,
        max_time => 7200,
        default_time => 1800,
        max_nodes => 500,
        priority => 50,
        allow_root => true,
        unknown_field => ignored
    },
    Updated = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(down, Updated#partition.state),
    ?assertEqual(7200, Updated#partition.max_time),
    ?assertEqual(1800, Updated#partition.default_time),
    ?assertEqual(500, Updated#partition.max_nodes),
    ?assertEqual(50, Updated#partition.priority),
    ?assertEqual(true, Updated#partition.allow_root),
    %% Name should not change
    ?assertEqual(<<"test">>, Updated#partition.name).
