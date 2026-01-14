%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_partition_manager
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_manager_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

partition_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates default partition", fun test_init/0},
      {"create_partition adds new partition", fun test_create_partition/0},
      {"create_partition rejects duplicate", fun test_create_partition_duplicate/0},
      {"update_partition modifies partition", fun test_update_partition/0},
      {"update_partition returns error for unknown", fun test_update_partition_not_found/0},
      {"delete_partition removes partition", fun test_delete_partition/0},
      {"delete_partition rejects default", fun test_delete_partition_default/0},
      {"delete_partition returns error for unknown", fun test_delete_partition_not_found/0},
      {"get_partition returns partition", fun test_get_partition/0},
      {"get_partition returns error for unknown", fun test_get_partition_not_found/0},
      {"list_partitions returns all partitions", fun test_list_partitions/0},
      {"add_node adds node to partition", fun test_add_node/0},
      {"add_node returns error for unknown partition", fun test_add_node_not_found/0},
      {"remove_node removes node from partition", fun test_remove_node/0},
      {"remove_node returns error for unknown partition", fun test_remove_node_not_found/0},
      {"handle_call unknown returns error", fun test_unknown_call/0},
      {"handle_cast ignores messages", fun test_handle_cast/0},
      {"handle_info ignores messages", fun test_handle_info/0},
      {"terminate returns ok", fun test_terminate/0}
     ]}.

setup() ->
    meck:new(flurm_core, [passthrough, non_strict]),
    meck:expect(flurm_core, new_partition, fun(Spec) ->
        #partition{
            name = maps:get(name, Spec, <<"unknown">>),
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
    meck:expect(flurm_core, add_node_to_partition, fun(#partition{nodes = Nodes} = P, Node) ->
        P#partition{nodes = [Node | Nodes]}
    end),
    ok.

cleanup(_) ->
    meck:unload(flurm_core),
    ok.

%% State record matching the module's internal state
-record(state, {
    partitions = #{} :: #{binary() => #partition{}}
}).

%%====================================================================
%% Test Cases
%%====================================================================

test_init() ->
    {ok, State} = flurm_partition_manager:init([]),
    ?assertMatch(#state{}, State),
    Partitions = State#state.partitions,
    ?assert(maps:is_key(<<"default">>, Partitions)).

test_create_partition() ->
    {ok, State0} = flurm_partition_manager:init([]),
    PartitionSpec = #{name => <<"compute">>, state => up},
    {reply, Result, State1} = flurm_partition_manager:handle_call(
        {create_partition, PartitionSpec}, {self(), make_ref()}, State0),
    ?assertEqual(ok, Result),
    ?assert(maps:is_key(<<"compute">>, State1#state.partitions)).

test_create_partition_duplicate() ->
    {ok, State0} = flurm_partition_manager:init([]),
    PartitionSpec = #{name => <<"default">>, state => up},
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {create_partition, PartitionSpec}, {self(), make_ref()}, State0),
    ?assertEqual({error, already_exists}, Result).

test_update_partition() ->
    {ok, State0} = flurm_partition_manager:init([]),
    Updates = #{state => down, priority => 50},
    {reply, Result, State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"default">>, Updates}, {self(), make_ref()}, State0),
    ?assertEqual(ok, Result),
    #{<<"default">> := UpdatedPartition} = State1#state.partitions,
    ?assertEqual(down, UpdatedPartition#partition.state),
    ?assertEqual(50, UpdatedPartition#partition.priority).

test_update_partition_not_found() ->
    {ok, State0} = flurm_partition_manager:init([]),
    Updates = #{state => down},
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"nonexistent">>, Updates}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_delete_partition() ->
    {ok, State0} = flurm_partition_manager:init([]),
    %% First create a partition
    PartitionSpec = #{name => <<"compute">>, state => up},
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {create_partition, PartitionSpec}, {self(), make_ref()}, State0),
    %% Then delete it
    {reply, Result, State2} = flurm_partition_manager:handle_call(
        {delete_partition, <<"compute">>}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    ?assertEqual(false, maps:is_key(<<"compute">>, State2#state.partitions)).

test_delete_partition_default() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {delete_partition, <<"default">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, cannot_delete_default}, Result).

test_delete_partition_not_found() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {delete_partition, <<"nonexistent">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_get_partition() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {get_partition, <<"default">>}, {self(), make_ref()}, State0),
    ?assertMatch({ok, #partition{name = <<"default">>}}, Result).

test_get_partition_not_found() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {get_partition, <<"nonexistent">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_list_partitions() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        list_partitions, {self(), make_ref()}, State0),
    ?assert(is_list(Result)),
    ?assertEqual(1, length(Result)),
    [Partition] = Result,
    ?assertEqual(<<"default">>, Partition#partition.name).

test_add_node() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, State1} = flurm_partition_manager:handle_call(
        {add_node, <<"default">>, <<"node1">>}, {self(), make_ref()}, State0),
    ?assertEqual(ok, Result),
    #{<<"default">> := UpdatedPartition} = State1#state.partitions,
    ?assert(lists:member(<<"node1">>, UpdatedPartition#partition.nodes)).

test_add_node_not_found() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {add_node, <<"nonexistent">>, <<"node1">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, partition_not_found}, Result).

test_remove_node() ->
    {ok, State0} = flurm_partition_manager:init([]),
    %% First add a node
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {add_node, <<"default">>, <<"node1">>}, {self(), make_ref()}, State0),
    %% Then remove it
    {reply, Result, State2} = flurm_partition_manager:handle_call(
        {remove_node, <<"default">>, <<"node1">>}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{<<"default">> := UpdatedPartition} = State2#state.partitions,
    ?assertEqual(false, lists:member(<<"node1">>, UpdatedPartition#partition.nodes)).

test_remove_node_not_found() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        {remove_node, <<"nonexistent">>, <<"node1">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, partition_not_found}, Result).

test_unknown_call() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, Result, _State1} = flurm_partition_manager:handle_call(
        unknown_request, {self(), make_ref()}, State0),
    ?assertEqual({error, unknown_request}, Result).

test_handle_cast() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {noreply, State1} = flurm_partition_manager:handle_cast(some_message, State0),
    ?assertEqual(State0, State1).

test_handle_info() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {noreply, State1} = flurm_partition_manager:handle_info(some_message, State0),
    ?assertEqual(State0, State1).

test_terminate() ->
    {ok, State0} = flurm_partition_manager:init([]),
    Result = flurm_partition_manager:terminate(normal, State0),
    ?assertEqual(ok, Result).

%%====================================================================
%% API Tests with Real Server
%%====================================================================

api_test_() ->
    {setup,
     fun setup_api/0,
     fun cleanup_api/1,
     [
      {"API create_partition", fun test_api_create_partition/0},
      {"API update_partition", fun test_api_update_partition/0},
      {"API delete_partition", fun test_api_delete_partition/0},
      {"API get_partition", fun test_api_get_partition/0},
      {"API list_partitions", fun test_api_list_partitions/0},
      {"API add_node", fun test_api_add_node/0},
      {"API remove_node", fun test_api_remove_node/0}
     ]}.

setup_api() ->
    meck:new(flurm_core, [passthrough, non_strict]),
    meck:expect(flurm_core, new_partition, fun(Spec) ->
        #partition{
            name = maps:get(name, Spec, <<"unknown">>),
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
    meck:expect(flurm_core, add_node_to_partition, fun(#partition{nodes = Nodes} = P, Node) ->
        P#partition{nodes = [Node | Nodes]}
    end),

    {ok, Pid} = flurm_partition_manager:start_link(),
    Pid.

cleanup_api(Pid) ->
    gen_server:stop(Pid),
    meck:unload(flurm_core),
    ok.

test_api_create_partition() ->
    Result = flurm_partition_manager:create_partition(#{name => <<"api_test">>, state => up}),
    ?assertEqual(ok, Result).

test_api_update_partition() ->
    Result = flurm_partition_manager:update_partition(<<"default">>, #{priority => 200}),
    ?assertEqual(ok, Result).

test_api_delete_partition() ->
    flurm_partition_manager:create_partition(#{name => <<"to_delete">>, state => up}),
    Result = flurm_partition_manager:delete_partition(<<"to_delete">>),
    ?assertEqual(ok, Result).

test_api_get_partition() ->
    Result = flurm_partition_manager:get_partition(<<"default">>),
    ?assertMatch({ok, #partition{}}, Result).

test_api_list_partitions() ->
    Result = flurm_partition_manager:list_partitions(),
    ?assert(is_list(Result)),
    ?assert(length(Result) >= 1).

test_api_add_node() ->
    Result = flurm_partition_manager:add_node(<<"default">>, <<"api_node">>),
    ?assertEqual(ok, Result).

test_api_remove_node() ->
    flurm_partition_manager:add_node(<<"default">>, <<"temp_node">>),
    Result = flurm_partition_manager:remove_node(<<"default">>, <<"temp_node">>),
    ?assertEqual(ok, Result).

%%====================================================================
%% Update Field Tests
%%====================================================================

update_fields_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"update max_time", fun test_update_max_time/0},
      {"update default_time", fun test_update_default_time/0},
      {"update max_nodes", fun test_update_max_nodes/0},
      {"update allow_root", fun test_update_allow_root/0},
      {"update unknown field is ignored", fun test_update_unknown_field/0}
     ]}.

test_update_max_time() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"default">>, #{max_time => 172800}}, {self(), make_ref()}, State0),
    #{<<"default">> := P} = State1#state.partitions,
    ?assertEqual(172800, P#partition.max_time).

test_update_default_time() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"default">>, #{default_time => 7200}}, {self(), make_ref()}, State0),
    #{<<"default">> := P} = State1#state.partitions,
    ?assertEqual(7200, P#partition.default_time).

test_update_max_nodes() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"default">>, #{max_nodes => 500}}, {self(), make_ref()}, State0),
    #{<<"default">> := P} = State1#state.partitions,
    ?assertEqual(500, P#partition.max_nodes).

test_update_allow_root() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"default">>, #{allow_root => true}}, {self(), make_ref()}, State0),
    #{<<"default">> := P} = State1#state.partitions,
    ?assertEqual(true, P#partition.allow_root).

test_update_unknown_field() ->
    {ok, State0} = flurm_partition_manager:init([]),
    {reply, ok, State1} = flurm_partition_manager:handle_call(
        {update_partition, <<"default">>, #{unknown_field => value}}, {self(), make_ref()}, State0),
    %% Should complete without error
    ?assertMatch(#state{}, State1).
