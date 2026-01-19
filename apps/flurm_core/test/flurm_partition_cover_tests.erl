%%%-------------------------------------------------------------------
%%% @doc FLURM Partition Coverage Integration Tests
%%%
%%% Integration tests designed to call through public APIs to improve
%%% code coverage tracking. Cover only tracks calls from test modules
%%% to the actual module code, so these tests call public functions
%%% that exercise internal code paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

partition_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates partition process", fun test_start_link/0},
        {"get_nodes returns node list", fun test_get_nodes/0},
        {"add_node adds to partition", fun test_add_node/0},
        {"remove_node removes from partition", fun test_remove_node/0},
        {"get_info returns partition data", fun test_get_info/0},
        {"set_state changes partition state", fun test_set_state/0},
        {"get_state returns current state", fun test_get_state/0},
        {"partition name API variants", fun test_partition_name_variants/0},
        {"max_nodes limit enforcement", fun test_max_nodes_limit/0},
        {"invalid state rejected", fun test_invalid_state/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    %% Keep track of started partitions for cleanup
    #{partitions => []}.

cleanup(#{partitions := Partitions}) ->
    %% Stop all started partitions
    lists:foreach(fun(Name) ->
        ProcessName = list_to_atom("flurm_partition_" ++ binary_to_list(Name)),
        catch gen_server:stop(ProcessName, shutdown, 5000)
    end, Partitions),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_partition_spec(Name) ->
    make_partition_spec(Name, #{}).

make_partition_spec(Name, Overrides) ->
    Defaults = #{
        nodes => [],
        max_time => 86400,
        default_time => 3600,
        max_nodes => 100,
        priority => 100
    },
    Props = maps:merge(Defaults, Overrides),
    #partition_spec{
        name = Name,
        nodes = maps:get(nodes, Props),
        max_time = maps:get(max_time, Props),
        default_time = maps:get(default_time, Props),
        max_nodes = maps:get(max_nodes, Props),
        priority = maps:get(priority, Props)
    }.

%%====================================================================
%% Coverage Tests - Public API Calls
%%====================================================================

test_start_link() ->
    %% Test start_link directly
    PartitionSpec = make_partition_spec(<<"test_partition">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Verify get_info works
    {ok, Info} = flurm_partition:get_info(Pid),
    ?assertEqual(<<"test_partition">>, maps:get(name, Info)),
    gen_server:stop(Pid),
    ok.

test_get_nodes() ->
    PartitionSpec = make_partition_spec(<<"compute">>, #{
        nodes => [<<"node1">>, <<"node2">>, <<"node3">>]
    }),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assertEqual(3, length(Nodes)),
    ?assert(lists:member(<<"node1">>, Nodes)),
    ?assert(lists:member(<<"node2">>, Nodes)),
    ?assert(lists:member(<<"node3">>, Nodes)),
    gen_server:stop(Pid),
    ok.

test_add_node() ->
    PartitionSpec = make_partition_spec(<<"addtest">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    %% Initially empty
    {ok, []} = flurm_partition:get_nodes(Pid),
    %% Add nodes
    ok = flurm_partition:add_node(Pid, <<"node1">>),
    ok = flurm_partition:add_node(Pid, <<"node2">>),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"node1">>, Nodes)),
    ?assert(lists:member(<<"node2">>, Nodes)),
    %% Adding same node again fails
    {error, already_member} = flurm_partition:add_node(Pid, <<"node1">>),
    gen_server:stop(Pid),
    ok.

test_remove_node() ->
    PartitionSpec = make_partition_spec(<<"removetest">>, #{
        nodes => [<<"node1">>, <<"node2">>, <<"node3">>]
    }),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    %% Remove a node
    ok = flurm_partition:remove_node(Pid, <<"node2">>),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assertEqual(2, length(Nodes)),
    ?assertNot(lists:member(<<"node2">>, Nodes)),
    ?assert(lists:member(<<"node1">>, Nodes)),
    ?assert(lists:member(<<"node3">>, Nodes)),
    %% Removing non-member fails
    {error, not_member} = flurm_partition:remove_node(Pid, <<"node4">>),
    gen_server:stop(Pid),
    ok.

test_get_info() ->
    PartitionSpec = make_partition_spec(<<"infotest">>, #{
        nodes => [<<"node1">>],
        max_time => 172800,
        default_time => 7200,
        max_nodes => 50,
        priority => 200
    }),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    {ok, Info} = flurm_partition:get_info(Pid),
    %% Verify all fields
    ?assertEqual(<<"infotest">>, maps:get(name, Info)),
    ?assertEqual([<<"node1">>], maps:get(nodes, Info)),
    ?assertEqual(1, maps:get(node_count, Info)),
    ?assertEqual(172800, maps:get(max_time, Info)),
    ?assertEqual(7200, maps:get(default_time, Info)),
    ?assertEqual(50, maps:get(max_nodes, Info)),
    ?assertEqual(200, maps:get(priority, Info)),
    ?assertEqual(up, maps:get(state, Info)),
    gen_server:stop(Pid),
    ok.

test_set_state() ->
    PartitionSpec = make_partition_spec(<<"statetest">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    %% Initial state is up
    {ok, up} = flurm_partition:get_state(Pid),
    %% Set to down
    ok = flurm_partition:set_state(Pid, down),
    {ok, down} = flurm_partition:get_state(Pid),
    %% Set to drain
    ok = flurm_partition:set_state(Pid, drain),
    {ok, drain} = flurm_partition:get_state(Pid),
    %% Set back to up
    ok = flurm_partition:set_state(Pid, up),
    {ok, up} = flurm_partition:get_state(Pid),
    gen_server:stop(Pid),
    ok.

test_get_state() ->
    PartitionSpec = make_partition_spec(<<"getstate">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    {ok, up} = flurm_partition:get_state(Pid),
    ok = flurm_partition:set_state(Pid, drain),
    {ok, drain} = flurm_partition:get_state(Pid),
    gen_server:stop(Pid),
    ok.

test_partition_name_variants() ->
    PartitionSpec = make_partition_spec(<<"nametest">>),
    {ok, _Pid} = flurm_partition:start_link(PartitionSpec),
    %% Test API calls with partition name instead of PID
    {ok, Info} = flurm_partition:get_info(<<"nametest">>),
    ?assertEqual(<<"nametest">>, maps:get(name, Info)),
    {ok, up} = flurm_partition:get_state(<<"nametest">>),
    ok = flurm_partition:add_node(<<"nametest">>, <<"node1">>),
    {ok, Nodes} = flurm_partition:get_nodes(<<"nametest">>),
    ?assertEqual([<<"node1">>], Nodes),
    ok = flurm_partition:remove_node(<<"nametest">>, <<"node1">>),
    ok = flurm_partition:set_state(<<"nametest">>, drain),
    {ok, drain} = flurm_partition:get_state(<<"nametest">>),
    %% Cleanup
    ProcessName = flurm_partition_nametest,
    gen_server:stop(ProcessName),
    ok.

test_max_nodes_limit() ->
    PartitionSpec = make_partition_spec(<<"limittest">>, #{max_nodes => 3}),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    %% Add up to max
    ok = flurm_partition:add_node(Pid, <<"node1">>),
    ok = flurm_partition:add_node(Pid, <<"node2">>),
    ok = flurm_partition:add_node(Pid, <<"node3">>),
    %% Adding more fails
    {error, max_nodes_reached} = flurm_partition:add_node(Pid, <<"node4">>),
    {ok, Nodes} = flurm_partition:get_nodes(Pid),
    ?assertEqual(3, length(Nodes)),
    gen_server:stop(Pid),
    ok.

test_invalid_state() ->
    PartitionSpec = make_partition_spec(<<"invalidstate">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    %% Invalid state rejected - only valid states are up, down, drain
    %% Note: The API has a guard requiring is_atom(State), so we test
    %% invalid atom states that will be rejected by validate_partition_state
    {error, invalid_state} = flurm_partition:set_state(Pid, invalid_state),
    {error, invalid_state} = flurm_partition:set_state(Pid, running),
    {error, invalid_state} = flurm_partition:set_state(Pid, maint),
    %% State unchanged
    {ok, up} = flurm_partition:get_state(Pid),
    gen_server:stop(Pid),
    ok.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

partition_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast ignored", fun test_unknown_cast/0},
        {"unknown info ignored", fun test_unknown_info/0},
        {"node operations with full partition", fun test_full_partition_ops/0}
     ]}.

test_unknown_call() ->
    PartitionSpec = make_partition_spec(<<"unknowncall">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    Result = gen_server:call(Pid, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result),
    gen_server:stop(Pid),
    ok.

test_unknown_cast() ->
    PartitionSpec = make_partition_spec(<<"unknowncast">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    gen_server:cast(Pid, {unknown_cast, test}),
    timer:sleep(50),
    %% Verify partition still works
    {ok, _Info} = flurm_partition:get_info(Pid),
    gen_server:stop(Pid),
    ok.

test_unknown_info() ->
    PartitionSpec = make_partition_spec(<<"unknowninfo">>),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    Pid ! {unknown_info, test},
    timer:sleep(50),
    %% Verify partition still works
    {ok, _Info} = flurm_partition:get_info(Pid),
    gen_server:stop(Pid),
    ok.

test_full_partition_ops() ->
    PartitionSpec = make_partition_spec(<<"fullops">>, #{
        nodes => [<<"n1">>, <<"n2">>, <<"n3">>],
        max_nodes => 5
    }),
    {ok, Pid} = flurm_partition:start_link(PartitionSpec),
    %% Verify initial state
    {ok, Info1} = flurm_partition:get_info(Pid),
    ?assertEqual(3, maps:get(node_count, Info1)),
    %% Add more nodes
    ok = flurm_partition:add_node(Pid, <<"n4">>),
    ok = flurm_partition:add_node(Pid, <<"n5">>),
    {ok, Info2} = flurm_partition:get_info(Pid),
    ?assertEqual(5, maps:get(node_count, Info2)),
    %% At limit
    {error, max_nodes_reached} = flurm_partition:add_node(Pid, <<"n6">>),
    %% Remove some
    ok = flurm_partition:remove_node(Pid, <<"n2">>),
    ok = flurm_partition:remove_node(Pid, <<"n4">>),
    {ok, Info3} = flurm_partition:get_info(Pid),
    ?assertEqual(3, maps:get(node_count, Info3)),
    %% Can add again
    ok = flurm_partition:add_node(Pid, <<"n6">>),
    gen_server:stop(Pid),
    ok.
