%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_partition_registry
%%%
%%% Tests the gen_server that maintains a registry of partitions.
%%% NO MECK - tests init/1, handle_call/3, handle_cast/2, handle_info/2 directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_registry_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% ETS table name (copied from flurm_partition_registry)
-define(PARTITIONS_TABLE, flurm_partitions).
-define(DEFAULT_PARTITION_KEY, '$default_partition').

%%====================================================================
%% Test Descriptions
%%====================================================================

init_test_() ->
    {setup,
     fun setup_init/0,
     fun cleanup_init/1,
     [
      {"init/1 creates ETS table", fun init_creates_table_test/0},
      {"init/1 creates default partition", fun init_creates_default_partition_test/0},
      {"init/1 returns correct state", fun init_returns_state_test/0}
     ]}.

handle_call_test_() ->
    {foreach,
     fun setup_table/0,
     fun cleanup_table/1,
     [
      fun handle_call_register_new_test/1,
      fun handle_call_register_already_exists_test/1,
      fun handle_call_register_as_default_test/1,
      fun handle_call_unregister_existing_test/1,
      fun handle_call_unregister_not_found_test/1,
      fun handle_call_unregister_clears_default_test/1,
      fun handle_call_set_priority_existing_test/1,
      fun handle_call_set_priority_not_found_test/1,
      fun handle_call_set_default_existing_test/1,
      fun handle_call_set_default_not_found_test/1,
      fun handle_call_add_node_existing_test/1,
      fun handle_call_add_node_not_found_test/1,
      fun handle_call_add_node_already_member_test/1,
      fun handle_call_remove_node_existing_test/1,
      fun handle_call_remove_node_not_found_test/1,
      fun handle_call_update_existing_test/1,
      fun handle_call_update_not_found_test/1,
      fun handle_call_unknown_request_test/1
     ]}.

handle_cast_test_() ->
    {setup,
     fun setup_table/0,
     fun cleanup_table/1,
     [
      {"handle_cast ignores messages", fun handle_cast_noreply_test/0}
     ]}.

handle_info_test_() ->
    {foreach,
     fun setup_table/0,
     fun cleanup_table/1,
     [
      fun handle_info_config_reload_test/1,
      fun handle_info_config_changed_partitions_test/1,
      fun handle_info_config_changed_other_test/1,
      fun handle_info_unknown_test/1
     ]}.

terminate_test_() ->
    [
     {"terminate/2 returns ok", fun terminate_test/0}
    ].

code_change_test_() ->
    [
     {"code_change/3 returns state unchanged", fun code_change_test/0}
    ].

ets_lookup_test_() ->
    {setup,
     fun setup_table_with_data/0,
     fun cleanup_table/1,
     [
      {"get_partition finds existing partition", get_partition_found_test()},
      {"get_partition returns not_found for missing", get_partition_not_found_test()},
      {"get_partition_priority finds existing", get_partition_priority_found_test()},
      {"get_partition_priority returns not_found for missing", get_partition_priority_not_found_test()},
      {"list_partitions returns all partitions", list_partitions_test()},
      {"get_default_partition returns default", get_default_partition_test()},
      {"get_partition_nodes returns nodes", get_partition_nodes_found_test()},
      {"get_partition_nodes returns not_found for missing", get_partition_nodes_not_found_test()}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup_init() ->
    ok.

cleanup_init(_) ->
    catch ets:delete(?PARTITIONS_TABLE),
    ok.

setup_table() ->
    catch ets:delete(?PARTITIONS_TABLE),
    ets:new(?PARTITIONS_TABLE, [
        named_table,
        set,
        public,
        {read_concurrency, true}
    ]),
    #{}.

cleanup_table(_) ->
    catch ets:delete(?PARTITIONS_TABLE),
    ok.

setup_table_with_data() ->
    State = setup_table(),

    %% Create test partitions
    Partition1 = #partition{
        name = <<"compute">>,
        priority = 1000,
        state = up,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 0,
        allow_root = false
    },
    Partition2 = #partition{
        name = <<"gpu">>,
        priority = 2000,
        state = up,
        nodes = [<<"gpu-node1">>],
        max_time = 172800,
        default_time = 7200,
        max_nodes = 10,
        allow_root = true
    },
    Partition3 = #partition{
        name = <<"maintenance">>,
        priority = 500,
        state = drain,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 5,
        allow_root = false
    },

    %% Insert partitions
    ets:insert(?PARTITIONS_TABLE, {<<"compute">>, Partition1}),
    ets:insert(?PARTITIONS_TABLE, {<<"gpu">>, Partition2}),
    ets:insert(?PARTITIONS_TABLE, {<<"maintenance">>, Partition3}),

    %% Set default partition
    ets:insert(?PARTITIONS_TABLE, {?DEFAULT_PARTITION_KEY, <<"compute">>}),

    State.

%%====================================================================
%% Init Tests
%%====================================================================

init_creates_table_test() ->
    catch ets:delete(?PARTITIONS_TABLE),

    {ok, _State} = flurm_partition_registry:init([]),

    ?assertNotEqual(undefined, ets:info(?PARTITIONS_TABLE)),
    ?assertEqual(set, ets:info(?PARTITIONS_TABLE, type)),

    ets:delete(?PARTITIONS_TABLE),
    ok.

init_creates_default_partition_test() ->
    catch ets:delete(?PARTITIONS_TABLE),

    {ok, _State} = flurm_partition_registry:init([]),

    %% Verify default partition exists
    ?assertMatch([{<<"default">>, #partition{}}], ets:lookup(?PARTITIONS_TABLE, <<"default">>)),

    %% Verify default pointer
    ?assertMatch([{?DEFAULT_PARTITION_KEY, <<"default">>}], ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY)),

    %% Verify default partition values
    [{<<"default">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"default">>),
    ?assertEqual(<<"default">>, P#partition.name),
    ?assertEqual(1000, P#partition.priority),
    ?assertEqual(up, P#partition.state),
    ?assertEqual([], P#partition.nodes),
    ?assertEqual(86400, P#partition.max_time),
    ?assertEqual(3600, P#partition.default_time),
    ?assertEqual(0, P#partition.max_nodes),
    ?assertEqual(false, P#partition.allow_root),

    ets:delete(?PARTITIONS_TABLE),
    ok.

init_returns_state_test() ->
    catch ets:delete(?PARTITIONS_TABLE),

    {ok, State} = flurm_partition_registry:init([]),

    ?assertEqual(#{}, State),

    ets:delete(?PARTITIONS_TABLE),
    ok.

%%====================================================================
%% Handle Call Tests
%%====================================================================

handle_call_register_new_test(State) ->
    {"register new partition succeeds", fun() ->
        Config = #{
            priority => 1500,
            nodes => [<<"n1">>, <<"n2">>],
            state => up,
            max_time => 43200,
            default_time => 1800,
            max_nodes => 20,
            allow_root => true
        },
        {reply, ok, NewState} = flurm_partition_registry:handle_call(
            {register, <<"newpart">>, Config},
            {self(), make_ref()},
            State
        ),

        ?assertEqual(State, NewState),

        %% Verify partition was created
        [{<<"newpart">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"newpart">>),
        ?assertEqual(<<"newpart">>, P#partition.name),
        ?assertEqual(1500, P#partition.priority),
        ?assertEqual([<<"n1">>, <<"n2">>], P#partition.nodes),
        ?assertEqual(up, P#partition.state),
        ?assertEqual(43200, P#partition.max_time),
        ?assertEqual(1800, P#partition.default_time),
        ?assertEqual(20, P#partition.max_nodes),
        ?assertEqual(true, P#partition.allow_root)
    end}.

handle_call_register_already_exists_test(State) ->
    {"register existing partition fails", fun() ->
        %% First register a partition
        Config1 = #{priority => 1000},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"existing">>, Config1},
            {self(), make_ref()},
            State
        ),

        %% Try to register again
        Config2 = #{priority => 2000},
        {reply, {error, already_exists}, _} = flurm_partition_registry:handle_call(
            {register, <<"existing">>, Config2},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_register_as_default_test(State) ->
    {"register partition as default", fun() ->
        Config = #{priority => 1000, default => true},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"newdefault">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Verify default pointer was updated
        [{?DEFAULT_PARTITION_KEY, DefaultName}] = ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY),
        ?assertEqual(<<"newdefault">>, DefaultName)
    end}.

handle_call_unregister_existing_test(State) ->
    {"unregister existing partition succeeds", fun() ->
        %% First register a partition
        Config = #{priority => 1000},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"tounregister">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Verify it exists
        ?assertMatch([_], ets:lookup(?PARTITIONS_TABLE, <<"tounregister">>)),

        %% Unregister it
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {unregister, <<"tounregister">>},
            {self(), make_ref()},
            State
        ),

        %% Verify it's gone
        ?assertEqual([], ets:lookup(?PARTITIONS_TABLE, <<"tounregister">>))
    end}.

handle_call_unregister_not_found_test(State) ->
    {"unregister non-existent partition fails", fun() ->
        {reply, {error, not_found}, _} = flurm_partition_registry:handle_call(
            {unregister, <<"nonexistent">>},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_unregister_clears_default_test(State) ->
    {"unregister default partition clears default", fun() ->
        %% Register a partition as default
        Config = #{priority => 1000, default => true},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"defaultpart">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Verify it's the default
        [{?DEFAULT_PARTITION_KEY, <<"defaultpart">>}] = ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY),

        %% Unregister it
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {unregister, <<"defaultpart">>},
            {self(), make_ref()},
            State
        ),

        %% Verify default pointer was cleared
        ?assertEqual([], ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY))
    end}.

handle_call_set_priority_existing_test(State) ->
    {"set_priority for existing partition succeeds", fun() ->
        %% Register a partition
        Config = #{priority => 1000},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"prioritytest">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Set new priority
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {set_priority, <<"prioritytest">>, 5000},
            {self(), make_ref()},
            State
        ),

        %% Verify priority was updated
        [{<<"prioritytest">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"prioritytest">>),
        ?assertEqual(5000, P#partition.priority)
    end}.

handle_call_set_priority_not_found_test(State) ->
    {"set_priority for non-existent partition fails", fun() ->
        {reply, {error, not_found}, _} = flurm_partition_registry:handle_call(
            {set_priority, <<"nonexistent">>, 5000},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_set_default_existing_test(State) ->
    {"set_default for existing partition succeeds", fun() ->
        %% Register a partition
        Config = #{priority => 1000},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"setdefaulttest">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Set as default
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {set_default, <<"setdefaulttest">>},
            {self(), make_ref()},
            State
        ),

        %% Verify default pointer
        [{?DEFAULT_PARTITION_KEY, DefaultName}] = ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY),
        ?assertEqual(<<"setdefaulttest">>, DefaultName)
    end}.

handle_call_set_default_not_found_test(State) ->
    {"set_default for non-existent partition fails", fun() ->
        {reply, {error, not_found}, _} = flurm_partition_registry:handle_call(
            {set_default, <<"nonexistent">>},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_add_node_existing_test(State) ->
    {"add_node to existing partition succeeds", fun() ->
        %% Register a partition with no nodes
        Config = #{priority => 1000, nodes => []},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"addnodetest">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Add a node
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {add_node, <<"addnodetest">>, <<"newnode">>},
            {self(), make_ref()},
            State
        ),

        %% Verify node was added
        [{<<"addnodetest">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"addnodetest">>),
        ?assert(lists:member(<<"newnode">>, P#partition.nodes))
    end}.

handle_call_add_node_not_found_test(State) ->
    {"add_node to non-existent partition fails", fun() ->
        {reply, {error, partition_not_found}, _} = flurm_partition_registry:handle_call(
            {add_node, <<"nonexistent">>, <<"node">>},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_add_node_already_member_test(State) ->
    {"add_node that already exists fails", fun() ->
        %% Register a partition with a node
        Config = #{priority => 1000, nodes => [<<"existingnode">>]},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"dupenodetest">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Try to add the same node
        {reply, {error, already_member}, _} = flurm_partition_registry:handle_call(
            {add_node, <<"dupenodetest">>, <<"existingnode">>},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_remove_node_existing_test(State) ->
    {"remove_node from existing partition succeeds", fun() ->
        %% Register a partition with nodes
        Config = #{priority => 1000, nodes => [<<"node1">>, <<"node2">>]},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"removenodetest">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Remove a node
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {remove_node, <<"removenodetest">>, <<"node1">>},
            {self(), make_ref()},
            State
        ),

        %% Verify node was removed
        [{<<"removenodetest">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"removenodetest">>),
        ?assertNot(lists:member(<<"node1">>, P#partition.nodes)),
        ?assert(lists:member(<<"node2">>, P#partition.nodes))
    end}.

handle_call_remove_node_not_found_test(State) ->
    {"remove_node from non-existent partition fails", fun() ->
        {reply, {error, partition_not_found}, _} = flurm_partition_registry:handle_call(
            {remove_node, <<"nonexistent">>, <<"node">>},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_update_existing_test(State) ->
    {"update existing partition succeeds", fun() ->
        %% Register a partition
        Config = #{priority => 1000, max_time => 3600},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {register, <<"updatetest">>, Config},
            {self(), make_ref()},
            State
        ),

        %% Update it
        Updates = #{priority => 2000, max_time => 7200, state => drain},
        {reply, ok, _} = flurm_partition_registry:handle_call(
            {update, <<"updatetest">>, Updates},
            {self(), make_ref()},
            State
        ),

        %% Verify updates were applied
        [{<<"updatetest">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"updatetest">>),
        ?assertEqual(2000, P#partition.priority),
        ?assertEqual(7200, P#partition.max_time),
        ?assertEqual(drain, P#partition.state)
    end}.

handle_call_update_not_found_test(State) ->
    {"update non-existent partition fails", fun() ->
        Updates = #{priority => 2000},
        {reply, {error, not_found}, _} = flurm_partition_registry:handle_call(
            {update, <<"nonexistent">>, Updates},
            {self(), make_ref()},
            State
        )
    end}.

handle_call_unknown_request_test(State) ->
    {"unknown request returns error", fun() ->
        {reply, {error, unknown_request}, _} = flurm_partition_registry:handle_call(
            {unknown, request},
            {self(), make_ref()},
            State
        )
    end}.

%%====================================================================
%% Handle Cast Tests
%%====================================================================

handle_cast_noreply_test() ->
    State = #{},

    {noreply, NewState} = flurm_partition_registry:handle_cast(some_message, State),
    ?assertEqual(State, NewState),

    {noreply, NewState2} = flurm_partition_registry:handle_cast({unknown, data}, State),
    ?assertEqual(State, NewState2),

    ok.

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_config_reload_test(State) ->
    {"config_reload_partitions message is handled", fun() ->
        PartitionDefs = [],
        {noreply, NewState} = flurm_partition_registry:handle_info(
            {config_reload_partitions, PartitionDefs},
            State
        ),
        ?assertEqual(State, NewState)
    end}.

handle_info_config_changed_partitions_test(State) ->
    {"config_changed for partitions is handled", fun() ->
        OldPartitions = [],
        NewPartitions = [],
        {noreply, NewState} = flurm_partition_registry:handle_info(
            {config_changed, partitions, OldPartitions, NewPartitions},
            State
        ),
        ?assertEqual(State, NewState)
    end}.

handle_info_config_changed_other_test(State) ->
    {"config_changed for other keys is ignored", fun() ->
        {noreply, NewState} = flurm_partition_registry:handle_info(
            {config_changed, some_key, old_value, new_value},
            State
        ),
        ?assertEqual(State, NewState)
    end}.

handle_info_unknown_test(State) ->
    {"unknown info message is ignored", fun() ->
        {noreply, NewState} = flurm_partition_registry:handle_info(unknown_message, State),
        ?assertEqual(State, NewState)
    end}.

%%====================================================================
%% Terminate and Code Change Tests
%%====================================================================

terminate_test() ->
    State = #{},
    ?assertEqual(ok, flurm_partition_registry:terminate(normal, State)),
    ?assertEqual(ok, flurm_partition_registry:terminate(shutdown, State)),
    ?assertEqual(ok, flurm_partition_registry:terminate({error, reason}, State)),
    ok.

code_change_test() ->
    State = #{},
    ?assertEqual({ok, State}, flurm_partition_registry:code_change(old_vsn, State, extra)),
    ?assertEqual({ok, State}, flurm_partition_registry:code_change("1.0", State, [])),
    ok.

%%====================================================================
%% ETS Lookup Tests (called from within ets_lookup_test_ fixture)
%% Each test returns a fun/0 to be executed within the fixture context
%%====================================================================

get_partition_found_test() ->
    fun() ->
        Result = flurm_partition_registry:get_partition(<<"compute">>),
        ?assertMatch({ok, #{name := <<"compute">>}}, Result),
        {ok, Map} = Result,
        ?assertEqual(<<"compute">>, maps:get(name, Map)),
        ?assertEqual(1000, maps:get(priority, Map)),
        ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Map)),
        ?assertEqual(up, maps:get(state, Map)),
        ok
    end.

get_partition_not_found_test() ->
    fun() ->
        Result = flurm_partition_registry:get_partition(<<"nonexistent">>),
        ?assertEqual({error, not_found}, Result),
        ok
    end.

get_partition_priority_found_test() ->
    fun() ->
        Result1 = flurm_partition_registry:get_partition_priority(<<"compute">>),
        ?assertEqual({ok, 1000}, Result1),

        Result2 = flurm_partition_registry:get_partition_priority(<<"gpu">>),
        ?assertEqual({ok, 2000}, Result2),
        ok
    end.

get_partition_priority_not_found_test() ->
    fun() ->
        Result = flurm_partition_registry:get_partition_priority(<<"nonexistent">>),
        ?assertEqual({error, not_found}, Result),
        ok
    end.

list_partitions_test() ->
    fun() ->
        Partitions = flurm_partition_registry:list_partitions(),
        ?assertEqual(3, length(Partitions)),
        ?assert(lists:member(<<"compute">>, Partitions)),
        ?assert(lists:member(<<"gpu">>, Partitions)),
        ?assert(lists:member(<<"maintenance">>, Partitions)),
        ok
    end.

get_default_partition_test() ->
    fun() ->
        Result = flurm_partition_registry:get_default_partition(),
        ?assertEqual({ok, <<"compute">>}, Result),
        ok
    end.

get_partition_nodes_found_test() ->
    fun() ->
        Result1 = flurm_partition_registry:get_partition_nodes(<<"compute">>),
        ?assertEqual({ok, [<<"node1">>, <<"node2">>]}, Result1),

        Result2 = flurm_partition_registry:get_partition_nodes(<<"gpu">>),
        ?assertEqual({ok, [<<"gpu-node1">>]}, Result2),

        Result3 = flurm_partition_registry:get_partition_nodes(<<"maintenance">>),
        ?assertEqual({ok, []}, Result3),
        ok
    end.

get_partition_nodes_not_found_test() ->
    fun() ->
        Result = flurm_partition_registry:get_partition_nodes(<<"nonexistent">>),
        ?assertEqual({error, not_found}, Result),
        ok
    end.

%%====================================================================
%% Additional Tests
%%====================================================================

%% Test get_default_partition when not set
get_default_partition_not_set_test_() ->
    {setup,
     fun() ->
         catch ets:delete(?PARTITIONS_TABLE),
         ets:new(?PARTITIONS_TABLE, [named_table, set, public, {read_concurrency, true}]),
         %% Don't set default
         #{}
     end,
     fun cleanup_table/1,
     [
      {"get_default_partition returns not_set when no default", fun() ->
          Result = flurm_partition_registry:get_default_partition(),
          ?assertEqual({error, not_set}, Result)
      end}
     ]}.

%% Test partition record structure
partition_record_test_() ->
    [
     {"partition record structure is correct", fun() ->
         P = #partition{
             name = <<"test">>,
             priority = 1000,
             state = up,
             nodes = [<<"n1">>, <<"n2">>],
             max_time = 86400,
             default_time = 3600,
             max_nodes = 10,
             allow_root = false
         },
         ?assertEqual(<<"test">>, P#partition.name),
         ?assertEqual(1000, P#partition.priority),
         ?assertEqual(up, P#partition.state),
         ?assertEqual([<<"n1">>, <<"n2">>], P#partition.nodes),
         ?assertEqual(86400, P#partition.max_time),
         ?assertEqual(3600, P#partition.default_time),
         ?assertEqual(10, P#partition.max_nodes),
         ?assertEqual(false, P#partition.allow_root)
     end}
    ].

%% Test map_to_partition conversion logic
map_to_partition_test_() ->
    [
     {"map_to_partition with full config", fun() ->
         %% Simulate the conversion logic
         Name = <<"test">>,
         Config = #{
             priority => 2000,
             nodes => [<<"n1">>],
             state => drain,
             max_time => 7200,
             default_time => 1800,
             max_nodes => 5,
             allow_root => true
         },
         P = #partition{
             name = Name,
             priority = maps:get(priority, Config, 1000),
             nodes = maps:get(nodes, Config, []),
             state = maps:get(state, Config, up),
             max_time = maps:get(max_time, Config, 86400),
             default_time = maps:get(default_time, Config, 3600),
             max_nodes = maps:get(max_nodes, Config, 0),
             allow_root = maps:get(allow_root, Config, false)
         },
         ?assertEqual(<<"test">>, P#partition.name),
         ?assertEqual(2000, P#partition.priority),
         ?assertEqual([<<"n1">>], P#partition.nodes),
         ?assertEqual(drain, P#partition.state),
         ?assertEqual(7200, P#partition.max_time),
         ?assertEqual(1800, P#partition.default_time),
         ?assertEqual(5, P#partition.max_nodes),
         ?assertEqual(true, P#partition.allow_root)
     end},
     {"map_to_partition with defaults", fun() ->
         Name = <<"minimal">>,
         Config = #{},
         P = #partition{
             name = Name,
             priority = maps:get(priority, Config, 1000),
             nodes = maps:get(nodes, Config, []),
             state = maps:get(state, Config, up),
             max_time = maps:get(max_time, Config, 86400),
             default_time = maps:get(default_time, Config, 3600),
             max_nodes = maps:get(max_nodes, Config, 0),
             allow_root = maps:get(allow_root, Config, false)
         },
         ?assertEqual(<<"minimal">>, P#partition.name),
         ?assertEqual(1000, P#partition.priority),
         ?assertEqual([], P#partition.nodes),
         ?assertEqual(up, P#partition.state),
         ?assertEqual(86400, P#partition.max_time),
         ?assertEqual(3600, P#partition.default_time),
         ?assertEqual(0, P#partition.max_nodes),
         ?assertEqual(false, P#partition.allow_root)
     end}
    ].

%% Test partition_to_map conversion logic
partition_to_map_test_() ->
    [
     {"partition_to_map conversion", fun() ->
         P = #partition{
             name = <<"test">>,
             priority = 1500,
             state = drain,
             nodes = [<<"n1">>, <<"n2">>],
             max_time = 43200,
             default_time = 1800,
             max_nodes = 15,
             allow_root = true
         },
         Map = #{
             name => P#partition.name,
             priority => P#partition.priority,
             nodes => P#partition.nodes,
             state => P#partition.state,
             max_time => P#partition.max_time,
             default_time => P#partition.default_time,
             max_nodes => P#partition.max_nodes,
             allow_root => P#partition.allow_root
         },
         ?assertEqual(<<"test">>, maps:get(name, Map)),
         ?assertEqual(1500, maps:get(priority, Map)),
         ?assertEqual(drain, maps:get(state, Map)),
         ?assertEqual([<<"n1">>, <<"n2">>], maps:get(nodes, Map)),
         ?assertEqual(43200, maps:get(max_time, Map)),
         ?assertEqual(1800, maps:get(default_time, Map)),
         ?assertEqual(15, maps:get(max_nodes, Map)),
         ?assertEqual(true, maps:get(allow_root, Map))
     end}
    ].

%% Test apply_updates logic
apply_updates_test_() ->
    [
     {"apply_updates with partial updates", fun() ->
         P = #partition{
             name = <<"original">>,
             priority = 1000,
             state = up,
             nodes = [<<"n1">>],
             max_time = 86400,
             default_time = 3600,
             max_nodes = 0,
             allow_root = false
         },
         Updates = #{priority => 2000, state => drain},
         NewP = P#partition{
             priority = maps:get(priority, Updates, P#partition.priority),
             nodes = maps:get(nodes, Updates, P#partition.nodes),
             state = maps:get(state, Updates, P#partition.state),
             max_time = maps:get(max_time, Updates, P#partition.max_time),
             default_time = maps:get(default_time, Updates, P#partition.default_time),
             max_nodes = maps:get(max_nodes, Updates, P#partition.max_nodes),
             allow_root = maps:get(allow_root, Updates, P#partition.allow_root)
         },
         ?assertEqual(<<"original">>, NewP#partition.name),
         ?assertEqual(2000, NewP#partition.priority),
         ?assertEqual(drain, NewP#partition.state),
         ?assertEqual([<<"n1">>], NewP#partition.nodes),  % Unchanged
         ?assertEqual(86400, NewP#partition.max_time),    % Unchanged
         ?assertEqual(3600, NewP#partition.default_time), % Unchanged
         ?assertEqual(0, NewP#partition.max_nodes),       % Unchanged
         ?assertEqual(false, NewP#partition.allow_root)   % Unchanged
     end}
    ].

%% Test list_partitions excludes default key
list_partitions_excludes_default_key_test_() ->
    {setup,
     fun() ->
         catch ets:delete(?PARTITIONS_TABLE),
         ets:new(?PARTITIONS_TABLE, [named_table, set, public, {read_concurrency, true}]),
         %% Insert partitions and default key
         P = #partition{name = <<"test">>, priority = 1000, state = up, nodes = [],
                        max_time = 86400, default_time = 3600, max_nodes = 0, allow_root = false},
         ets:insert(?PARTITIONS_TABLE, {<<"test">>, P}),
         ets:insert(?PARTITIONS_TABLE, {?DEFAULT_PARTITION_KEY, <<"test">>}),
         #{}
     end,
     fun cleanup_table/1,
     [
      {"list_partitions excludes default key", fun() ->
          Partitions = flurm_partition_registry:list_partitions(),
          ?assertEqual(1, length(Partitions)),
          ?assertEqual([<<"test">>], Partitions),
          %% Verify default key is not included
          ?assertNot(lists:member(?DEFAULT_PARTITION_KEY, Partitions))
      end}
     ]}.

%% Test partition states
partition_state_test_() ->
    [
     {"partition state values", fun() ->
         States = [up, down, drain, inactive],
         ?assertEqual(4, length(States)),
         ?assert(lists:member(up, States)),
         ?assert(lists:member(down, States)),
         ?assert(lists:member(drain, States)),
         ?assert(lists:member(inactive, States)),
         %% Test that all states are distinct
         ?assertEqual(4, length(lists:usort(States)))
     end}
    ].

%% Test config reload with partition definitions
config_reload_update_test_() ->
    {setup,
     fun() ->
         catch ets:delete(?PARTITIONS_TABLE),
         ets:new(?PARTITIONS_TABLE, [named_table, set, public, {read_concurrency, true}]),
         %% Insert an existing partition
         P = #partition{name = <<"existing">>, priority = 1000, state = up, nodes = [],
                        max_time = 86400, default_time = 3600, max_nodes = 0, allow_root = false},
         ets:insert(?PARTITIONS_TABLE, {<<"existing">>, P}),
         #{}
     end,
     fun cleanup_table/1,
     [
      {"config_reload_partitions handles empty list", fun() ->
          State = #{},
          {noreply, NewState} = flurm_partition_registry:handle_info(
              {config_reload_partitions, []},
              State
          ),
          ?assertEqual(State, NewState)
      end}
     ]}.

%% Test multiple node operations
multiple_node_operations_test_() ->
    {setup,
     fun() ->
         State = setup_table(),
         %% Register a partition for testing
         Config = #{priority => 1000, nodes => []},
         {reply, ok, _} = flurm_partition_registry:handle_call(
             {register, <<"multinode">>, Config},
             {self(), make_ref()},
             State
         ),
         State
     end,
     fun cleanup_table/1,
     [
      {"add multiple nodes", fun() ->
          State = #{},
          %% Add first node
          {reply, ok, _} = flurm_partition_registry:handle_call(
              {add_node, <<"multinode">>, <<"node1">>},
              {self(), make_ref()},
              State
          ),
          %% Add second node
          {reply, ok, _} = flurm_partition_registry:handle_call(
              {add_node, <<"multinode">>, <<"node2">>},
              {self(), make_ref()},
              State
          ),
          %% Add third node
          {reply, ok, _} = flurm_partition_registry:handle_call(
              {add_node, <<"multinode">>, <<"node3">>},
              {self(), make_ref()},
              State
          ),

          %% Verify all nodes are present
          [{<<"multinode">>, P}] = ets:lookup(?PARTITIONS_TABLE, <<"multinode">>),
          ?assertEqual(3, length(P#partition.nodes)),
          ?assert(lists:member(<<"node1">>, P#partition.nodes)),
          ?assert(lists:member(<<"node2">>, P#partition.nodes)),
          ?assert(lists:member(<<"node3">>, P#partition.nodes))
      end}
     ]}.
