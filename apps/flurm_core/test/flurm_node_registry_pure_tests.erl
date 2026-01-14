%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_node_registry
%%%
%%% Tests the gen_server that maintains a registry of compute nodes.
%%% NO MECK - tests init/1, handle_call/3, handle_cast/2, handle_info/2 directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_registry_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Internal state record (copied from flurm_node_registry)
-record(state, {
    monitors = #{} :: #{reference() => binary()}
}).

%% ETS table names (copied from flurm_node_registry)
-define(NODES_BY_NAME, flurm_nodes_by_name).
-define(NODES_BY_STATE, flurm_nodes_by_state).
-define(NODES_BY_PARTITION, flurm_nodes_by_partition).

%%====================================================================
%% Test Descriptions
%%====================================================================

init_test_() ->
    {setup,
     fun setup_init/0,
     fun cleanup_init/1,
     [
      {"init/1 creates ETS tables", fun init_creates_tables_test/0},
      {"init/1 returns correct state", fun init_returns_state_test/0}
     ]}.

handle_call_test_() ->
    {foreach,
     fun setup_tables/0,
     fun cleanup_tables/1,
     [
      fun handle_call_unregister_not_found_test/1,
      fun handle_call_update_state_not_found_test/1,
      fun handle_call_update_entry_not_found_test/1,
      fun handle_call_unknown_request_test/1
     ]}.

handle_cast_test_() ->
    {setup,
     fun setup_tables/0,
     fun cleanup_tables/1,
     [
      {"handle_cast ignores messages", fun handle_cast_noreply_test/0}
     ]}.

handle_info_test_() ->
    {foreach,
     fun setup_tables/0,
     fun cleanup_tables/1,
     [
      fun handle_info_down_unknown_test/1,
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
     fun setup_tables_with_data/0,
     fun cleanup_tables/1,
     [
      {"lookup_node finds existing node", lookup_node_found_test()},
      {"lookup_node returns not_found for missing", lookup_node_not_found_test()},
      {"get_node_entry returns full entry", get_node_entry_found_test()},
      {"get_node_entry returns not_found for missing", get_node_entry_not_found_test()},
      {"list_nodes returns all nodes", list_nodes_test()},
      {"list_nodes_by_state returns filtered nodes", list_nodes_by_state_test()},
      {"list_nodes_by_partition returns filtered nodes", list_nodes_by_partition_test()},
      {"get_available_nodes filters by resources", get_available_nodes_test()},
      {"count_by_state returns counts", count_by_state_test()},
      {"count_nodes returns total", count_nodes_test()},
      {"list_all_nodes is alias for list_nodes", list_all_nodes_test()}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup_init() ->
    ok.

cleanup_init(_) ->
    %% Clean up any tables created
    catch ets:delete(?NODES_BY_NAME),
    catch ets:delete(?NODES_BY_STATE),
    catch ets:delete(?NODES_BY_PARTITION),
    ok.

setup_tables() ->
    %% Create ETS tables like init/1 does
    catch ets:delete(?NODES_BY_NAME),
    catch ets:delete(?NODES_BY_STATE),
    catch ets:delete(?NODES_BY_PARTITION),

    ets:new(?NODES_BY_NAME, [
        named_table,
        set,
        public,
        {keypos, #node_entry.name},
        {read_concurrency, true}
    ]),
    ets:new(?NODES_BY_STATE, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    ets:new(?NODES_BY_PARTITION, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    #state{monitors = #{}}.

cleanup_tables(_) ->
    catch ets:delete(?NODES_BY_NAME),
    catch ets:delete(?NODES_BY_STATE),
    catch ets:delete(?NODES_BY_PARTITION),
    ok.

setup_tables_with_data() ->
    State = setup_tables(),

    %% Insert test data
    Node1 = #node_entry{
        name = <<"node1">>,
        pid = list_to_pid("<0.100.0>"),
        hostname = <<"node1.example.com">>,
        state = up,
        partitions = [<<"compute">>, <<"batch">>],
        cpus_total = 64,
        cpus_avail = 32,
        memory_total = 128000,
        memory_avail = 64000,
        gpus_total = 4,
        gpus_avail = 2
    },
    Node2 = #node_entry{
        name = <<"node2">>,
        pid = list_to_pid("<0.101.0>"),
        hostname = <<"node2.example.com">>,
        state = up,
        partitions = [<<"gpu">>],
        cpus_total = 128,
        cpus_avail = 64,
        memory_total = 256000,
        memory_avail = 128000,
        gpus_total = 8,
        gpus_avail = 4
    },
    Node3 = #node_entry{
        name = <<"node3">>,
        pid = list_to_pid("<0.102.0>"),
        hostname = <<"node3.example.com">>,
        state = down,
        partitions = [<<"compute">>],
        cpus_total = 64,
        cpus_avail = 0,
        memory_total = 128000,
        memory_avail = 0,
        gpus_total = 0,
        gpus_avail = 0
    },

    %% Insert into primary table
    ets:insert(?NODES_BY_NAME, Node1),
    ets:insert(?NODES_BY_NAME, Node2),
    ets:insert(?NODES_BY_NAME, Node3),

    %% Insert into state index
    ets:insert(?NODES_BY_STATE, {up, <<"node1">>, Node1#node_entry.pid}),
    ets:insert(?NODES_BY_STATE, {up, <<"node2">>, Node2#node_entry.pid}),
    ets:insert(?NODES_BY_STATE, {down, <<"node3">>, Node3#node_entry.pid}),

    %% Insert into partition indexes
    ets:insert(?NODES_BY_PARTITION, {<<"compute">>, <<"node1">>, Node1#node_entry.pid}),
    ets:insert(?NODES_BY_PARTITION, {<<"batch">>, <<"node1">>, Node1#node_entry.pid}),
    ets:insert(?NODES_BY_PARTITION, {<<"gpu">>, <<"node2">>, Node2#node_entry.pid}),
    ets:insert(?NODES_BY_PARTITION, {<<"compute">>, <<"node3">>, Node3#node_entry.pid}),

    State.

%%====================================================================
%% Init Tests
%%====================================================================

init_creates_tables_test() ->
    %% Clean up any existing tables first
    catch ets:delete(?NODES_BY_NAME),
    catch ets:delete(?NODES_BY_STATE),
    catch ets:delete(?NODES_BY_PARTITION),

    %% Call init
    {ok, _State} = flurm_node_registry:init([]),

    %% Verify tables exist
    ?assertNotEqual(undefined, ets:info(?NODES_BY_NAME)),
    ?assertNotEqual(undefined, ets:info(?NODES_BY_STATE)),
    ?assertNotEqual(undefined, ets:info(?NODES_BY_PARTITION)),

    %% Verify table properties
    ?assertEqual(set, ets:info(?NODES_BY_NAME, type)),
    ?assertEqual(bag, ets:info(?NODES_BY_STATE, type)),
    ?assertEqual(bag, ets:info(?NODES_BY_PARTITION, type)),

    %% Clean up
    ets:delete(?NODES_BY_NAME),
    ets:delete(?NODES_BY_STATE),
    ets:delete(?NODES_BY_PARTITION),

    ok.

init_returns_state_test() ->
    %% Clean up any existing tables first
    catch ets:delete(?NODES_BY_NAME),
    catch ets:delete(?NODES_BY_STATE),
    catch ets:delete(?NODES_BY_PARTITION),

    %% Call init
    {ok, State} = flurm_node_registry:init([]),

    %% Verify state structure
    ?assertMatch(#state{monitors = #{}}, State),

    %% Clean up
    ets:delete(?NODES_BY_NAME),
    ets:delete(?NODES_BY_STATE),
    ets:delete(?NODES_BY_PARTITION),

    ok.

%%====================================================================
%% Handle Call Tests
%%====================================================================

handle_call_unregister_not_found_test(State) ->
    {"unregister non-existent node returns ok", fun() ->
        %% Unregistering non-existent node should still return ok
        {reply, ok, NewState} = flurm_node_registry:handle_call(
            {unregister, <<"nonexistent">>},
            {self(), make_ref()},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_call_update_state_not_found_test(State) ->
    {"update_state for non-existent node returns error", fun() ->
        {reply, {error, not_found}, NewState} = flurm_node_registry:handle_call(
            {update_state, <<"nonexistent">>, up},
            {self(), make_ref()},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_call_update_entry_not_found_test(State) ->
    {"update_entry for non-existent node returns error", fun() ->
        Entry = #node_entry{
            name = <<"nonexistent">>,
            pid = self(),
            hostname = <<"nonexistent">>,
            state = up,
            partitions = [],
            cpus_total = 0,
            cpus_avail = 0,
            memory_total = 0,
            memory_avail = 0,
            gpus_total = 0,
            gpus_avail = 0
        },
        {reply, {error, not_found}, NewState} = flurm_node_registry:handle_call(
            {update_entry, <<"nonexistent">>, Entry},
            {self(), make_ref()},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_call_unknown_request_test(State) ->
    {"unknown request returns error", fun() ->
        {reply, {error, unknown_request}, NewState} = flurm_node_registry:handle_call(
            {unknown_request, some_data},
            {self(), make_ref()},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

%%====================================================================
%% Handle Cast Tests
%%====================================================================

handle_cast_noreply_test() ->
    State = #state{monitors = #{}},

    %% Any cast should be ignored
    {noreply, NewState} = flurm_node_registry:handle_cast(some_message, State),
    ?assertEqual(State, NewState),

    {noreply, NewState2} = flurm_node_registry:handle_cast({unknown, data}, State),
    ?assertEqual(State, NewState2),

    ok.

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_down_unknown_test(State) ->
    {"DOWN from unknown monitor is ignored", fun() ->
        MonRef = make_ref(),
        {noreply, NewState} = flurm_node_registry:handle_info(
            {'DOWN', MonRef, process, self(), normal},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_info_config_reload_test(State) ->
    {"config_reload_nodes message is handled", fun() ->
        NodeDefs = [],
        {noreply, NewState} = flurm_node_registry:handle_info(
            {config_reload_nodes, NodeDefs},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_info_config_changed_partitions_test(State) ->
    {"config_changed for nodes is handled", fun() ->
        OldNodes = [],
        NewNodes = [],
        {noreply, NewState} = flurm_node_registry:handle_info(
            {config_changed, nodes, OldNodes, NewNodes},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_info_config_changed_other_test(State) ->
    {"config_changed for other keys is ignored", fun() ->
        {noreply, NewState} = flurm_node_registry:handle_info(
            {config_changed, some_key, old_value, new_value},
            State
        ),
        ?assertMatch(#state{}, NewState)
    end}.

handle_info_unknown_test(State) ->
    {"unknown info message is ignored", fun() ->
        {noreply, NewState} = flurm_node_registry:handle_info(unknown_message, State),
        ?assertMatch(#state{}, NewState)
    end}.

%%====================================================================
%% Terminate and Code Change Tests
%%====================================================================

terminate_test() ->
    State = #state{monitors = #{}},
    ?assertEqual(ok, flurm_node_registry:terminate(normal, State)),
    ?assertEqual(ok, flurm_node_registry:terminate(shutdown, State)),
    ?assertEqual(ok, flurm_node_registry:terminate({error, reason}, State)),
    ok.

code_change_test() ->
    State = #state{monitors = #{}},
    ?assertEqual({ok, State}, flurm_node_registry:code_change(old_vsn, State, extra)),
    ?assertEqual({ok, State}, flurm_node_registry:code_change("1.0", State, [])),
    ok.

%%====================================================================
%% ETS Lookup Tests (these are called from within ets_lookup_test_ fixture)
%% Each test is wrapped as an instantiator that receives the fixture result
%%====================================================================

lookup_node_found_test() ->
    fun() ->
        %% Node exists
        Result = flurm_node_registry:lookup_node(<<"node1">>),
        ?assertMatch({ok, _Pid}, Result),
        {ok, Pid} = Result,
        ?assert(is_pid(Pid)),
        ok
    end.

lookup_node_not_found_test() ->
    fun() ->
        Result = flurm_node_registry:lookup_node(<<"nonexistent">>),
        ?assertEqual({error, not_found}, Result),
        ok
    end.

get_node_entry_found_test() ->
    fun() ->
        Result = flurm_node_registry:get_node_entry(<<"node1">>),
        ?assertMatch({ok, #node_entry{}}, Result),
        {ok, Entry} = Result,
        ?assertEqual(<<"node1">>, Entry#node_entry.name),
        ?assertEqual(up, Entry#node_entry.state),
        ok
    end.

get_node_entry_not_found_test() ->
    fun() ->
        Result = flurm_node_registry:get_node_entry(<<"nonexistent">>),
        ?assertEqual({error, not_found}, Result),
        ok
    end.

list_nodes_test() ->
    fun() ->
        Nodes = flurm_node_registry:list_nodes(),
        ?assertEqual(3, length(Nodes)),
        Names = [Name || {Name, _Pid} <- Nodes],
        ?assert(lists:member(<<"node1">>, Names)),
        ?assert(lists:member(<<"node2">>, Names)),
        ?assert(lists:member(<<"node3">>, Names)),
        ok
    end.

list_nodes_by_state_test() ->
    fun() ->
        %% Up nodes
        UpNodes = flurm_node_registry:list_nodes_by_state(up),
        ?assertEqual(2, length(UpNodes)),
        UpNames = [Name || {Name, _Pid} <- UpNodes],
        ?assert(lists:member(<<"node1">>, UpNames)),
        ?assert(lists:member(<<"node2">>, UpNames)),

        %% Down nodes
        DownNodes = flurm_node_registry:list_nodes_by_state(down),
        ?assertEqual(1, length(DownNodes)),
        [{DownName, _}] = DownNodes,
        ?assertEqual(<<"node3">>, DownName),

        %% Drain nodes (none)
        DrainNodes = flurm_node_registry:list_nodes_by_state(drain),
        ?assertEqual(0, length(DrainNodes)),

        ok
    end.

list_nodes_by_partition_test() ->
    fun() ->
        %% Compute partition
        ComputeNodes = flurm_node_registry:list_nodes_by_partition(<<"compute">>),
        ?assertEqual(2, length(ComputeNodes)),
        ComputeNames = [Name || {Name, _Pid} <- ComputeNodes],
        ?assert(lists:member(<<"node1">>, ComputeNames)),
        ?assert(lists:member(<<"node3">>, ComputeNames)),

        %% GPU partition
        GpuNodes = flurm_node_registry:list_nodes_by_partition(<<"gpu">>),
        ?assertEqual(1, length(GpuNodes)),

        %% Batch partition
        BatchNodes = flurm_node_registry:list_nodes_by_partition(<<"batch">>),
        ?assertEqual(1, length(BatchNodes)),

        %% Non-existent partition
        NoNodes = flurm_node_registry:list_nodes_by_partition(<<"nonexistent">>),
        ?assertEqual(0, length(NoNodes)),

        ok
    end.

get_available_nodes_test() ->
    fun() ->
        %% Get nodes with at least 32 CPUs, 64000 MB memory, 2 GPUs
        Available = flurm_node_registry:get_available_nodes({32, 64000, 2}),

        %% Only node1 and node2 are up, and only node2 has 4 GPUs available
        %% node1 has cpus_avail=32, memory_avail=64000, gpus_avail=2 - should match
        %% node2 has cpus_avail=64, memory_avail=128000, gpus_avail=4 - should match
        ?assertEqual(2, length(Available)),

        %% More restrictive filter
        Available2 = flurm_node_registry:get_available_nodes({64, 100000, 4}),
        ?assertEqual(1, length(Available2)),
        [Entry] = Available2,
        ?assertEqual(<<"node2">>, Entry#node_entry.name),

        %% Filter that no node can satisfy
        Available3 = flurm_node_registry:get_available_nodes({1000, 0, 0}),
        ?assertEqual(0, length(Available3)),

        ok
    end.

count_by_state_test() ->
    fun() ->
        Counts = flurm_node_registry:count_by_state(),
        ?assertEqual(2, maps:get(up, Counts)),
        ?assertEqual(1, maps:get(down, Counts)),
        ?assertEqual(0, maps:get(drain, Counts)),
        ?assertEqual(0, maps:get(maint, Counts)),
        ok
    end.

count_nodes_test() ->
    fun() ->
        Count = flurm_node_registry:count_nodes(),
        ?assertEqual(3, Count),
        ok
    end.

list_all_nodes_test() ->
    fun() ->
        %% list_all_nodes should return same as list_nodes
        AllNodes = flurm_node_registry:list_all_nodes(),
        Nodes = flurm_node_registry:list_nodes(),
        ?assertEqual(length(Nodes), length(AllNodes)),
        ok
    end.

%%====================================================================
%% Additional Tests
%%====================================================================

%% Test update_state handle_call with existing node
update_state_existing_test_() ->
    {setup,
     fun() ->
         State = setup_tables(),
         %% Add a node to update
         Entry = #node_entry{
             name = <<"testnode">>,
             pid = self(),
             hostname = <<"testnode">>,
             state = up,
             partitions = [],
             cpus_total = 8,
             cpus_avail = 8,
             memory_total = 16000,
             memory_avail = 16000,
             gpus_total = 0,
             gpus_avail = 0
         },
         ets:insert(?NODES_BY_NAME, Entry),
         ets:insert(?NODES_BY_STATE, {up, <<"testnode">>, self()}),
         State
     end,
     fun cleanup_tables/1,
     [
      {"update_state changes node state", fun() ->
          State = #state{monitors = #{}},
          {reply, ok, _NewState} = flurm_node_registry:handle_call(
              {update_state, <<"testnode">>, drain},
              {self(), make_ref()},
              State
          ),

          %% Verify state was updated
          [UpdatedEntry] = ets:lookup(?NODES_BY_NAME, <<"testnode">>),
          ?assertEqual(drain, UpdatedEntry#node_entry.state),

          %% Verify state index was updated
          OldStateEntries = ets:lookup(?NODES_BY_STATE, up),
          ?assertNot(lists:keymember(<<"testnode">>, 2, OldStateEntries)),

          NewStateEntries = ets:lookup(?NODES_BY_STATE, drain),
          ?assert(lists:keymember(<<"testnode">>, 2, NewStateEntries))
      end}
     ]}.

%% Test update_entry handle_call with existing node
update_entry_existing_test_() ->
    {setup,
     fun() ->
         State = setup_tables(),
         %% Add a node to update
         Entry = #node_entry{
             name = <<"updatenode">>,
             pid = self(),
             hostname = <<"updatenode">>,
             state = up,
             partitions = [<<"compute">>],
             cpus_total = 8,
             cpus_avail = 8,
             memory_total = 16000,
             memory_avail = 16000,
             gpus_total = 0,
             gpus_avail = 0
         },
         ets:insert(?NODES_BY_NAME, Entry),
         ets:insert(?NODES_BY_STATE, {up, <<"updatenode">>, self()}),
         ets:insert(?NODES_BY_PARTITION, {<<"compute">>, <<"updatenode">>, self()}),
         State
     end,
     fun cleanup_tables/1,
     [
      {"update_entry updates node entry", fun() ->
          State = #state{monitors = #{}},
          NewEntry = #node_entry{
              name = <<"updatenode">>,
              pid = self(),
              hostname = <<"updatenode.new">>,
              state = up,  % Same state
              partitions = [<<"compute">>],
              cpus_total = 16,  % Changed
              cpus_avail = 8,   % Changed
              memory_total = 32000,  % Changed
              memory_avail = 16000,  % Changed
              gpus_total = 2,  % Changed
              gpus_avail = 2   % Changed
          },

          {reply, ok, _NewState} = flurm_node_registry:handle_call(
              {update_entry, <<"updatenode">>, NewEntry},
              {self(), make_ref()},
              State
          ),

          %% Verify entry was updated
          [UpdatedEntry] = ets:lookup(?NODES_BY_NAME, <<"updatenode">>),
          ?assertEqual(<<"updatenode.new">>, UpdatedEntry#node_entry.hostname),
          ?assertEqual(16, UpdatedEntry#node_entry.cpus_total),
          ?assertEqual(32000, UpdatedEntry#node_entry.memory_total),
          ?assertEqual(2, UpdatedEntry#node_entry.gpus_total)
      end},
      {"update_entry with state change updates index", fun() ->
          State = #state{monitors = #{}},

          %% First set state back to up
          ets:insert(?NODES_BY_NAME, #node_entry{
              name = <<"updatenode">>,
              pid = self(),
              hostname = <<"updatenode">>,
              state = up,
              partitions = [<<"compute">>],
              cpus_total = 8,
              cpus_avail = 8,
              memory_total = 16000,
              memory_avail = 16000,
              gpus_total = 0,
              gpus_avail = 0
          }),

          NewEntry = #node_entry{
              name = <<"updatenode">>,
              pid = self(),
              hostname = <<"updatenode">>,
              state = maint,  % Changed state
              partitions = [<<"compute">>],
              cpus_total = 8,
              cpus_avail = 0,
              memory_total = 16000,
              memory_avail = 0,
              gpus_total = 0,
              gpus_avail = 0
          },

          {reply, ok, _NewState} = flurm_node_registry:handle_call(
              {update_entry, <<"updatenode">>, NewEntry},
              {self(), make_ref()},
              State
          ),

          %% Verify state index was updated
          [UpdatedEntry] = ets:lookup(?NODES_BY_NAME, <<"updatenode">>),
          ?assertEqual(maint, UpdatedEntry#node_entry.state)
      end}
     ]}.

%% Test unregister with existing node
unregister_existing_test_() ->
    {setup,
     fun() ->
         State = setup_tables(),
         %% Add a node to unregister
         Entry = #node_entry{
             name = <<"removenode">>,
             pid = self(),
             hostname = <<"removenode">>,
             state = up,
             partitions = [<<"compute">>, <<"batch">>],
             cpus_total = 8,
             cpus_avail = 8,
             memory_total = 16000,
             memory_avail = 16000,
             gpus_total = 0,
             gpus_avail = 0
         },
         ets:insert(?NODES_BY_NAME, Entry),
         ets:insert(?NODES_BY_STATE, {up, <<"removenode">>, self()}),
         ets:insert(?NODES_BY_PARTITION, {<<"compute">>, <<"removenode">>, self()}),
         ets:insert(?NODES_BY_PARTITION, {<<"batch">>, <<"removenode">>, self()}),
         State
     end,
     fun cleanup_tables/1,
     [
      {"unregister removes node from all tables", fun() ->
          State = #state{monitors = #{}},

          {reply, ok, _NewState} = flurm_node_registry:handle_call(
              {unregister, <<"removenode">>},
              {self(), make_ref()},
              State
          ),

          %% Verify removed from primary table
          ?assertEqual([], ets:lookup(?NODES_BY_NAME, <<"removenode">>)),

          %% Verify removed from state index
          StateEntries = ets:lookup(?NODES_BY_STATE, up),
          ?assertNot(lists:keymember(<<"removenode">>, 2, StateEntries)),

          %% Verify removed from partition indexes
          ComputeEntries = ets:lookup(?NODES_BY_PARTITION, <<"compute">>),
          ?assertNot(lists:keymember(<<"removenode">>, 2, ComputeEntries)),

          BatchEntries = ets:lookup(?NODES_BY_PARTITION, <<"batch">>),
          ?assertNot(lists:keymember(<<"removenode">>, 2, BatchEntries))
      end}
     ]}.

%% Test DOWN message with known monitor
down_known_monitor_test_() ->
    {setup,
     fun() ->
         State = setup_tables(),
         %% Add a node that will be removed
         Entry = #node_entry{
             name = <<"downnode">>,
             pid = self(),
             hostname = <<"downnode">>,
             state = up,
             partitions = [<<"compute">>],
             cpus_total = 8,
             cpus_avail = 8,
             memory_total = 16000,
             memory_avail = 16000,
             gpus_total = 0,
             gpus_avail = 0
         },
         ets:insert(?NODES_BY_NAME, Entry),
         ets:insert(?NODES_BY_STATE, {up, <<"downnode">>, self()}),
         ets:insert(?NODES_BY_PARTITION, {<<"compute">>, <<"downnode">>, self()}),

         %% Create state with monitor reference
         MonRef = make_ref(),
         #state{monitors = #{MonRef => <<"downnode">>}}
     end,
     fun cleanup_tables/1,
     [
      {"DOWN message removes node", fun() ->
          MonRef = make_ref(),
          State = #state{monitors = #{MonRef => <<"downnode">>}},

          {noreply, NewState} = flurm_node_registry:handle_info(
              {'DOWN', MonRef, process, self(), normal},
              State
          ),

          %% Verify monitor was removed from state
          ?assertNot(maps:is_key(MonRef, NewState#state.monitors)),

          %% Verify node was removed from tables
          ?assertEqual([], ets:lookup(?NODES_BY_NAME, <<"downnode">>))
      end}
     ]}.

%% Test internal helper functions behavior
find_monitor_ref_test_() ->
    [
     {"find_monitor_ref finds existing ref", fun() ->
         Monitors = #{make_ref() => <<"node1">>, make_ref() => <<"node2">>},
         %% This tests the logic used in find_monitor_ref
         Result = maps:filter(fun(_, V) -> V =:= <<"node1">> end, Monitors),
         ?assertEqual(1, maps:size(Result))
     end},
     {"find_monitor_ref returns empty for missing", fun() ->
         Monitors = #{make_ref() => <<"node1">>},
         Result = maps:filter(fun(_, V) -> V =:= <<"nonexistent">> end, Monitors),
         ?assertEqual(0, maps:size(Result))
     end}
    ].

%% Test record structures
node_entry_record_test_() ->
    [
     {"node_entry record structure is correct", fun() ->
         Entry = #node_entry{
             name = <<"test">>,
             pid = self(),
             hostname = <<"test.local">>,
             state = up,
             partitions = [<<"p1">>, <<"p2">>],
             cpus_total = 16,
             cpus_avail = 8,
             memory_total = 32000,
             memory_avail = 16000,
             gpus_total = 2,
             gpus_avail = 1
         },
         ?assertEqual(<<"test">>, Entry#node_entry.name),
         ?assert(is_pid(Entry#node_entry.pid)),
         ?assertEqual(<<"test.local">>, Entry#node_entry.hostname),
         ?assertEqual(up, Entry#node_entry.state),
         ?assertEqual([<<"p1">>, <<"p2">>], Entry#node_entry.partitions),
         ?assertEqual(16, Entry#node_entry.cpus_total),
         ?assertEqual(8, Entry#node_entry.cpus_avail),
         ?assertEqual(32000, Entry#node_entry.memory_total),
         ?assertEqual(16000, Entry#node_entry.memory_avail),
         ?assertEqual(2, Entry#node_entry.gpus_total),
         ?assertEqual(1, Entry#node_entry.gpus_avail)
     end}
    ].

%% Test state record
state_record_test_() ->
    [
     {"state record structure is correct", fun() ->
         Ref1 = make_ref(),
         Ref2 = make_ref(),
         State = #state{monitors = #{Ref1 => <<"node1">>, Ref2 => <<"node2">>}},
         ?assertEqual(2, maps:size(State#state.monitors)),
         ?assertEqual(<<"node1">>, maps:get(Ref1, State#state.monitors)),
         ?assertEqual(<<"node2">>, maps:get(Ref2, State#state.monitors))
     end}
    ].
