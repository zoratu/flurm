%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_dbd_storage
%%%
%%% Tests gen_server callbacks directly without mocking.
%%% NO MECK USAGE - all tests are pure unit tests.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_storage_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup fixture for tests
setup() ->
    %% Suppress lager output during tests
    application:load(lager),
    application:set_env(lager, handlers, []),
    application:set_env(lager, crash_log, false),
    %% Ensure ets backend is used
    application:load(flurm_dbd),
    application:set_env(flurm_dbd, storage_backend, ets),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"init with ets backend creates all tables",
        fun() ->
            application:set_env(flurm_dbd, storage_backend, ets),
            {ok, State} = flurm_dbd_storage:init([]),
            %% Verify state has backend and tables
            ?assertEqual(ets, element(2, State)),  % backend
            Tables = element(3, State),            % tables map
            ?assert(is_map(Tables)),
            %% Verify all required tables exist
            ?assert(maps:is_key(jobs, Tables)),
            ?assert(maps:is_key(steps, Tables)),
            ?assert(maps:is_key(events, Tables)),
            ?assert(maps:is_key(accounts, Tables)),
            ?assert(maps:is_key(users, Tables)),
            ?assert(maps:is_key(associations, Tables)),
            ?assert(maps:is_key(qos, Tables)),
            ?assert(maps:is_key(usage, Tables)),
            %% Verify tables are ETS references
            ?assert(is_reference(maps:get(jobs, Tables))),
            ?assert(is_reference(maps:get(steps, Tables))),
            ?assert(is_reference(maps:get(events, Tables))),
            ?assert(is_reference(maps:get(accounts, Tables))),
            ?assert(is_reference(maps:get(users, Tables))),
            ?assert(is_reference(maps:get(associations, Tables))),
            ?assert(is_reference(maps:get(qos, Tables))),
            ?assert(is_reference(maps:get(usage, Tables))),
            cleanup_state(State)
        end},

       {"init defaults to ets backend when not configured",
        fun() ->
            application:unset_env(flurm_dbd, storage_backend),
            {ok, State} = flurm_dbd_storage:init([]),
            ?assertEqual(ets, element(2, State)),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Store
%%====================================================================

handle_call_store_test_() ->
    {"handle_call store tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"store inserts value into known table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, NewState} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"test_job">>}},
                {self(), make_ref()}, State),
            ?assertEqual(ok, Result),
            cleanup_state(NewState)
        end},

       {"store returns error for unknown table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {store, unknown_table, 1, #{data => value}},
                {self(), make_ref()}, State),
            ?assertEqual({error, unknown_table}, Result),
            cleanup_state(State)
        end},

       {"store overwrites existing value",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store initial value
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"original">>}},
                {self(), make_ref()}, State),
            %% Overwrite with new value
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"updated">>}},
                {self(), make_ref()}, State2),
            %% Verify new value
            {reply, {ok, Value}, _NewState} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State3),
            ?assertEqual(#{name => <<"updated">>}, Value),
            cleanup_state(State3)
        end},

       {"store works with different value types",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store map
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, key1, #{data => value}},
                {self(), make_ref()}, State),
            %% Store list
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, accounts, key2, [a, b, c]},
                {self(), make_ref()}, State2),
            %% Store tuple
            {reply, ok, State4} = flurm_dbd_storage:handle_call(
                {store, users, key3, {user, <<"alice">>, 1000}},
                {self(), make_ref()}, State3),
            %% Store binary
            {reply, ok, State5} = flurm_dbd_storage:handle_call(
                {store, qos, key4, <<"high">>},
                {self(), make_ref()}, State4),
            %% Store integer
            {reply, ok, State6} = flurm_dbd_storage:handle_call(
                {store, usage, key5, 12345},
                {self(), make_ref()}, State5),
            cleanup_state(State6)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Fetch
%%====================================================================

handle_call_fetch_test_() ->
    {"handle_call fetch tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"fetch returns value for existing key",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store a value
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"test">>}},
                {self(), make_ref()}, State),
            %% Fetch it
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            ?assertEqual({ok, #{name => <<"test">>}}, Result),
            cleanup_state(State2)
        end},

       {"fetch returns not_found for missing key",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 999}, {self(), make_ref()}, State),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(State)
        end},

       {"fetch returns error for unknown table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {fetch, unknown_table, 1}, {self(), make_ref()}, State),
            ?assertEqual({error, unknown_table}, Result),
            cleanup_state(State)
        end},

       {"fetch works with different key types",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Integer key
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, value1}, {self(), make_ref()}, State),
            {reply, {ok, value1}, _} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            %% Binary key
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, accounts, <<"acct1">>, value2}, {self(), make_ref()}, State2),
            {reply, {ok, value2}, _} = flurm_dbd_storage:handle_call(
                {fetch, accounts, <<"acct1">>}, {self(), make_ref()}, State3),
            %% Tuple key
            {reply, ok, State4} = flurm_dbd_storage:handle_call(
                {store, usage, {user, <<"alice">>}, value3}, {self(), make_ref()}, State3),
            {reply, {ok, value3}, _} = flurm_dbd_storage:handle_call(
                {fetch, usage, {user, <<"alice">>}}, {self(), make_ref()}, State4),
            cleanup_state(State4)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Delete
%%====================================================================

handle_call_delete_test_() ->
    {"handle_call delete tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"delete removes existing key",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store a value
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"test">>}},
                {self(), make_ref()}, State),
            %% Delete it
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {delete, jobs, 1}, {self(), make_ref()}, State2),
            %% Verify it's gone
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State3),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(State3)
        end},

       {"delete returns ok for non-existing key",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {delete, jobs, 999}, {self(), make_ref()}, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end},

       {"delete returns ok for unknown table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {delete, unknown_table, 1}, {self(), make_ref()}, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - List
%%====================================================================

handle_call_list_test_() ->
    {"handle_call list tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"list returns empty list for empty table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {list, jobs}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"list returns all values in table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store multiple values
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"job1">>}},
                {self(), make_ref()}, State),
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, jobs, 2, #{name => <<"job2">>}},
                {self(), make_ref()}, State2),
            {reply, ok, State4} = flurm_dbd_storage:handle_call(
                {store, jobs, 3, #{name => <<"job3">>}},
                {self(), make_ref()}, State3),
            %% List all
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {list, jobs}, {self(), make_ref()}, State4),
            ?assertEqual(3, length(Result)),
            ?assert(lists:member(#{name => <<"job1">>}, Result)),
            ?assert(lists:member(#{name => <<"job2">>}, Result)),
            ?assert(lists:member(#{name => <<"job3">>}, Result)),
            cleanup_state(State4)
        end},

       {"list returns empty list for unknown table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {list, unknown_table}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"list with pattern returns matching values",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store multiple values with integer keys
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, job1_value},
                {self(), make_ref()}, State),
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, jobs, 2, job2_value},
                {self(), make_ref()}, State2),
            %% List with pattern matching key 1
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {list, jobs, {1, '_'}}, {self(), make_ref()}, State3),
            ?assertEqual(1, length(Result)),
            ?assert(lists:member(job1_value, Result)),
            cleanup_state(State3)
        end},

       {"list with pattern returns empty for no matches",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, value1},
                {self(), make_ref()}, State),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {list, jobs, {999, '_'}}, {self(), make_ref()}, State2),
            ?assertEqual([], Result),
            cleanup_state(State2)
        end},

       {"list with pattern returns empty for unknown table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {list, unknown_table, {'_', '_'}}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Sync
%%====================================================================

handle_call_sync_test_() ->
    {"handle_call sync tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"sync returns ok for ets backend",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                sync, {self(), make_ref()}, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Unknown Request
%%====================================================================

handle_call_unknown_test_() ->
    {"handle_call unknown request tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"unknown request returns error",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, Result, _NewState} = flurm_dbd_storage:handle_call(
                {unknown_request, data}, {self(), make_ref()}, State),
            ?assertEqual({error, unknown_request}, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    {"handle_cast tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"unknown cast is ignored",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {noreply, NewState} = flurm_dbd_storage:handle_cast(
                {unknown_cast, data}, State),
            ?assertEqual(State, NewState),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"unknown info message is ignored",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {noreply, NewState} = flurm_dbd_storage:handle_info(
                unknown_message, State),
            ?assertEqual(State, NewState),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"terminate returns ok",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            Result = flurm_dbd_storage:terminate(normal, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end},

       {"terminate with error reason returns ok",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            Result = flurm_dbd_storage:terminate({shutdown, error}, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% Integration Tests - CRUD Operations
%%====================================================================

crud_integration_test_() ->
    {"CRUD integration tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"full CRUD lifecycle for jobs table",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),

            %% Create
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{id => 1, name => <<"job1">>, status => pending}},
                {self(), make_ref()}, State),

            %% Read
            {reply, {ok, Job1}, State3} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            ?assertEqual(#{id => 1, name => <<"job1">>, status => pending}, Job1),

            %% Update
            {reply, ok, State4} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{id => 1, name => <<"job1">>, status => running}},
                {self(), make_ref()}, State3),
            {reply, {ok, Job1Updated}, State5} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State4),
            ?assertEqual(running, maps:get(status, Job1Updated)),

            %% Delete
            {reply, ok, State6} = flurm_dbd_storage:handle_call(
                {delete, jobs, 1}, {self(), make_ref()}, State5),
            {reply, {error, not_found}, _State7} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State6),

            cleanup_state(State6)
        end},

       {"operations across multiple tables",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),

            %% Store in different tables
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{name => <<"job1">>}},
                {self(), make_ref()}, State),
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, accounts, <<"acct1">>, #{name => <<"Research">>}},
                {self(), make_ref()}, State2),
            {reply, ok, State4} = flurm_dbd_storage:handle_call(
                {store, users, <<"alice">>, #{uid => 1000}},
                {self(), make_ref()}, State3),
            {reply, ok, State5} = flurm_dbd_storage:handle_call(
                {store, qos, <<"high">>, #{priority => 100}},
                {self(), make_ref()}, State4),

            %% Verify each table has correct data
            {reply, {ok, _}, _} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State5),
            {reply, {ok, _}, _} = flurm_dbd_storage:handle_call(
                {fetch, accounts, <<"acct1">>}, {self(), make_ref()}, State5),
            {reply, {ok, _}, _} = flurm_dbd_storage:handle_call(
                {fetch, users, <<"alice">>}, {self(), make_ref()}, State5),
            {reply, {ok, _}, _} = flurm_dbd_storage:handle_call(
                {fetch, qos, <<"high">>}, {self(), make_ref()}, State5),

            %% List each table
            {reply, Jobs, _} = flurm_dbd_storage:handle_call(
                {list, jobs}, {self(), make_ref()}, State5),
            ?assertEqual(1, length(Jobs)),
            {reply, Accounts, _} = flurm_dbd_storage:handle_call(
                {list, accounts}, {self(), make_ref()}, State5),
            ?assertEqual(1, length(Accounts)),

            cleanup_state(State5)
        end}
      ]}}.

%%====================================================================
%% Table Type Tests
%%====================================================================

table_types_test_() ->
    {"table type tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"jobs table (set type)",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store same key twice - second should overwrite
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, first}, {self(), make_ref()}, State),
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, second}, {self(), make_ref()}, State2),
            %% Only one value should exist
            {reply, Values, _} = flurm_dbd_storage:handle_call(
                {list, jobs}, {self(), make_ref()}, State3),
            ?assertEqual(1, length(Values)),
            ?assertEqual([second], Values),
            cleanup_state(State3)
        end},

       {"steps table (bag type) allows duplicate keys",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            Tables = element(3, State),
            StepsTable = maps:get(steps, Tables),
            %% Bag tables in ETS allow multiple values for same key
            %% Store multiple values directly for same key
            ets:insert(StepsTable, {1, step1}),
            ets:insert(StepsTable, {1, step2}),
            %% Both values should exist
            Values = ets:lookup(StepsTable, 1),
            ?assertEqual(2, length(Values)),
            cleanup_state(State)
        end},

       {"events table (ordered_set type)",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store with timestamps as keys
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, events, 1000, event1}, {self(), make_ref()}, State),
            {reply, ok, State3} = flurm_dbd_storage:handle_call(
                {store, events, 2000, event2}, {self(), make_ref()}, State2),
            {reply, ok, State4} = flurm_dbd_storage:handle_call(
                {store, events, 3000, event3}, {self(), make_ref()}, State3),
            %% List all events
            {reply, Values, _} = flurm_dbd_storage:handle_call(
                {list, events}, {self(), make_ref()}, State4),
            ?assertEqual(3, length(Values)),
            cleanup_state(State4)
        end}
      ]}}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {"edge case tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"store with undefined value",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, undefined}, {self(), make_ref()}, State),
            {reply, {ok, Value}, _} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            ?assertEqual(undefined, Value),
            cleanup_state(State2)
        end},

       {"store with empty map value",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, #{}}, {self(), make_ref()}, State),
            {reply, {ok, Value}, _} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            ?assertEqual(#{}, Value),
            cleanup_state(State2)
        end},

       {"store with empty binary key",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, accounts, <<>>, #{name => <<"empty key">>}},
                {self(), make_ref()}, State),
            {reply, {ok, Value}, _} = flurm_dbd_storage:handle_call(
                {fetch, accounts, <<>>}, {self(), make_ref()}, State2),
            ?assertEqual(#{name => <<"empty key">>}, Value),
            cleanup_state(State2)
        end},

       {"store with large value",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            LargeValue = #{data => lists:duplicate(10000, <<"x">>)},
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, LargeValue}, {self(), make_ref()}, State),
            {reply, {ok, Retrieved}, _} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            ?assertEqual(LargeValue, Retrieved),
            cleanup_state(State2)
        end},

       {"store with nested data structures",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            NestedValue = #{
                level1 => #{
                    level2 => #{
                        level3 => [1, 2, #{key => value}]
                    }
                }
            },
            {reply, ok, State2} = flurm_dbd_storage:handle_call(
                {store, jobs, 1, NestedValue}, {self(), make_ref()}, State),
            {reply, {ok, Retrieved}, _} = flurm_dbd_storage:handle_call(
                {fetch, jobs, 1}, {self(), make_ref()}, State2),
            ?assertEqual(NestedValue, Retrieved),
            cleanup_state(State2)
        end}
      ]}}.

%%====================================================================
%% Concurrency Simulation Tests
%%====================================================================

concurrency_test_() ->
    {"concurrency simulation tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"multiple stores then list",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Simulate multiple rapid stores
            State2 = lists:foldl(
                fun(N, AccState) ->
                    {reply, ok, NewState} = flurm_dbd_storage:handle_call(
                        {store, jobs, N, #{id => N}}, {self(), make_ref()}, AccState),
                    NewState
                end,
                State,
                lists:seq(1, 100)
            ),
            %% List should return all 100 values
            {reply, Values, _} = flurm_dbd_storage:handle_call(
                {list, jobs}, {self(), make_ref()}, State2),
            ?assertEqual(100, length(Values)),
            cleanup_state(State2)
        end},

       {"interleaved stores and deletes",
        fun() ->
            {ok, State} = flurm_dbd_storage:init([]),
            %% Store 10 items
            State2 = lists:foldl(
                fun(N, AccState) ->
                    {reply, ok, NewState} = flurm_dbd_storage:handle_call(
                        {store, jobs, N, #{id => N}}, {self(), make_ref()}, AccState),
                    NewState
                end,
                State,
                lists:seq(1, 10)
            ),
            %% Delete even items
            State3 = lists:foldl(
                fun(N, AccState) ->
                    {reply, ok, NewState} = flurm_dbd_storage:handle_call(
                        {delete, jobs, N}, {self(), make_ref()}, AccState),
                    NewState
                end,
                State2,
                [2, 4, 6, 8, 10]
            ),
            %% Should have 5 items left
            {reply, Values, _} = flurm_dbd_storage:handle_call(
                {list, jobs}, {self(), make_ref()}, State3),
            ?assertEqual(5, length(Values)),
            cleanup_state(State3)
        end}
      ]}}.

%%====================================================================
%% Helper Functions
%%====================================================================

cleanup_state(State) ->
    %% Delete all ETS tables in state
    try
        Tables = element(3, State),
        maps:foreach(fun(_, Tab) ->
            catch ets:delete(Tab)
        end, Tables)
    catch
        _:_ -> ok
    end,
    ok.
