%%%-------------------------------------------------------------------
%%% @doc Test suite for flurm_dbd_storage internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_storage_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    application:ensure_all_started(lager),
    %% Set backend to ETS for testing (no Mnesia required)
    application:set_env(flurm_dbd, storage_backend, ets),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

dbd_storage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init_mnesia_tables creates tables", fun init_mnesia_tables_test/0},
      {"gen_server init with ets backend", fun init_ets_test/0},
      {"store and fetch operations", fun store_fetch_test/0},
      {"delete operation", fun delete_test/0},
      {"list operations", fun list_test/0},
      {"sync operation", fun sync_test/0}
     ]}.

%%====================================================================
%% Tests
%%====================================================================

init_mnesia_tables_test() ->
    %% Ensure Mnesia is started
    application:ensure_all_started(mnesia),

    %% Call init_mnesia_tables - this may create tables or report they exist
    %% Either way it should not crash
    Result = flurm_dbd_storage:init_mnesia_tables(),
    ?assertEqual(ok, Result).

init_ets_test() ->
    %% Test the gen_server init with ETS backend
    {ok, State} = flurm_dbd_storage:init([]),

    %% State should be a record, check structure
    ?assertMatch({state, ets, _Tables}, State),

    %% Tables map should have all expected tables
    {state, ets, Tables} = State,
    ?assert(maps:is_key(jobs, Tables)),
    ?assert(maps:is_key(steps, Tables)),
    ?assert(maps:is_key(events, Tables)),
    ?assert(maps:is_key(accounts, Tables)),
    ?assert(maps:is_key(users, Tables)),
    ?assert(maps:is_key(associations, Tables)),
    ?assert(maps:is_key(qos, Tables)),
    ?assert(maps:is_key(usage, Tables)).

store_fetch_test() ->
    %% Start the storage server
    {ok, Pid} = flurm_dbd_storage:start_link(),

    %% Store a value
    StoreResult = flurm_dbd_storage:store(jobs, job_123, #{name => <<"test_job">>}),
    ?assertEqual(ok, StoreResult),

    %% Fetch it back
    {ok, Value} = flurm_dbd_storage:fetch(jobs, job_123),
    ?assertEqual(#{name => <<"test_job">>}, Value),

    %% Fetch non-existent
    NotFoundResult = flurm_dbd_storage:fetch(jobs, nonexistent_key),
    ?assertEqual({error, not_found}, NotFoundResult),

    %% Fetch from unknown table
    UnknownResult = flurm_dbd_storage:fetch(unknown_table, key),
    ?assertEqual({error, unknown_table}, UnknownResult),

    %% Stop server
    gen_server:stop(Pid).

delete_test() ->
    %% Start the storage server
    {ok, Pid} = flurm_dbd_storage:start_link(),

    %% Store then delete
    flurm_dbd_storage:store(accounts, acct_1, #{name => <<"account1">>}),

    %% Verify it exists
    {ok, _} = flurm_dbd_storage:fetch(accounts, acct_1),

    %% Delete it
    DeleteResult = flurm_dbd_storage:delete(accounts, acct_1),
    ?assertEqual(ok, DeleteResult),

    %% Verify it's gone
    NotFoundResult = flurm_dbd_storage:fetch(accounts, acct_1),
    ?assertEqual({error, not_found}, NotFoundResult),

    %% Stop server
    gen_server:stop(Pid).

list_test() ->
    %% Start the storage server
    {ok, Pid} = flurm_dbd_storage:start_link(),

    %% Store some values
    flurm_dbd_storage:store(users, user_1, #{name => <<"alice">>}),
    flurm_dbd_storage:store(users, user_2, #{name => <<"bob">>}),
    flurm_dbd_storage:store(users, user_3, #{name => <<"charlie">>}),

    %% List all
    AllUsers = flurm_dbd_storage:list(users),
    ?assertEqual(3, length(AllUsers)),

    %% List from empty/unknown table
    EmptyResult = flurm_dbd_storage:list(unknown_table),
    ?assertEqual([], EmptyResult),

    %% Stop server
    gen_server:stop(Pid).

sync_test() ->
    %% Start the storage server
    {ok, Pid} = flurm_dbd_storage:start_link(),

    %% Sync should return ok for ETS backend
    SyncResult = flurm_dbd_storage:sync(),
    ?assertEqual(ok, SyncResult),

    %% Stop server
    gen_server:stop(Pid).
