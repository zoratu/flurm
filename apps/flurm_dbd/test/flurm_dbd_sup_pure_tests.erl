%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_dbd_sup
%%%
%%% Tests the supervisor init/1 callback and pure utility functions
%%% without starting actual processes or mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Store original values to restore later
    Keys = [listen_port, listen_address, num_acceptors, max_connections],
    OriginalValues = [{K, application:get_env(flurm_dbd, K)} || K <- Keys],
    OriginalValues.

cleanup(OriginalValues) ->
    lists:foreach(
        fun({Key, undefined}) ->
                application:unset_env(flurm_dbd, Key);
           ({Key, {ok, Value}}) ->
                application:set_env(flurm_dbd, Key, Value)
        end, OriginalValues).

%%====================================================================
%% Test Generators
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init returns valid supervisor spec",
       fun init_returns_valid_spec/0},
      {"init returns correct supervisor flags",
       fun init_returns_correct_flags/0},
      {"init includes required children",
       fun init_includes_required_children/0},
      {"init children count is correct",
       fun init_children_count_is_correct/0}
     ]}.

%%====================================================================
%% Init Tests
%%====================================================================

init_returns_valid_spec() ->
    {ok, {SupFlags, Children}} = flurm_dbd_sup:init([]),

    ?assert(is_map(SupFlags)),
    ?assert(is_list(Children)),
    ?assert(length(Children) > 0).

init_returns_correct_flags() ->
    {ok, {SupFlags, _Children}} = flurm_dbd_sup:init([]),

    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(10, maps:get(period, SupFlags)).

init_includes_required_children() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    %% Get the IDs of all children
    ChildIds = [maps:get(id, Child) || Child <- Children],

    %% Verify required children are present
    ?assert(lists:member(flurm_dbd_storage, ChildIds)),
    ?assert(lists:member(flurm_dbd_server, ChildIds)).

init_children_count_is_correct() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    %% DBD should have exactly 2 children: storage and server
    ?assertEqual(2, length(Children)).

%%====================================================================
%% Child Spec Validation Tests
%%====================================================================

child_spec_validation_test_() ->
    [
     {"all child specs have required fields",
      fun all_child_specs_have_required_fields/0},
     {"all child specs have valid restart strategy",
      fun all_child_specs_have_valid_restart/0},
     {"all child specs have valid type",
      fun all_child_specs_have_valid_type/0},
     {"all child specs have valid shutdown",
      fun all_child_specs_have_valid_shutdown/0}
    ].

all_child_specs_have_required_fields() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    RequiredKeys = [id, start, restart, shutdown, type, modules],
    lists:foreach(
        fun(Child) ->
            lists:foreach(
                fun(Key) ->
                    ?assert(maps:is_key(Key, Child),
                           io_lib:format("Child ~p missing key ~p",
                                        [maps:get(id, Child), Key]))
                end, RequiredKeys)
        end, Children).

all_child_specs_have_valid_restart() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    ValidRestarts = [permanent, temporary, transient],
    lists:foreach(
        fun(Child) ->
            Restart = maps:get(restart, Child),
            ?assert(lists:member(Restart, ValidRestarts),
                   io_lib:format("Child ~p has invalid restart: ~p",
                                [maps:get(id, Child), Restart]))
        end, Children).

all_child_specs_have_valid_type() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    ValidTypes = [worker, supervisor],
    lists:foreach(
        fun(Child) ->
            Type = maps:get(type, Child),
            ?assert(lists:member(Type, ValidTypes),
                   io_lib:format("Child ~p has invalid type: ~p",
                                [maps:get(id, Child), Type]))
        end, Children).

all_child_specs_have_valid_shutdown() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    lists:foreach(
        fun(Child) ->
            Shutdown = maps:get(shutdown, Child),
            %% Shutdown can be brutal_kill, infinity, or a positive integer
            ?assert(Shutdown =:= brutal_kill orelse
                   Shutdown =:= infinity orelse
                   (is_integer(Shutdown) andalso Shutdown > 0),
                   io_lib:format("Child ~p has invalid shutdown: ~p",
                                [maps:get(id, Child), Shutdown]))
        end, Children).

%%====================================================================
%% Storage Child Spec Test
%%====================================================================

storage_child_spec_test() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    %% Find the storage child
    [StorageChild] = [C || C <- Children, maps:get(id, C) =:= flurm_dbd_storage],

    ?assertEqual(flurm_dbd_storage, maps:get(id, StorageChild)),
    ?assertEqual({flurm_dbd_storage, start_link, []}, maps:get(start, StorageChild)),
    ?assertEqual(permanent, maps:get(restart, StorageChild)),
    ?assertEqual(5000, maps:get(shutdown, StorageChild)),
    ?assertEqual(worker, maps:get(type, StorageChild)),
    ?assertEqual([flurm_dbd_storage], maps:get(modules, StorageChild)).

%%====================================================================
%% Server Child Spec Test
%%====================================================================

server_child_spec_test() ->
    {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),

    %% Find the server child
    [ServerChild] = [C || C <- Children, maps:get(id, C) =:= flurm_dbd_server],

    ?assertEqual(flurm_dbd_server, maps:get(id, ServerChild)),
    ?assertEqual({flurm_dbd_server, start_link, []}, maps:get(start, ServerChild)),
    ?assertEqual(permanent, maps:get(restart, ServerChild)),
    ?assertEqual(5000, maps:get(shutdown, ServerChild)),
    ?assertEqual(worker, maps:get(type, ServerChild)),
    ?assertEqual([flurm_dbd_server], maps:get(modules, ServerChild)).

%%====================================================================
%% Listener Info Tests
%%====================================================================

listener_info_test() ->
    %% Without Ranch listener running, should return error
    Result = flurm_dbd_sup:listener_info(),
    ?assertEqual({error, not_found}, Result).
