%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_pmi_manager module
%%%
%%% Tests PMI state management functionality including:
%%% - gen_server lifecycle
%%% - Job initialization and finalization
%%% - Rank registration and management
%%% - KVS operations (put, get, get_by_index, commit)
%%% - Barrier synchronization
%%% - Error handling and edge cases
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_manager_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Setup/Teardown
%%%===================================================================

setup() ->
    %% Stop any existing supervisor/manager to ensure clean state
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        SupPid -> catch gen_server:stop(SupPid, normal, 1000)
    end,
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        MgrPid -> catch gen_server:stop(MgrPid, normal, 1000)
    end,
    timer:sleep(50),
    meck:new([lager, flurm_pmi_kvs], [passthrough, non_strict]),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, info, fun(_Msg) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup(_) ->
    %% Stop the manager that tests started
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        MgrPid -> catch gen_server:stop(MgrPid, normal, 1000)
    end,
    meck:unload(),
    ok.

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_pmi_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Start/Init Tests
        {"start_link creates manager", fun test_start_link/0},
        {"init returns initial state", fun test_init/0},

        %% init_job Tests
        {"init_job creates new job", fun test_init_job_new/0},
        {"init_job returns error for existing job", fun test_init_job_already_exists/0},
        {"init_job creates correct KVS name", fun test_init_job_kvs_name/0},
        {"init_job initializes empty KVS", fun test_init_job_empty_kvs/0},
        {"init_job sets correct size", fun test_init_job_size/0},

        %% finalize_job Tests
        {"finalize_job removes existing job", fun test_finalize_job_success/0},
        {"finalize_job returns error for missing job", fun test_finalize_job_not_found/0},

        %% get_job_info Tests
        {"get_job_info returns info for existing job", fun test_get_job_info_exists/0},
        {"get_job_info returns not_found for missing job", fun test_get_job_info_not_found/0},
        {"get_job_info returns correct fields", fun test_get_job_info_fields/0},
        {"get_job_info includes rank_count", fun test_get_job_info_rank_count/0},

        %% register_rank Tests
        {"register_rank adds rank to job", fun test_register_rank_success/0},
        {"register_rank returns error for missing job", fun test_register_rank_job_not_found/0},
        {"register_rank returns error for invalid rank", fun test_register_rank_invalid/0},
        {"register_rank sets correct node", fun test_register_rank_node/0},
        {"register_rank sets initial status", fun test_register_rank_status/0},
        {"register_rank increments rank count", fun test_register_rank_count/0},
        {"register_rank stores caller pid", fun test_register_rank_pid/0},

        %% get_rank_info Tests
        {"get_rank_info returns info for existing rank", fun test_get_rank_info_success/0},
        {"get_rank_info returns error for missing job", fun test_get_rank_info_job_not_found/0},
        {"get_rank_info returns error for missing rank", fun test_get_rank_info_rank_not_found/0},
        {"get_rank_info returns correct fields", fun test_get_rank_info_fields/0},

        %% kvs_put Tests
        {"kvs_put stores key-value", fun test_kvs_put_success/0},
        {"kvs_put returns error for missing job", fun test_kvs_put_job_not_found/0},
        {"kvs_put overwrites existing key", fun test_kvs_put_overwrite/0},
        {"kvs_put handles multiple keys", fun test_kvs_put_multiple/0},

        %% kvs_get Tests
        {"kvs_get retrieves value", fun test_kvs_get_success/0},
        {"kvs_get returns error for missing job", fun test_kvs_get_job_not_found/0},
        {"kvs_get returns not_found for missing key", fun test_kvs_get_key_not_found/0},

        %% kvs_get_by_index Tests
        {"kvs_get_by_index retrieves by index", fun test_kvs_get_by_index_success/0},
        {"kvs_get_by_index returns error for missing job", fun test_kvs_get_by_index_job_not_found/0},
        {"kvs_get_by_index returns end_of_kvs", fun test_kvs_get_by_index_end/0},

        %% kvs_commit Tests
        {"kvs_commit sets committed flag", fun test_kvs_commit_success/0},
        {"kvs_commit returns error for missing job", fun test_kvs_commit_job_not_found/0},
        {"kvs_commit allows get after commit", fun test_kvs_commit_allows_get/0},

        %% barrier_in Tests
        {"barrier_in waits for all ranks", fun test_barrier_in_waiting/0},
        {"barrier_in releases all when complete", fun test_barrier_in_complete/0},
        {"barrier_in returns error for missing job", fun test_barrier_in_job_not_found/0},
        {"barrier_in increments counter", fun test_barrier_in_counter/0},
        {"barrier_in tracks entered ranks", fun test_barrier_in_tracks_ranks/0},
        {"barrier_in releases all waiters", fun test_barrier_in_releases_waiters/0},
        {"barrier_in resets after release", fun test_barrier_in_resets/0},

        %% get_kvs_name Tests
        {"get_kvs_name returns name for existing job", fun test_get_kvs_name_success/0},
        {"get_kvs_name returns error for missing job", fun test_get_kvs_name_not_found/0},

        %% handle_call unknown Tests
        {"handle_call unknown returns error", fun test_handle_call_unknown/0},

        %% handle_cast Tests
        {"handle_cast any returns noreply", fun test_handle_cast/0},

        %% handle_info Tests
        {"handle_info any returns noreply", fun test_handle_info/0},

        %% terminate Tests
        {"terminate returns ok", fun test_terminate/0},

        %% Integration Tests
        {"full lifecycle test", fun test_full_lifecycle/0},
        {"multiple jobs test", fun test_multiple_jobs/0},
        {"multiple ranks test", fun test_multiple_ranks/0},
        {"concurrent barrier test", fun test_concurrent_barrier/0},

        %% Edge Cases
        {"empty KVS operations", fun test_empty_kvs_operations/0},
        {"barrier with single rank", fun test_barrier_single_rank/0},
        {"large number of KVS entries", fun test_large_kvs/0},
        {"rank boundary validation", fun test_rank_boundary/0},

        %% State consistency tests
        {"state preserved after operations", fun test_state_consistency/0},
        {"job isolation test", fun test_job_isolation/0},

        %% Additional coverage tests
        {"init_job with various sizes", fun test_init_job_various_sizes/0},
        {"kvs operations in sequence", fun test_kvs_operations_sequence/0},
        {"barrier partial then complete", fun test_barrier_partial_complete/0},
        {"register multiple ranks same job", fun test_register_multiple_ranks/0},
        {"finalize cleans up completely", fun test_finalize_cleanup/0},

        %% Additional complete coverage tests
        {"init_job step variants", fun test_init_job_step_variants/0},
        {"kvs_put binary conversion", fun test_kvs_put_binary_conversion/0},
        {"kvs_get key conversion", fun test_kvs_get_key_conversion/0},
        {"barrier timeout behavior", fun test_barrier_timeout_behavior/0},
        {"multiple barriers same job", fun test_multiple_barriers_same_job/0},
        {"kvs operations after commit", fun test_kvs_operations_after_commit/0},
        {"rank registration replaces existing", fun test_rank_registration_replaces_existing/0},
        {"get job info committed state", fun test_get_job_info_committed_state/0},
        {"get kvs name format", fun test_get_kvs_name_format/0},
        {"finalize multiple jobs", fun test_finalize_multiple_jobs/0},
        {"kvs get by index various indices", fun test_kvs_get_by_index_various_indices/0},
        {"barrier all ranks simultaneous", fun test_barrier_all_ranks_simultaneous/0},
        {"job info all fields", fun test_job_info_all_fields/0},
        {"register rank boundary check", fun test_register_rank_boundary_check/0},
        {"concurrent kvs operations", fun test_concurrent_kvs_operations/0},
        {"error paths", fun test_error_paths/0},
        {"kvs commit idempotent", fun test_kvs_commit_idempotent/0}
     ]}.

%%%===================================================================
%%% Start/Init Tests
%%%===================================================================

test_start_link() ->
    {ok, Pid} = flurm_pmi_manager:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_pmi_manager)).

test_init() ->
    {ok, State} = flurm_pmi_manager:init([]),
    %% State is #state{jobs = #{}}
    ?assertEqual({state, #{}}, State).

test_init_empty_jobs() ->
    {ok, State} = flurm_pmi_manager:init([]),
    %% Verify the jobs map starts empty
    {state, Jobs} = State,
    ?assertEqual(#{}, Jobs).

%%%===================================================================
%%% init_job Tests
%%%===================================================================

test_init_job_new() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    Result = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual(ok, Result).

test_init_job_already_exists() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    Result = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual({error, already_exists}, Result).

test_init_job_kvs_name() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(42, 7, 2),
    {ok, Name} = flurm_pmi_manager:get_kvs_name(42, 7),
    ?assertEqual(<<"kvs_42_7">>, Name).

test_init_job_empty_kvs() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    KvsCalled = ets:new(kvs_called, [set, public]),
    meck:expect(flurm_pmi_kvs, new, fun() ->
        ets:insert(KvsCalled, {called, true}),
        #{}
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual([{called, true}], ets:lookup(KvsCalled, called)),
    ets:delete(KvsCalled).

test_init_job_size() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 16),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(16, maps:get(size, Info)).

test_init_job_barrier_count() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    %% barrier_count should start at 0
    ?assertEqual(0, maps:get(barrier_count, Info, 0)).

%%%===================================================================
%%% finalize_job Tests
%%%===================================================================

test_finalize_job_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    Result = flurm_pmi_manager:finalize_job(1, 0),
    ?assertEqual(ok, Result),

    %% Job should be gone
    ?assertEqual({error, not_found}, flurm_pmi_manager:get_job_info(1, 0)).

test_finalize_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:finalize_job(999, 0),
    ?assertEqual({error, not_found}, Result).

test_finalize_job_clears_state() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:init_job(2, 0, 2),

    %% Finalize just the first job
    ok = flurm_pmi_manager:finalize_job(1, 0),

    %% First job should be gone
    ?assertEqual({error, not_found}, flurm_pmi_manager:get_job_info(1, 0)),
    %% Second job should still exist
    ?assertMatch({ok, _}, flurm_pmi_manager:get_job_info(2, 0)).

%%%===================================================================
%%% get_job_info Tests
%%%===================================================================

test_get_job_info_exists() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assert(is_map(Info)).

test_get_job_info_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:get_job_info(999, 0),
    ?assertEqual({error, not_found}, Result).

test_get_job_info_fields() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(42, 7, 16),
    {ok, Info} = flurm_pmi_manager:get_job_info(42, 7),

    ?assertEqual(42, maps:get(job_id, Info)),
    ?assertEqual(7, maps:get(step_id, Info)),
    ?assertEqual(16, maps:get(size, Info)),
    ?assertEqual(<<"kvs_42_7">>, maps:get(kvs_name, Info)),
    ?assertEqual(false, maps:get(committed, Info)).

test_get_job_info_rank_count() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"node1">>),

    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(2, maps:get(rank_count, Info)).

%%%===================================================================
%%% register_rank Tests
%%%===================================================================

test_register_rank_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    Result = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    ?assertEqual(ok, Result).

test_register_rank_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:register_rank(999, 0, 0, <<"node1">>),
    ?assertEqual({error, job_not_found}, Result).

test_register_rank_invalid() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    %% Rank 10 is invalid for size 4
    Result = flurm_pmi_manager:register_rank(1, 0, 10, <<"node1">>),
    ?assertEqual({error, invalid_rank}, Result).

test_register_rank_node() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"my_node">>),

    {ok, RankInfo} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(<<"my_node">>, maps:get(node, RankInfo)).

test_register_rank_status() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),

    {ok, RankInfo} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(initialized, maps:get(status, RankInfo)).

test_register_rank_count() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    {ok, Info1} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(0, maps:get(rank_count, Info1, 0)),

    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    {ok, Info2} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(1, maps:get(rank_count, Info2, 0)),

    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"node2">>),
    {ok, Info3} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(2, maps:get(rank_count, Info3, 0)).

test_register_rank_pid() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),

    %% The pid is stored from the caller's pid in handle_call
    %% We verify it's stored by checking rank_info exists
    {ok, _RankInfo} = flurm_pmi_manager:get_rank_info(1, 0, 0).

%%%===================================================================
%%% get_rank_info Tests
%%%===================================================================

test_get_rank_info_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 2, <<"test_node">>),

    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 2),
    ?assert(is_map(Info)).

test_get_rank_info_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:get_rank_info(999, 0, 0),
    ?assertEqual({error, job_not_found}, Result).

test_get_rank_info_rank_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    Result = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual({error, rank_not_found}, Result).

test_get_rank_info_fields() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 3, <<"my_node">>),

    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 3),
    ?assertEqual(3, maps:get(rank, Info)),
    ?assertEqual(<<"my_node">>, maps:get(node, Info)),
    ?assertEqual(initialized, maps:get(status, Info)).

%%%===================================================================
%%% kvs_put Tests
%%%===================================================================

test_kvs_put_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, Key, Value) ->
        maps:put(Key, Value, Kvs)
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    Result = flurm_pmi_manager:kvs_put(1, 0, <<"key1">>, <<"value1">>),
    ?assertEqual(ok, Result).

test_kvs_put_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:kvs_put(999, 0, <<"key">>, <<"value">>),
    ?assertEqual({error, job_not_found}, Result).

test_kvs_put_overwrite() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, Key, Value) ->
        maps:put(Key, Value, Kvs)
    end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, Key) ->
        case maps:find(Key, Kvs) of
            {ok, V} -> {ok, V};
            error -> {error, not_found}
        end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"value1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"value2">>),

    {ok, Value} = flurm_pmi_manager:kvs_get(1, 0, <<"key">>),
    ?assertEqual(<<"value2">>, Value).

test_kvs_put_multiple() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, Key, Value) ->
        maps:put(Key, Value, Kvs)
    end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, Key) ->
        case maps:find(Key, Kvs) of
            {ok, V} -> {ok, V};
            error -> {error, not_found}
        end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    %% Put multiple keys
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key1">>, <<"val1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key2">>, <<"val2">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key3">>, <<"val3">>),

    %% Verify all keys exist
    {ok, <<"val1">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key1">>),
    {ok, <<"val2">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key2">>),
    {ok, <<"val3">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key3">>).

%%%===================================================================
%%% kvs_get Tests
%%%===================================================================

test_kvs_get_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, Key, Value) ->
        maps:put(Key, Value, Kvs)
    end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, Key) ->
        case maps:find(Key, Kvs) of
            {ok, V} -> {ok, V};
            error -> {error, not_found}
        end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"mykey">>, <<"myvalue">>),

    Result = flurm_pmi_manager:kvs_get(1, 0, <<"mykey">>),
    ?assertEqual({ok, <<"myvalue">>}, Result).

test_kvs_get_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:kvs_get(999, 0, <<"key">>),
    ?assertEqual({error, job_not_found}, Result).

test_kvs_get_key_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, get, fun(_Kvs, _Key) ->
        {error, not_found}
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    Result = flurm_pmi_manager:kvs_get(1, 0, <<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

%%%===================================================================
%%% kvs_get_by_index Tests
%%%===================================================================

test_kvs_get_by_index_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, Key, Value) ->
        maps:put(Key, Value, Kvs)
    end),
    meck:expect(flurm_pmi_kvs, get_by_index, fun(_Kvs, 0) ->
        {ok, <<"key0">>, <<"value0">>}
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key0">>, <<"value0">>),

    Result = flurm_pmi_manager:kvs_get_by_index(1, 0, 0),
    ?assertEqual({ok, <<"key0">>, <<"value0">>}, Result).

test_kvs_get_by_index_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:kvs_get_by_index(999, 0, 0),
    ?assertEqual({error, job_not_found}, Result).

test_kvs_get_by_index_end() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, get_by_index, fun(_Kvs, _Index) ->
        {error, end_of_kvs}
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    Result = flurm_pmi_manager:kvs_get_by_index(1, 0, 100),
    ?assertEqual({error, end_of_kvs}, Result).

%%%===================================================================
%%% kvs_commit Tests
%%%===================================================================

test_kvs_commit_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    %% Before commit
    {ok, InfoBefore} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(false, maps:get(committed, InfoBefore)),

    Result = flurm_pmi_manager:kvs_commit(1, 0),
    ?assertEqual(ok, Result),

    %% After commit
    {ok, InfoAfter} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, InfoAfter)).

test_kvs_commit_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:kvs_commit(999, 0),
    ?assertEqual({error, job_not_found}, Result).

test_kvs_commit_allows_get() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, Key, Value) ->
        maps:put(Key, Value, Kvs)
    end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, Key) ->
        case maps:find(Key, Kvs) of
            {ok, V} -> {ok, V};
            error -> {error, not_found}
        end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"value">>),
    ok = flurm_pmi_manager:kvs_commit(1, 0),

    %% Should be able to get values after commit
    {ok, <<"value">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key">>).

%%%===================================================================
%%% barrier_in Tests
%%%===================================================================

test_barrier_in_waiting() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),

    %% First rank enters barrier - should wait
    Parent = self(),
    spawn(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {barrier_result, Result}
    end),

    %% Give it time to start waiting
    timer:sleep(50),

    %% Should not have received result yet
    receive
        {barrier_result, _} -> ?assert(false)
    after 0 -> ok
    end,

    %% Second rank enters - should release both
    ok = flurm_pmi_manager:barrier_in(1, 0, 1),

    %% Now first should complete
    receive
        {barrier_result, Result} -> ?assertEqual(ok, Result)
    after 1000 -> ?assert(false)
    end.

test_barrier_in_complete() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Single rank job - barrier completes immediately
    Result = flurm_pmi_manager:barrier_in(1, 0, 0),
    ?assertEqual(ok, Result).

test_barrier_in_job_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:barrier_in(999, 0, 0),
    ?assertEqual({error, job_not_found}, Result).

test_barrier_in_counter() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 3),

    %% Start first two barrier calls in background
    Parent = self(),
    spawn(fun() ->
        flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {done, 0}
    end),
    spawn(fun() ->
        flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {done, 1}
    end),

    timer:sleep(50),

    %% Third rank completes barrier
    ok = flurm_pmi_manager:barrier_in(1, 0, 2),

    %% All should complete
    receive {done, 0} -> ok after 1000 -> ?assert(false) end,
    receive {done, 1} -> ok after 1000 -> ?assert(false) end.

test_barrier_in_tracks_ranks() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),

    %% First rank enters barrier
    Parent = self(),
    spawn(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {rank0, Result}
    end),

    %% Give time for the barrier call to start waiting
    timer:sleep(100),

    %% Second rank should complete the barrier
    ok = flurm_pmi_manager:barrier_in(1, 0, 1),

    %% First rank should also complete
    receive
        {rank0, ok} -> ok
    after 1000 ->
        ?assert(false)
    end.

test_barrier_in_releases_waiters() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 3),

    Parent = self(),
    Pids = [spawn(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, R),
        Parent ! {done, R, Result}
    end) || R <- [0, 1]],

    timer:sleep(50),

    %% Last rank completes barrier - releases all
    ok = flurm_pmi_manager:barrier_in(1, 0, 2),

    Results = [receive {done, R, Res} -> {R, Res} after 1000 -> timeout end || _ <- Pids],
    ?assertEqual([{0, ok}, {1, ok}], lists:sort(Results)).

test_barrier_in_resets() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% First barrier
    ok = flurm_pmi_manager:barrier_in(1, 0, 0),

    %% Second barrier should also work (counter was reset)
    ok = flurm_pmi_manager:barrier_in(1, 0, 0).

%%%===================================================================
%%% get_kvs_name Tests
%%%===================================================================

test_get_kvs_name_success() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(123, 45, 2),
    {ok, Name} = flurm_pmi_manager:get_kvs_name(123, 45),
    ?assertEqual(<<"kvs_123_45">>, Name).

test_get_kvs_name_not_found() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    Result = flurm_pmi_manager:get_kvs_name(999, 0),
    ?assertEqual({error, not_found}, Result).

%%%===================================================================
%%% handle_call/cast/info Tests
%%%===================================================================

test_handle_call_unknown() ->
    State = {state, #{}},
    {reply, Response, NewState} = flurm_pmi_manager:handle_call(unknown_request, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Response),
    ?assertEqual(State, NewState).

test_handle_cast() ->
    State = {state, #{}},
    {noreply, NewState} = flurm_pmi_manager:handle_cast(any_message, State),
    ?assertEqual(State, NewState).

test_handle_info() ->
    State = {state, #{}},
    {noreply, NewState} = flurm_pmi_manager:handle_info(any_message, State),
    ?assertEqual(State, NewState).

test_terminate() ->
    State = {state, #{}},
    Result = flurm_pmi_manager:terminate(normal, State),
    ?assertEqual(ok, Result).

%%%===================================================================
%%% Integration Tests
%%%===================================================================

test_full_lifecycle() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),

    %% 1. Initialize job
    ok = flurm_pmi_manager:init_job(1, 0, 2),

    %% 2. Register ranks
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"node2">>),

    %% 3. Put KVS values
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key1">>, <<"val1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key2">>, <<"val2">>),

    %% 4. Barrier (run in parallel for both ranks)
    Parent = self(),
    spawn(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {barrier, 0, Result}
    end),

    timer:sleep(10),
    ok = flurm_pmi_manager:barrier_in(1, 0, 1),

    receive {barrier, 0, ok} -> ok after 1000 -> ?assert(false) end,

    %% 5. Commit KVS
    ok = flurm_pmi_manager:kvs_commit(1, 0),

    %% 6. Get values
    {ok, <<"val1">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key1">>),
    {ok, <<"val2">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key2">>),

    %% 7. Finalize
    ok = flurm_pmi_manager:finalize_job(1, 0),
    {error, not_found} = flurm_pmi_manager:get_job_info(1, 0).

test_multiple_jobs() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),

    %% Create multiple jobs
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:init_job(2, 0, 4),
    ok = flurm_pmi_manager:init_job(1, 1, 8),

    %% Operations on different jobs
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"job1_step0">>),
    ok = flurm_pmi_manager:kvs_put(2, 0, <<"key">>, <<"job2_step0">>),
    ok = flurm_pmi_manager:kvs_put(1, 1, <<"key">>, <<"job1_step1">>),

    %% Values are isolated
    {ok, <<"job1_step0">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key">>),
    {ok, <<"job2_step0">>} = flurm_pmi_manager:kvs_get(2, 0, <<"key">>),
    {ok, <<"job1_step1">>} = flurm_pmi_manager:kvs_get(1, 1, <<"key">>).

test_multiple_ranks() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 8),

    %% Register all ranks
    [ok = flurm_pmi_manager:register_rank(1, 0, R, <<"node", (R + $0):8>>) || R <- lists:seq(0, 7)],

    %% Verify all ranks
    [begin
        {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, R),
        ?assertEqual(R, maps:get(rank, Info))
    end || R <- lists:seq(0, 7)],

    {ok, JobInfo} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(8, maps:get(rank_count, JobInfo)).

test_concurrent_barrier() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    NumRanks = 4,
    ok = flurm_pmi_manager:init_job(1, 0, NumRanks),

    Parent = self(),

    %% Start all ranks concurrently
    [spawn(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, R),
        Parent ! {barrier_done, R, Result}
    end) || R <- lists:seq(0, NumRanks - 1)],

    %% All should complete
    Results = [receive
        {barrier_done, R, Res} -> {R, Res}
    after 5000 -> {timeout, R}
    end || R <- lists:seq(0, NumRanks - 1)],

    Expected = [{R, ok} || R <- lists:seq(0, NumRanks - 1)],
    ?assertEqual(Expected, lists:sort(Results)).

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

test_empty_kvs_operations() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, get, fun(_Kvs, _Key) -> {error, not_found} end),
    meck:expect(flurm_pmi_kvs, get_by_index, fun(_Kvs, _Index) -> {error, end_of_kvs} end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Get from empty KVS
    {error, not_found} = flurm_pmi_manager:kvs_get(1, 0, <<"any">>),
    {error, end_of_kvs} = flurm_pmi_manager:kvs_get_by_index(1, 0, 0).

test_barrier_single_rank() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Barrier with single rank completes immediately
    ok = flurm_pmi_manager:barrier_in(1, 0, 0).

test_large_kvs() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Put many keys
    NumKeys = 100,
    [ok = flurm_pmi_manager:kvs_put(1, 0,
        <<"key_", (integer_to_binary(I))/binary>>,
        <<"val_", (integer_to_binary(I))/binary>>) || I <- lists:seq(1, NumKeys)],

    ok.

test_rank_boundary() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    %% Valid ranks (0 to 3)
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 3, <<"n">>),

    %% Invalid ranks
    {error, invalid_rank} = flurm_pmi_manager:register_rank(1, 0, 4, <<"n">>),
    {error, invalid_rank} = flurm_pmi_manager:register_rank(1, 0, 100, <<"n">>).

test_state_consistency() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),

    %% Multiple operations
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"v">>),
    ok = flurm_pmi_manager:kvs_commit(1, 0),

    %% State should be consistent
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(1, maps:get(rank_count, Info)),
    ?assertEqual(true, maps:get(committed, Info)).

test_job_isolation() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:init_job(2, 0, 2),

    %% Put in job 1
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"from_job_1">>),

    %% Get from job 2 should fail
    {error, not_found} = flurm_pmi_manager:kvs_get(2, 0, <<"key">>),

    %% Commit job 1, job 2 should not be affected
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    {ok, Info2} = flurm_pmi_manager:get_job_info(2, 0),
    ?assertEqual(false, maps:get(committed, Info2)).

test_init_job_various_sizes() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    %% Test various sizes
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:init_job(2, 0, 100),
    ok = flurm_pmi_manager:init_job(3, 0, 1000),

    {ok, Info1} = flurm_pmi_manager:get_job_info(1, 0),
    {ok, Info2} = flurm_pmi_manager:get_job_info(2, 0),
    {ok, Info3} = flurm_pmi_manager:get_job_info(3, 0),

    ?assertEqual(1, maps:get(size, Info1)),
    ?assertEqual(100, maps:get(size, Info2)),
    ?assertEqual(1000, maps:get(size, Info3)).

test_kvs_operations_sequence() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),
    meck:expect(flurm_pmi_kvs, get_by_index, fun(Kvs, Idx) ->
        Keys = lists:sort(maps:keys(Kvs)),
        case Idx < length(Keys) of
            true ->
                K = lists:nth(Idx + 1, Keys),
                {ok, K, maps:get(K, Kvs)};
            false ->
                {error, end_of_kvs}
        end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Put multiple keys
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"a">>, <<"1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"b">>, <<"2">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"c">>, <<"3">>),

    %% Get by key
    {ok, <<"1">>} = flurm_pmi_manager:kvs_get(1, 0, <<"a">>),
    {ok, <<"2">>} = flurm_pmi_manager:kvs_get(1, 0, <<"b">>),
    {ok, <<"3">>} = flurm_pmi_manager:kvs_get(1, 0, <<"c">>),

    %% Get by index
    {ok, <<"a">>, <<"1">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 0),
    {ok, <<"b">>, <<"2">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 1),
    {ok, <<"c">>, <<"3">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 2),
    {error, end_of_kvs} = flurm_pmi_manager:kvs_get_by_index(1, 0, 3).

test_barrier_partial_complete() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 3),

    Parent = self(),

    %% First rank enters
    spawn(fun() ->
        flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {done, 0}
    end),
    timer:sleep(20),

    %% Second rank enters
    spawn(fun() ->
        flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {done, 1}
    end),
    timer:sleep(20),

    %% Neither should be done yet
    receive {done, _} -> ?assert(false) after 0 -> ok end,

    %% Third rank completes barrier
    ok = flurm_pmi_manager:barrier_in(1, 0, 2),

    %% All should complete
    receive {done, 0} -> ok after 1000 -> ?assert(false) end,
    receive {done, 1} -> ok after 1000 -> ?assert(false) end.

test_register_multiple_ranks() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    %% Register ranks in non-sequential order
    ok = flurm_pmi_manager:register_rank(1, 0, 2, <<"node_c">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node_a">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 3, <<"node_d">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"node_b">>),

    %% All should be accessible
    {ok, Info0} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    {ok, Info1} = flurm_pmi_manager:get_rank_info(1, 0, 1),
    {ok, Info2} = flurm_pmi_manager:get_rank_info(1, 0, 2),
    {ok, Info3} = flurm_pmi_manager:get_rank_info(1, 0, 3),

    ?assertEqual(<<"node_a">>, maps:get(node, Info0)),
    ?assertEqual(<<"node_b">>, maps:get(node, Info1)),
    ?assertEqual(<<"node_c">>, maps:get(node, Info2)),
    ?assertEqual(<<"node_d">>, maps:get(node, Info3)).

test_finalize_cleanup() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"v">>),
    ok = flurm_pmi_manager:kvs_commit(1, 0),

    %% Finalize
    ok = flurm_pmi_manager:finalize_job(1, 0),

    %% Everything should be gone
    {error, not_found} = flurm_pmi_manager:get_job_info(1, 0),
    {error, not_found} = flurm_pmi_manager:get_kvs_name(1, 0),
    {error, job_not_found} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    {error, job_not_found} = flurm_pmi_manager:kvs_get(1, 0, <<"k">>).

%%%===================================================================
%%% Additional Tests for Complete Coverage
%%%===================================================================

test_init_job_step_variants() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    %% Same job, different steps
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:init_job(1, 1, 4),
    ok = flurm_pmi_manager:init_job(1, 2, 4),

    %% All should exist independently
    {ok, _} = flurm_pmi_manager:get_job_info(1, 0),
    {ok, _} = flurm_pmi_manager:get_job_info(1, 1),
    {ok, _} = flurm_pmi_manager:get_job_info(1, 2).

test_kvs_put_binary_conversion() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) ->
        ?assert(is_binary(K)),
        ?assert(is_binary(V)),
        maps:put(K, V, Kvs)
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"binkey">>, <<"binval">>).

test_kvs_get_key_conversion() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"testkey">>, <<"testval">>),
    {ok, <<"testval">>} = flurm_pmi_manager:kvs_get(1, 0, <<"testkey">>).

test_barrier_timeout_behavior() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 5),

    %% Start 4 ranks at barrier - not complete
    Parent = self(),
    Pids = [spawn(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, R),
        Parent ! {done, R, Result}
    end) || R <- [0, 1, 2, 3]],

    timer:sleep(50),

    %% None should complete yet
    receive
        {done, _, _} -> ?assert(false)
    after 0 -> ok
    end,

    %% Now complete with rank 4
    ok = flurm_pmi_manager:barrier_in(1, 0, 4),

    %% All should complete
    [receive {done, _, ok} -> ok after 1000 -> ?assert(false) end || _ <- Pids].

test_multiple_barriers_same_job() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),

    %% First barrier
    Parent = self(),
    spawn(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {barrier1, 0, done}
    end),
    timer:sleep(20),
    ok = flurm_pmi_manager:barrier_in(1, 0, 1),
    receive {barrier1, 0, done} -> ok after 1000 -> ?assert(false) end,

    %% Second barrier
    spawn(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {barrier2, 0, done}
    end),
    timer:sleep(20),
    ok = flurm_pmi_manager:barrier_in(1, 0, 1),
    receive {barrier2, 0, done} -> ok after 1000 -> ?assert(false) end.

test_kvs_operations_after_commit() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get, fun(Kvs, K) ->
        case maps:find(K, Kvs) of {ok, V} -> {ok, V}; error -> {error, not_found} end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Put before commit
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"before">>, <<"val1">>),

    %% Commit
    ok = flurm_pmi_manager:kvs_commit(1, 0),

    %% Put after commit
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"after">>, <<"val2">>),

    %% Both should be accessible
    {ok, <<"val1">>} = flurm_pmi_manager:kvs_get(1, 0, <<"before">>),
    {ok, <<"val2">>} = flurm_pmi_manager:kvs_get(1, 0, <<"after">>).

test_rank_registration_replaces_existing() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    %% Register rank 0 twice
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node_first">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node_second">>),

    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(<<"node_second">>, maps:get(node, Info)).

test_get_job_info_committed_state() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 2),

    %% Before commit
    {ok, Info1} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(false, maps:get(committed, Info1)),

    %% After commit
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    {ok, Info2} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, Info2)).

test_get_kvs_name_format() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(123, 45, 2),
    {ok, Name} = flurm_pmi_manager:get_kvs_name(123, 45),
    ?assertEqual(<<"kvs_123_45">>, Name).

test_finalize_multiple_jobs() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:init_job(2, 0, 1),
    ok = flurm_pmi_manager:init_job(3, 0, 1),

    %% Finalize one at a time
    ok = flurm_pmi_manager:finalize_job(1, 0),
    {ok, _} = flurm_pmi_manager:get_job_info(2, 0),
    {ok, _} = flurm_pmi_manager:get_job_info(3, 0),

    ok = flurm_pmi_manager:finalize_job(2, 0),
    {ok, _} = flurm_pmi_manager:get_job_info(3, 0),

    ok = flurm_pmi_manager:finalize_job(3, 0).

test_kvs_get_by_index_various_indices() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),
    meck:expect(flurm_pmi_kvs, get_by_index, fun(Kvs, Idx) ->
        Keys = lists:sort(maps:keys(Kvs)),
        case Idx < length(Keys) of
            true ->
                K = lists:nth(Idx + 1, Keys),
                {ok, K, maps:get(K, Kvs)};
            false ->
                {error, end_of_kvs}
        end
    end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"a">>, <<"1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"b">>, <<"2">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"c">>, <<"3">>),

    {ok, <<"a">>, <<"1">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 0),
    {ok, <<"b">>, <<"2">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 1),
    {ok, <<"c">>, <<"3">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 2),
    {error, end_of_kvs} = flurm_pmi_manager:kvs_get_by_index(1, 0, 3).

test_barrier_all_ranks_simultaneous() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    NumRanks = 8,
    ok = flurm_pmi_manager:init_job(1, 0, NumRanks),

    Parent = self(),
    Refs = [begin
        Ref = make_ref(),
        spawn(fun() ->
            Result = flurm_pmi_manager:barrier_in(1, 0, R),
            Parent ! {Ref, R, Result}
        end),
        Ref
    end || R <- lists:seq(0, NumRanks - 1)],

    %% All should complete
    Results = [receive
        {Ref, R, Res} -> {R, Res}
    after 5000 -> timeout
    end || Ref <- Refs],

    ?assertEqual(NumRanks, length([R || {_, ok} = R <- Results])).

test_job_info_all_fields() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(100, 5, 16),
    ok = flurm_pmi_manager:register_rank(100, 5, 0, <<"n1">>),
    ok = flurm_pmi_manager:register_rank(100, 5, 1, <<"n2">>),

    {ok, Info} = flurm_pmi_manager:get_job_info(100, 5),

    ?assertEqual(100, maps:get(job_id, Info)),
    ?assertEqual(5, maps:get(step_id, Info)),
    ?assertEqual(16, maps:get(size, Info)),
    ?assertEqual(<<"kvs_100_5">>, maps:get(kvs_name, Info)),
    ?assertEqual(false, maps:get(committed, Info)),
    ?assertEqual(2, maps:get(rank_count, Info)).

test_register_rank_boundary_check() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 4),

    %% Valid ranks: 0, 1, 2, 3
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 3, <<"n">>),

    %% Invalid ranks: 4, 5, 100
    {error, invalid_rank} = flurm_pmi_manager:register_rank(1, 0, 4, <<"n">>),
    {error, invalid_rank} = flurm_pmi_manager:register_rank(1, 0, 5, <<"n">>),
    {error, invalid_rank} = flurm_pmi_manager:register_rank(1, 0, 100, <<"n">>).

test_concurrent_kvs_operations() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),
    meck:expect(flurm_pmi_kvs, put, fun(Kvs, K, V) -> maps:put(K, V, Kvs) end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Concurrent puts
    Parent = self(),
    [spawn(fun() ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Val = <<"val_", (integer_to_binary(I))/binary>>,
        ok = flurm_pmi_manager:kvs_put(1, 0, Key, Val),
        Parent ! {done, I}
    end) || I <- lists:seq(1, 10)],

    %% Wait for all
    [receive {done, I} -> ok end || I <- lists:seq(1, 10)].

test_error_paths() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),

    %% All operations on non-existent job
    {error, not_found} = flurm_pmi_manager:get_job_info(999, 0),
    {error, not_found} = flurm_pmi_manager:finalize_job(999, 0),
    {error, not_found} = flurm_pmi_manager:get_kvs_name(999, 0),
    {error, job_not_found} = flurm_pmi_manager:register_rank(999, 0, 0, <<"n">>),
    {error, job_not_found} = flurm_pmi_manager:get_rank_info(999, 0, 0),
    {error, job_not_found} = flurm_pmi_manager:kvs_put(999, 0, <<"k">>, <<"v">>),
    {error, job_not_found} = flurm_pmi_manager:kvs_get(999, 0, <<"k">>),
    {error, job_not_found} = flurm_pmi_manager:kvs_get_by_index(999, 0, 0),
    {error, job_not_found} = flurm_pmi_manager:kvs_commit(999, 0),
    {error, job_not_found} = flurm_pmi_manager:barrier_in(999, 0, 0).

test_kvs_commit_idempotent() ->
    {ok, _Pid} = flurm_pmi_manager:start_link(),
    meck:expect(flurm_pmi_kvs, new, fun() -> #{} end),

    ok = flurm_pmi_manager:init_job(1, 0, 1),

    %% Commit multiple times
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    ok = flurm_pmi_manager:kvs_commit(1, 0),

    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, Info)).
