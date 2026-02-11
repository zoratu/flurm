%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_pmi_task
%%%
%%% Covers PMI environment variable generation, setup/cleanup
%%% lifecycle with mocked listener and manager.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_pmi_task_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_task_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

pmi_task_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_pmi_env returns list of tuples",
         fun test_get_pmi_env_returns_list/0},
        {"get_pmi_env PMI_RANK matches input",
         fun test_get_pmi_env_rank/0},
        {"get_pmi_env PMI_SIZE matches input",
         fun test_get_pmi_env_size/0},
        {"get_pmi_env PMI_JOBID has correct format",
         fun test_get_pmi_env_jobid/0},
        {"get_pmi_env PMI_SPAWNED is 0",
         fun test_get_pmi_env_spawned/0},
        {"get_pmi_env PMI_APPNUM is 0",
         fun test_get_pmi_env_appnum/0},
        {"get_pmi_env PMI_SOCK_PATH contains pmi path",
         fun test_get_pmi_env_sockpath/0},
        {"get_pmi_env returns all 10 expected variables",
         fun test_get_pmi_env_count/0},
        {"setup_pmi starts listener when not running",
         fun test_setup_pmi_starts_listener/0},
        {"setup_pmi reuses running listener",
         fun test_setup_pmi_reuses_listener/0},
        {"setup_pmi failure returns error",
         fun test_setup_pmi_failure/0},
        {"cleanup_pmi stops listener and finalizes",
         fun test_cleanup_pmi/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    catch meck:unload(flurm_pmi_listener),
    catch meck:unload(flurm_pmi_manager),
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:new(flurm_pmi_manager, [non_strict]),
    %% Default: get_socket_path returns a predictable path
    meck:expect(flurm_pmi_listener, get_socket_path, fun(JobId, StepId) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(JobId) ++ "_" ++ integer_to_list(StepId) ++ ".sock"
    end),
    meck:expect(flurm_pmi_listener, stop, fun(_JobId, _StepId) -> ok end),
    meck:expect(flurm_pmi_manager, finalize_job, fun(_JobId, _StepId) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_pmi_listener),
    catch meck:unload(flurm_pmi_manager),
    ok.

%%====================================================================
%% Tests
%%====================================================================

test_get_pmi_env_returns_list() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    ?assert(is_list(Env)),
    lists:foreach(fun({K, V}) ->
        ?assert(is_binary(K)),
        ?assert(is_binary(V))
    end, Env).

test_get_pmi_env_rank() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    ?assertEqual(<<"3">>, proplists:get_value(<<"PMI_RANK">>, Env)).

test_get_pmi_env_size() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    ?assertEqual(<<"8">>, proplists:get_value(<<"PMI_SIZE">>, Env)).

test_get_pmi_env_jobid() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    ?assertEqual(<<"10_0">>, proplists:get_value(<<"PMI_JOBID">>, Env)).

test_get_pmi_env_spawned() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_SPAWNED">>, Env)).

test_get_pmi_env_appnum() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_APPNUM">>, Env)).

test_get_pmi_env_sockpath() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    Path = proplists:get_value(<<"PMI_SOCK_PATH">>, Env),
    ?assertNotEqual(undefined, Path),
    ?assertNotEqual(nomatch, binary:match(Path, <<"flurm_pmi_10_0">>)).

test_get_pmi_env_count() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(10, length(Env)).

test_setup_pmi_starts_listener() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, _Sz) -> {ok, FakePid} end),
    {ok, Pid, Path} = flurm_pmi_task:setup_pmi(5, 0, 4, <<"node1">>),
    ?assertEqual(FakePid, Pid),
    ?assert(is_list(Path)),
    ?assert(meck:called(flurm_pmi_listener, start_link, [5, 0, 4])),
    FakePid ! stop.

test_setup_pmi_reuses_listener() ->
    %% Register a fake process under the listener name
    FakePid = spawn(fun() -> receive stop -> ok end end),
    ListenerName = list_to_atom(lists:flatten(io_lib:format("flurm_pmi_listener_~p_~p", [5, 0]))),
    register(ListenerName, FakePid),
    meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, _Sz) -> error(should_not_be_called) end),
    {ok, Pid, _Path} = flurm_pmi_task:setup_pmi(5, 0, 4, <<"node1">>),
    ?assertEqual(FakePid, Pid),
    %% start_link should NOT have been called
    ?assertEqual(0, meck:num_calls(flurm_pmi_listener, start_link, '_')),
    FakePid ! stop,
    timer:sleep(50).  % Let process die and unregister

test_setup_pmi_failure() ->
    meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, _Sz) -> {error, eaddrinuse} end),
    ?assertEqual({error, eaddrinuse}, flurm_pmi_task:setup_pmi(5, 0, 4, <<"node1">>)).

test_cleanup_pmi() ->
    ok = flurm_pmi_task:cleanup_pmi(5, 0),
    ?assert(meck:called(flurm_pmi_listener, stop, [5, 0])),
    ?assert(meck:called(flurm_pmi_manager, finalize_job, [5, 0])).
