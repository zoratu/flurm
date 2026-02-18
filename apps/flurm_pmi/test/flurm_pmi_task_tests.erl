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
         fun test_cleanup_pmi/0},
        {"get_pmi_env SLURM_PMI_FD undefined",
         fun test_get_pmi_env_slurm_pmi_fd/0},
        {"get_pmi_env MPICH_INTERFACE_HOSTNAME present",
         fun test_get_pmi_env_mpich_hostname/0},
        {"get_pmi_env OMPI_MCA_btl tcp,self",
         fun test_get_pmi_env_ompi_mca_btl/0},
        {"get_pmi_env different job ids",
         fun test_get_pmi_env_different_job_ids/0},
        {"get_pmi_env different step ids",
         fun test_get_pmi_env_different_step_ids/0},
        {"get_pmi_env large rank",
         fun test_get_pmi_env_large_rank/0},
        {"get_pmi_env rank zero",
         fun test_get_pmi_env_rank_zero/0},
        {"get_pmi_env all binaries",
         fun test_get_pmi_env_all_binaries/0},
        {"get_pmi_env no duplicates",
         fun test_get_pmi_env_no_duplicates/0},
        {"setup_pmi different sizes",
         fun test_setup_pmi_different_sizes/0},
        {"cleanup_pmi different job",
         fun test_cleanup_pmi_different_job/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Stop any real processes before mocking
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        SupPid -> catch gen_server:stop(SupPid, normal, 1000)
    end,
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        MgrPid -> catch gen_server:stop(MgrPid, normal, 1000)
    end,
    timer:sleep(50),
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

%%====================================================================
%% Additional PMI Environment Tests
%%====================================================================

test_get_pmi_env_slurm_pmi_fd() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    %% SLURM_PMI_FD should be undefined (not using pre-connected FD)
    ?assertEqual(<<"undefined">>, proplists:get_value(<<"SLURM_PMI_FD">>, Env)).

test_get_pmi_env_mpich_hostname() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    %% MPICH_INTERFACE_HOSTNAME should be present
    Hostname = proplists:get_value(<<"MPICH_INTERFACE_HOSTNAME">>, Env),
    ?assertNotEqual(undefined, Hostname),
    ?assert(is_binary(Hostname)).

test_get_pmi_env_ompi_mca_btl() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    %% OMPI_MCA_btl should be tcp,self
    ?assertEqual(<<"tcp,self">>, proplists:get_value(<<"OMPI_MCA_btl">>, Env)).

test_get_pmi_env_different_job_ids() ->
    Env1 = flurm_pmi_task:get_pmi_env(100, 0, 0, 4),
    Env2 = flurm_pmi_task:get_pmi_env(200, 0, 0, 4),
    JobId1 = proplists:get_value(<<"PMI_JOBID">>, Env1),
    JobId2 = proplists:get_value(<<"PMI_JOBID">>, Env2),
    ?assertEqual(<<"100_0">>, JobId1),
    ?assertEqual(<<"200_0">>, JobId2),
    ?assertNotEqual(JobId1, JobId2).

test_get_pmi_env_different_step_ids() ->
    Env1 = flurm_pmi_task:get_pmi_env(10, 0, 0, 4),
    Env2 = flurm_pmi_task:get_pmi_env(10, 1, 0, 4),
    JobId1 = proplists:get_value(<<"PMI_JOBID">>, Env1),
    JobId2 = proplists:get_value(<<"PMI_JOBID">>, Env2),
    ?assertEqual(<<"10_0">>, JobId1),
    ?assertEqual(<<"10_1">>, JobId2).

test_get_pmi_env_large_rank() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 999, 1000),
    ?assertEqual(<<"999">>, proplists:get_value(<<"PMI_RANK">>, Env)),
    ?assertEqual(<<"1000">>, proplists:get_value(<<"PMI_SIZE">>, Env)).

test_get_pmi_env_rank_zero() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_RANK">>, Env)),
    ?assertEqual(<<"1">>, proplists:get_value(<<"PMI_SIZE">>, Env)).

test_get_pmi_env_all_binaries() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    lists:foreach(fun({K, V}) ->
        ?assert(is_binary(K)),
        ?assert(is_binary(V))
    end, Env).

test_get_pmi_env_no_duplicates() ->
    Env = flurm_pmi_task:get_pmi_env(10, 0, 3, 8),
    Keys = [K || {K, _V} <- Env],
    UniqueKeys = lists:usort(Keys),
    ?assertEqual(length(Keys), length(UniqueKeys)).

test_setup_pmi_different_sizes() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_pmi_listener, start_link, fun(J, S, Sz) ->
        ?assertEqual(10, J),
        ?assertEqual(0, S),
        ?assertEqual(16, Sz),
        {ok, FakePid}
    end),
    {ok, _Pid, _Path} = flurm_pmi_task:setup_pmi(10, 0, 16, <<"node1">>),
    FakePid ! stop.

test_cleanup_pmi_different_job() ->
    ok = flurm_pmi_task:cleanup_pmi(999, 5),
    ?assert(meck:called(flurm_pmi_listener, stop, [999, 5])),
    ?assert(meck:called(flurm_pmi_manager, finalize_job, [999, 5])).

%%====================================================================
%% Hostname Fallback Tests
%%====================================================================

hostname_fallback_test_() ->
    {foreach,
     fun setup_hostname/0,
     fun cleanup_hostname/1,
     [
        {"get_hostname returns localhost on inet error",
         fun test_get_hostname_fallback/0}
     ]}.

setup_hostname() ->
    application:ensure_all_started(sasl),
    catch meck:unload(inet),
    catch meck:unload(flurm_pmi_listener),
    catch meck:unload(flurm_pmi_manager),
    meck:new(inet, [unstick, passthrough]),
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:new(flurm_pmi_manager, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(JobId, StepId) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(JobId) ++ "_" ++ integer_to_list(StepId) ++ ".sock"
    end),
    ok.

cleanup_hostname(_) ->
    catch meck:unload(inet),
    catch meck:unload(flurm_pmi_listener),
    catch meck:unload(flurm_pmi_manager),
    ok.

test_get_hostname_fallback() ->
    %% Mock inet:gethostname to return error
    meck:expect(inet, gethostname, fun() -> {error, nxdomain} end),

    %% Now get_pmi_env should use "localhost" fallback
    Env = flurm_pmi_task:get_pmi_env(10, 0, 0, 4),
    Hostname = proplists:get_value(<<"MPICH_INTERFACE_HOSTNAME">>, Env),
    ?assertEqual(<<"localhost">>, Hostname).
