%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_pmi_task module
%%% Coverage target: 100% of all functions and branches
%%%
%%% This file supplements the existing flurm_pmi_task_tests.erl
%%% with additional edge cases and branch coverage.
%%%-------------------------------------------------------------------
-module(flurm_pmi_task_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_pmi_task_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% get_pmi_env tests
      {"get_pmi_env returns 10 variables",
       fun get_pmi_env_count/0},
      {"get_pmi_env all keys are binaries",
       fun get_pmi_env_binary_keys/0},
      {"get_pmi_env all values are binaries",
       fun get_pmi_env_binary_values/0},
      {"get_pmi_env PMI_RANK correct",
       fun get_pmi_env_pmi_rank/0},
      {"get_pmi_env PMI_SIZE correct",
       fun get_pmi_env_pmi_size/0},
      {"get_pmi_env PMI_JOBID format",
       fun get_pmi_env_pmi_jobid/0},
      {"get_pmi_env PMI_SPAWNED is zero",
       fun get_pmi_env_pmi_spawned/0},
      {"get_pmi_env PMI_APPNUM is zero",
       fun get_pmi_env_pmi_appnum/0},
      {"get_pmi_env PMI_SOCK_PATH format",
       fun get_pmi_env_pmi_sock_path/0},
      {"get_pmi_env SLURM_PMI_FD undefined",
       fun get_pmi_env_slurm_pmi_fd/0},
      {"get_pmi_env SLURM_STEP_NODELIST localhost",
       fun get_pmi_env_slurm_step_nodelist/0},
      {"get_pmi_env MPICH_INTERFACE_HOSTNAME present",
       fun get_pmi_env_mpich_hostname/0},
      {"get_pmi_env OMPI_MCA_btl tcp,self",
       fun get_pmi_env_ompi_mca_btl/0},

      %% get_pmi_env edge cases
      {"get_pmi_env rank 0",
       fun get_pmi_env_rank_zero/0},
      {"get_pmi_env large rank",
       fun get_pmi_env_large_rank/0},
      {"get_pmi_env large job id",
       fun get_pmi_env_large_job_id/0},
      {"get_pmi_env large step id",
       fun get_pmi_env_large_step_id/0},
      {"get_pmi_env size 1",
       fun get_pmi_env_size_one/0},
      {"get_pmi_env large size",
       fun get_pmi_env_large_size/0},

      %% setup_pmi tests
      {"setup_pmi starts new listener",
       fun setup_pmi_starts_listener/0},
      {"setup_pmi reuses existing listener",
       fun setup_pmi_reuses_listener/0},
      {"setup_pmi returns error on failure",
       fun setup_pmi_error/0},
      {"setup_pmi returns socket path",
       fun setup_pmi_socket_path/0},

      %% cleanup_pmi tests
      {"cleanup_pmi stops listener",
       fun cleanup_pmi_stops_listener/0},
      {"cleanup_pmi finalizes job",
       fun cleanup_pmi_finalizes_job/0},
      {"cleanup_pmi returns ok",
       fun cleanup_pmi_returns_ok/0},

      %% get_hostname internal function coverage
      {"get_hostname returns binary",
       fun get_hostname_binary/0}
     ]}.

%%%===================================================================
%%% Setup / Cleanup
%%%===================================================================

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
    %% Default mocks
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

%%%===================================================================
%%% get_pmi_env Tests
%%%===================================================================

get_pmi_env_count() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
    ?assertEqual(10, length(Env)).

get_pmi_env_binary_keys() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
    lists:foreach(fun({K, _V}) ->
        ?assert(is_binary(K))
    end, Env).

get_pmi_env_binary_values() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
    lists:foreach(fun({_K, V}) ->
        ?assert(is_binary(V))
    end, Env).

get_pmi_env_pmi_rank() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 5, 10),
    ?assertEqual(<<"5">>, proplists:get_value(<<"PMI_RANK">>, Env)).

get_pmi_env_pmi_size() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 16),
    ?assertEqual(<<"16">>, proplists:get_value(<<"PMI_SIZE">>, Env)).

get_pmi_env_pmi_jobid() ->
    Env = flurm_pmi_task:get_pmi_env(42, 3, 0, 1),
    ?assertEqual(<<"42_3">>, proplists:get_value(<<"PMI_JOBID">>, Env)).

get_pmi_env_pmi_spawned() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_SPAWNED">>, Env)).

get_pmi_env_pmi_appnum() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_APPNUM">>, Env)).

get_pmi_env_pmi_sock_path() ->
    Env = flurm_pmi_task:get_pmi_env(123, 456, 0, 1),
    Path = proplists:get_value(<<"PMI_SOCK_PATH">>, Env),
    ?assertNotEqual(undefined, Path),
    ?assertNotEqual(nomatch, binary:match(Path, <<"123_456">>)).

get_pmi_env_slurm_pmi_fd() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"undefined">>, proplists:get_value(<<"SLURM_PMI_FD">>, Env)).

get_pmi_env_slurm_step_nodelist() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"localhost">>, proplists:get_value(<<"SLURM_STEP_NODELIST">>, Env)).

get_pmi_env_mpich_hostname() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    Hostname = proplists:get_value(<<"MPICH_INTERFACE_HOSTNAME">>, Env),
    ?assertNotEqual(undefined, Hostname),
    ?assert(is_binary(Hostname)).

get_pmi_env_ompi_mca_btl() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"tcp,self">>, proplists:get_value(<<"OMPI_MCA_btl">>, Env)).

%%%===================================================================
%%% get_pmi_env Edge Cases
%%%===================================================================

get_pmi_env_rank_zero() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 8),
    ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_RANK">>, Env)).

get_pmi_env_large_rank() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 9999, 10000),
    ?assertEqual(<<"9999">>, proplists:get_value(<<"PMI_RANK">>, Env)).

get_pmi_env_large_job_id() ->
    Env = flurm_pmi_task:get_pmi_env(999999, 0, 0, 1),
    JobId = proplists:get_value(<<"PMI_JOBID">>, Env),
    ?assertNotEqual(nomatch, binary:match(JobId, <<"999999">>)).

get_pmi_env_large_step_id() ->
    Env = flurm_pmi_task:get_pmi_env(1, 9999, 0, 1),
    JobId = proplists:get_value(<<"PMI_JOBID">>, Env),
    ?assertEqual(<<"1_9999">>, JobId).

get_pmi_env_size_one() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    ?assertEqual(<<"1">>, proplists:get_value(<<"PMI_SIZE">>, Env)).

get_pmi_env_large_size() ->
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 100000),
    ?assertEqual(<<"100000">>, proplists:get_value(<<"PMI_SIZE">>, Env)).

%%%===================================================================
%%% setup_pmi Tests
%%%===================================================================

setup_pmi_starts_listener() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_pmi_listener, start_link, fun(JobId, StepId, Size) ->
        ?assertEqual(10, JobId),
        ?assertEqual(0, StepId),
        ?assertEqual(4, Size),
        {ok, FakePid}
    end),
    Result = flurm_pmi_task:setup_pmi(10, 0, 4, <<"node1">>),
    ?assertMatch({ok, _, _}, Result),
    {ok, Pid, _Path} = Result,
    ?assertEqual(FakePid, Pid),
    FakePid ! stop.

setup_pmi_reuses_listener() ->
    %% Register a fake listener process
    FakePid = spawn(fun() -> receive stop -> ok end end),
    ListenerName = flurm_pmi_listener_10_0,
    register(ListenerName, FakePid),

    %% start_link should NOT be called
    meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, _Sz) ->
        error(should_not_be_called)
    end),

    Result = flurm_pmi_task:setup_pmi(10, 0, 4, <<"node1">>),
    ?assertMatch({ok, _, _}, Result),
    {ok, Pid, _Path} = Result,
    ?assertEqual(FakePid, Pid),
    ?assertEqual(0, meck:num_calls(flurm_pmi_listener, start_link, '_')),

    FakePid ! stop,
    timer:sleep(50).

setup_pmi_error() ->
    meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, _Sz) ->
        {error, eaddrinuse}
    end),
    Result = flurm_pmi_task:setup_pmi(10, 0, 4, <<"node1">>),
    ?assertEqual({error, eaddrinuse}, Result).

setup_pmi_socket_path() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, _Sz) ->
        {ok, FakePid}
    end),
    {ok, _Pid, Path} = flurm_pmi_task:setup_pmi(77, 5, 8, <<"mynode">>),
    ?assert(is_list(Path)),
    ?assertNotEqual([], Path),
    %% Path should contain job and step IDs
    ?assertNotEqual(nomatch, string:find(Path, "77")),
    ?assertNotEqual(nomatch, string:find(Path, "5")),
    FakePid ! stop.

%%%===================================================================
%%% cleanup_pmi Tests
%%%===================================================================

cleanup_pmi_stops_listener() ->
    ok = flurm_pmi_task:cleanup_pmi(10, 0),
    ?assert(meck:called(flurm_pmi_listener, stop, [10, 0])).

cleanup_pmi_finalizes_job() ->
    ok = flurm_pmi_task:cleanup_pmi(10, 0),
    ?assert(meck:called(flurm_pmi_manager, finalize_job, [10, 0])).

cleanup_pmi_returns_ok() ->
    Result = flurm_pmi_task:cleanup_pmi(10, 0),
    ?assertEqual(ok, Result).

%%%===================================================================
%%% get_hostname Internal Function Coverage
%%%===================================================================

get_hostname_binary() ->
    %% get_hostname is internal but called via get_pmi_env
    Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
    Hostname = proplists:get_value(<<"MPICH_INTERFACE_HOSTNAME">>, Env),
    ?assert(is_binary(Hostname)),
    ?assert(byte_size(Hostname) > 0).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Verify no duplicate keys in environment
no_duplicate_keys_test_() ->
    {"get_pmi_env has no duplicate keys",
     fun() ->
         Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
         Keys = [K || {K, _V} <- Env],
         UniqueKeys = lists:usort(Keys),
         ?assertEqual(length(Keys), length(UniqueKeys))
     end}.

%% Verify different jobs get different environment
different_jobs_different_env_test_() ->
    {"different jobs get different PMI_JOBID",
     fun() ->
         meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
             "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
         end),
         Env1 = flurm_pmi_task:get_pmi_env(100, 0, 0, 4),
         Env2 = flurm_pmi_task:get_pmi_env(200, 0, 0, 4),
         JobId1 = proplists:get_value(<<"PMI_JOBID">>, Env1),
         JobId2 = proplists:get_value(<<"PMI_JOBID">>, Env2),
         ?assertNotEqual(JobId1, JobId2)
     end}.

%% Verify different steps get different environment
different_steps_different_env_test_() ->
    {"different steps get different PMI_JOBID",
     fun() ->
         meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
             "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
         end),
         Env1 = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
         Env2 = flurm_pmi_task:get_pmi_env(1, 1, 0, 4),
         JobId1 = proplists:get_value(<<"PMI_JOBID">>, Env1),
         JobId2 = proplists:get_value(<<"PMI_JOBID">>, Env2),
         ?assertNotEqual(JobId1, JobId2)
     end}.

%% Verify socket paths differ per job/step
socket_path_differs_test_() ->
    {"different jobs get different socket paths",
     fun() ->
         meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
             "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
         end),
         Env1 = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
         Env2 = flurm_pmi_task:get_pmi_env(2, 0, 0, 1),
         Path1 = proplists:get_value(<<"PMI_SOCK_PATH">>, Env1),
         Path2 = proplists:get_value(<<"PMI_SOCK_PATH">>, Env2),
         ?assertNotEqual(Path1, Path2)
     end}.

%% Test cleanup with various job/step IDs
cleanup_various_ids_test_() ->
    {"cleanup works with various job/step IDs",
     {foreach,
      fun setup/0,
      fun cleanup/1,
      [
       fun() ->
           ok = flurm_pmi_task:cleanup_pmi(1, 0),
           ?assert(meck:called(flurm_pmi_listener, stop, [1, 0]))
       end,
       fun() ->
           ok = flurm_pmi_task:cleanup_pmi(999, 999),
           ?assert(meck:called(flurm_pmi_listener, stop, [999, 999]))
       end,
       fun() ->
           ok = flurm_pmi_task:cleanup_pmi(1000000, 0),
           ?assert(meck:called(flurm_pmi_listener, stop, [1000000, 0]))
       end
      ]}}.

%% Test setup with various sizes
setup_various_sizes_test_() ->
    {"setup works with various sizes",
     {foreach,
      fun setup/0,
      fun cleanup/1,
      [
       fun() ->
           FakePid = spawn(fun() -> receive stop -> ok end end),
           meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, Sz) ->
               ?assertEqual(1, Sz),
               {ok, FakePid}
           end),
           {ok, _, _} = flurm_pmi_task:setup_pmi(1, 0, 1, <<"n">>),
           FakePid ! stop
       end,
       fun() ->
           FakePid = spawn(fun() -> receive stop -> ok end end),
           meck:expect(flurm_pmi_listener, start_link, fun(_J, _S, Sz) ->
               ?assertEqual(1000, Sz),
               {ok, FakePid}
           end),
           {ok, _, _} = flurm_pmi_task:setup_pmi(1, 0, 1000, <<"n">>),
           FakePid ! stop
       end
      ]}}.

%%%===================================================================
%%% Additional Tests for Complete Coverage
%%% These tests use standalone test functions (not foreach) for simpler testing
%%%===================================================================

%% Test all PMI environment variables are present - standalone test
all_env_vars_present_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
        Keys = [K || {K, _} <- Env],
        ?assert(lists:member(<<"PMI_RANK">>, Keys)),
        ?assert(lists:member(<<"PMI_SIZE">>, Keys)),
        ?assert(lists:member(<<"PMI_JOBID">>, Keys)),
        ?assert(lists:member(<<"PMI_SPAWNED">>, Keys)),
        ?assert(lists:member(<<"PMI_APPNUM">>, Keys)),
        ?assert(lists:member(<<"PMI_SOCK_PATH">>, Keys)),
        ?assert(lists:member(<<"SLURM_PMI_FD">>, Keys)),
        ?assert(lists:member(<<"SLURM_STEP_NODELIST">>, Keys)),
        ?assert(lists:member(<<"MPICH_INTERFACE_HOSTNAME">>, Keys)),
        ?assert(lists:member(<<"OMPI_MCA_btl">>, Keys))
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test environment variable values are correct types
env_var_types_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
        lists:foreach(fun({K, V}) ->
            ?assert(is_binary(K)),
            ?assert(is_binary(V))
        end, Env)
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test PMI_RANK values for various ranks
rank_values_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        lists:foreach(fun(Rank) ->
            Env = flurm_pmi_task:get_pmi_env(1, 0, Rank, 100),
            Expected = integer_to_binary(Rank),
            ?assertEqual(Expected, proplists:get_value(<<"PMI_RANK">>, Env))
        end, [0, 1, 50, 99])
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test PMI_SIZE values for various sizes
size_values_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        lists:foreach(fun(Size) ->
            Env = flurm_pmi_task:get_pmi_env(1, 0, 0, Size),
            Expected = integer_to_binary(Size),
            ?assertEqual(Expected, proplists:get_value(<<"PMI_SIZE">>, Env))
        end, [1, 2, 16, 256, 10000])
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test PMI_JOBID format for various jobs/steps
jobid_format_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Cases = [{1, 0, <<"1_0">>},
                 {42, 3, <<"42_3">>},
                 {1000, 10, <<"1000_10">>},
                 {999999, 0, <<"999999_0">>}],
        lists:foreach(fun({JobId, StepId, Expected}) ->
            Env = flurm_pmi_task:get_pmi_env(JobId, StepId, 0, 1),
            ?assertEqual(Expected, proplists:get_value(<<"PMI_JOBID">>, Env))
        end, Cases)
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test socket path format for various jobs/steps
socket_path_format_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    try
        Cases = [{1, 0}, {42, 3}, {1000, 10}, {999999, 0}],
        lists:foreach(fun({JobId, StepId}) ->
            meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
                "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
            end),
            Env = flurm_pmi_task:get_pmi_env(JobId, StepId, 0, 1),
            Path = proplists:get_value(<<"PMI_SOCK_PATH">>, Env),
            ?assert(is_binary(Path)),
            ?assert(byte_size(Path) > 0)
        end, Cases)
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test static environment variables are correct
static_env_vars_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
        ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_SPAWNED">>, Env)),
        ?assertEqual(<<"0">>, proplists:get_value(<<"PMI_APPNUM">>, Env)),
        ?assertEqual(<<"undefined">>, proplists:get_value(<<"SLURM_PMI_FD">>, Env)),
        ?assertEqual(<<"localhost">>, proplists:get_value(<<"SLURM_STEP_NODELIST">>, Env)),
        ?assertEqual(<<"tcp,self">>, proplists:get_value(<<"OMPI_MCA_btl">>, Env))
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test get_pmi_env consistency
get_pmi_env_consistency_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Env1 = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
        Env2 = flurm_pmi_task:get_pmi_env(1, 0, 0, 4),
        ?assertEqual(Env1, Env2)
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test MPICH_INTERFACE_HOSTNAME is set
mpich_hostname_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
        Hostname = proplists:get_value(<<"MPICH_INTERFACE_HOSTNAME">>, Env),
        ?assert(is_binary(Hostname)),
        ?assert(byte_size(Hostname) > 0)
    after
        meck:unload(flurm_pmi_listener)
    end.

%% Test get_hostname internal function coverage via get_pmi_env
get_hostname_coverage_standalone_test() ->
    meck:new(flurm_pmi_listener, [non_strict]),
    meck:expect(flurm_pmi_listener, get_socket_path, fun(J, S) ->
        "/tmp/flurm_pmi_" ++ integer_to_list(J) ++ "_" ++ integer_to_list(S) ++ ".sock"
    end),
    try
        Env = flurm_pmi_task:get_pmi_env(1, 0, 0, 1),
        Hostname = proplists:get_value(<<"MPICH_INTERFACE_HOSTNAME">>, Env),
        ?assert(is_binary(Hostname))
    after
        meck:unload(flurm_pmi_listener)
    end.
