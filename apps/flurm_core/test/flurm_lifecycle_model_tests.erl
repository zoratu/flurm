%%%-------------------------------------------------------------------
%%% @doc Deterministic lifecycle model tests against live gen_servers.
%%%
%%% These tests replay fixed operation traces and validate model invariants
%%% after every step. This complements randomized PropEr runs with
%%% reproducible stateful checks suitable for CI gating.
%%%-------------------------------------------------------------------
-module(flurm_lifecycle_model_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

-record(model, {
    refs = #{} :: #{pos_integer() => pos_integer()},
    states = #{} :: #{pos_integer() => atom()}
}).

-define(VALID_STATES, [pending, held, configuring, running, suspended,
                       completing, completed, failed, timeout, cancelled, node_fail]).
-define(TERMINAL_STATES, [completed, failed, timeout, cancelled, node_fail]).

lifecycle_model_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"deterministic lifecycle trace", fun deterministic_lifecycle_trace_case/0},
      {"deterministic interleaved trace", fun deterministic_interleaved_trace_case/0},
      {"deterministic ID uniqueness trace", fun deterministic_id_uniqueness_trace_case/0}
     ]}.

deterministic_lifecycle_trace_case() ->
    Ops = [
        {submit, 1},
        {get, 1},
        {hold, 1},
        {get, 1},
        {release, 1},
        {cancel, 1},
        {cancel, 1},
        {get, 1}
    ],
    Final = replay(Ops),
    JobId = resolve_ref(Final, 1),
    ?assertEqual(cancelled, maps:get(JobId, Final#model.states)).

deterministic_interleaved_trace_case() ->
    Ops = [
        {submit, 1},
        {submit, 2},
        {submit, 3},
        {hold, 1},
        {cancel, 2},
        {release, 1},
        {get, 1},
        {get, 2},
        {cancel, 3},
        {get, 3}
    ],
    Final = replay(Ops),
    Id1 = resolve_ref(Final, 1),
    Id2 = resolve_ref(Final, 2),
    Id3 = resolve_ref(Final, 3),
    ?assertEqual(pending, maps:get(Id1, Final#model.states)),
    ?assertEqual(cancelled, maps:get(Id2, Final#model.states)),
    ?assertEqual(cancelled, maps:get(Id3, Final#model.states)).

deterministic_id_uniqueness_trace_case() ->
    Ops = [{submit, N} || N <- lists:seq(1, 25)],
    Final = replay(Ops),
    JobIds = maps:values(Final#model.refs),
    ?assertEqual(length(JobIds), length(lists:usort(JobIds))),
    ?assert(lists:all(fun(Id) -> is_integer(Id) andalso Id > 0 end, JobIds)).

replay(Ops) ->
    lists:foldl(fun apply_op/2, #model{}, Ops).

apply_op({submit, Ref}, #model{refs = Refs, states = States} = Model) ->
    Spec = #{
        name => list_to_binary(io_lib:format("det_model_~B", [Ref])),
        script => <<"#!/bin/bash\necho model">>,
        num_cpus => 1,
        memory_mb => 64,
        time_limit => 3600
    },
    {ok, JobId} = flurm_job_manager:submit_job(Spec),
    Next = Model#model{
        refs = maps:put(Ref, JobId, Refs),
        states = maps:put(JobId, pending, States)
    },
    assert_job_matches_model(JobId, pending),
    assert_invariants(Next),
    Next;
apply_op({hold, Ref}, Model) ->
    JobId = resolve_ref(Model, Ref),
    Result = flurm_job_manager:hold_job(JobId),
    NextState = case {maps:get(JobId, Model#model.states), Result} of
        {pending, ok} -> held;
        {S, _} -> S
    end,
    Next = put_state(Model, JobId, NextState),
    assert_job_matches_model(JobId, NextState),
    assert_invariants(Next),
    Next;
apply_op({release, Ref}, Model) ->
    JobId = resolve_ref(Model, Ref),
    Result = flurm_job_manager:release_job(JobId),
    NextState = case {maps:get(JobId, Model#model.states), Result} of
        {held, ok} -> pending;
        {S, _} -> S
    end,
    Next = put_state(Model, JobId, NextState),
    assert_job_matches_model(JobId, NextState),
    assert_invariants(Next),
    Next;
apply_op({cancel, Ref}, Model) ->
    JobId = resolve_ref(Model, Ref),
    _ = flurm_job_manager:cancel_job(JobId),
    Current = maps:get(JobId, Model#model.states),
    NextState = case lists:member(Current, ?TERMINAL_STATES) of
        true -> Current;
        false -> cancelled
    end,
    Next = put_state(Model, JobId, NextState),
    assert_job_matches_model(JobId, NextState),
    assert_invariants(Next),
    Next;
apply_op({get, Ref}, Model) ->
    JobId = resolve_ref(Model, Ref),
    {ok, #job{id = JobId, state = State}} = flurm_job_manager:get_job(JobId),
    Expected = maps:get(JobId, Model#model.states),
    ?assertEqual(Expected, State),
    assert_invariants(Model),
    Model.

resolve_ref(#model{refs = Refs}, Ref) ->
    case maps:get(Ref, Refs, undefined) of
        undefined -> error({unknown_ref, Ref});
        JobId -> JobId
    end.

put_state(#model{states = States} = Model, JobId, NextState) ->
    Model#model{states = maps:put(JobId, NextState, States)}.

assert_invariants(#model{refs = Refs, states = States}) ->
    JobIds = maps:values(Refs),
    ?assertEqual(length(JobIds), length(lists:usort(JobIds))),
    maps:foreach(
        fun(_Id, State) ->
            ?assert(lists:member(State, ?VALID_STATES))
        end,
        States
    ).

assert_job_matches_model(JobId, ExpectedState) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, #job{id = JobId, state = ActualState}} ->
            ?assertEqual(ExpectedState, ActualState);
        Other ->
            ?assertMatch({ok, _}, Other)
    end.

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    catch meck:unload(flurm_db_persist),
    meck:new(flurm_db_persist, [passthrough, non_strict]),
    meck:expect(flurm_db_persist, persistence_mode, fun() -> none end),
    meck:expect(flurm_db_persist, store_job, fun(_Job) -> ok end),
    meck:expect(flurm_db_persist, update_job, fun(_JobId, _Updates) -> ok end),
    meck:expect(flurm_db_persist, delete_job, fun(_JobId) -> ok end),
    meck:expect(flurm_db_persist, list_jobs, fun() -> [] end),

    catch meck:unload(flurm_metrics),
    meck:new(flurm_metrics, [passthrough, non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),

    catch meck:unload(flurm_job_dispatcher_server),
    meck:new(flurm_job_dispatcher_server, [passthrough, non_strict]),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_, _) -> ok end),

    start_if_not_running(flurm_job_registry, fun flurm_job_registry:start_link/0),
    start_if_not_running(flurm_job_sup, fun flurm_job_sup:start_link/0),
    start_if_not_running(flurm_limits, fun flurm_limits:start_link/0),
    start_if_not_running(flurm_license, fun flurm_license:start_link/0),
    start_if_not_running(flurm_job_deps, fun flurm_job_deps:start_link/0),
    start_if_not_running(flurm_job_array, fun flurm_job_array:start_link/0),
    start_if_not_running(flurm_job_manager, fun flurm_job_manager:start_link/0),
    ok.

cleanup(_) ->
    safe_stop(flurm_job_manager),
    safe_stop(flurm_job_array),
    safe_stop(flurm_job_deps),
    safe_stop(flurm_license),
    safe_stop(flurm_limits),
    safe_stop(flurm_job_sup),
    safe_stop(flurm_job_registry),
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    ok.

start_if_not_running(Name, StartFun) ->
    case whereis(Name) of
        undefined ->
            case StartFun() of
                {ok, _Pid} -> ok;
                {error, {already_started, _}} -> ok;
                Other -> error({failed_to_start, Name, Other})
            end;
        _Pid -> ok
    end.

safe_stop(Name) ->
    case whereis(Name) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, normal, 5000),
            ok
    end.
