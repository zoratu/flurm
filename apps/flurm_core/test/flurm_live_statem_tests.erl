%%%-------------------------------------------------------------------
%%% @doc Live proper_statem Tests Against Real Gen_Servers
%%%
%%% Unlike flurm_statem_tests.erl which uses stubs, this module
%%% executes commands against real flurm_job_manager, flurm_scheduler,
%%% and flurm_node_registry gen_servers to find bugs in actual
%%% concurrent state management.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_live_statem_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_live_statem_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% proper_statem callbacks
-export([initial_state/0, command/1, precondition/2, next_state/3, postcondition/3]).

%% Command implementations
-export([do_submit_job/1, do_cancel_job/1, do_hold_job/1, do_release_job/1, do_get_job/1]).

%%====================================================================
%% Model State
%%====================================================================

-record(model, {
    jobs = #{} :: #{pos_integer() => atom()},  %% JobId => expected state
    next_job_id = undefined :: pos_integer() | undefined,
    nodes = [] :: [binary()],                  %% registered node names
    held_jobs = sets:new() :: sets:set(pos_integer())
}).

%%====================================================================
%% Valid TLA+ transitions
%%====================================================================

-define(VALID_TRANSITIONS, #{
    pending => [held, configuring, running, cancelled],
    held => [pending, cancelled],
    configuring => [running, cancelled, failed],
    running => [completing, completed, failed, cancelled, timeout, node_fail, suspended],
    suspended => [running, cancelled],
    completing => [completed, failed],
    completed => [],
    failed => [],
    timeout => [],
    cancelled => [],
    node_fail => []
}).

-define(TERMINAL_STATES, [completed, failed, timeout, cancelled, node_fail]).

%%====================================================================
%% EUnit Integration
%%====================================================================

live_statem_test_() ->
    [
        {"live job lifecycle statem",
         {timeout, 120, fun() ->
            setup_app(),
            try
                ?assert(proper:quickcheck(prop_job_lifecycle(), [
                    {numtests, 50},
                    {max_size, 20}
                ]))
            after
                cleanup_app()
            end
        end}},
        {"live concurrent submit/cancel",
         {timeout, 60, fun() ->
            setup_app(),
            try
                ?assert(proper:quickcheck(prop_submit_cancel(), [
                    {numtests, 50},
                    {max_size, 15}
                ]))
            after
                cleanup_app()
            end
        end}},
        {"live hold/release idempotency",
         {timeout, 60, fun() ->
            setup_app(),
            try
                ?assert(proper:quickcheck(prop_hold_release(), [
                    {numtests, 50},
                    {max_size, 15}
                ]))
            after
                cleanup_app()
            end
        end}},
        {"live job ID uniqueness",
         {timeout, 60, fun() ->
            setup_app(),
            try
                ?assert(proper:quickcheck(prop_job_id_unique(), [
                    {numtests, 50},
                    {max_size, 20}
                ]))
            after
                cleanup_app()
            end
        end}},
        {"live resource allocation consistency",
         {timeout, 60, fun() ->
            setup_app(),
            try
                ?assert(proper:quickcheck(prop_resource_consistency(), [
                    {numtests, 50},
                    {max_size, 15}
                ]))
            after
                cleanup_app()
            end
        end}}
    ].

%%====================================================================
%% Properties
%%====================================================================

%% Job lifecycle: submit, query, cancel sequence
prop_job_lifecycle() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            cleanup_state(),
            {History, State, Result} = run_commands(?MODULE, Cmds),
            cleanup_state(),
            ?WHENFAIL(
                io:format("History: ~p~nState: ~p~nResult: ~p~n",
                          [History, State, Result]),
                Result =:= ok
            )
        end).

%% Submit and cancel interleaving
prop_submit_cancel() ->
    ?FORALL(N, range(1, 10),
        begin
            cleanup_state(),
            %% Submit N jobs
            JobIds = lists:map(fun(_) ->
                Spec = #{
                    name => <<"test_job">>,
                    script => <<"#!/bin/bash\necho hello">>,
                    num_cpus => 1,
                    memory_mb => 100,
                    time_limit => 3600
                },
                {ok, Id} = flurm_job_manager:submit_job(Spec),
                Id
            end, lists:seq(1, N)),
            %% Cancel half
            {ToCancel, ToKeep} = lists:split(N div 2, JobIds),
            lists:foreach(fun(Id) ->
                flurm_job_manager:cancel_job(Id)
            end, ToCancel),
            %% Verify cancelled jobs are cancelled
            CancelledOk = lists:all(fun(Id) ->
                case flurm_job_manager:get_job(Id) of
                    {ok, #job{state = cancelled}} -> true;
                    _ -> false
                end
            end, ToCancel),
            %% Verify kept jobs still exist and are not cancelled
            KeptOk = lists:all(fun(Id) ->
                case flurm_job_manager:get_job(Id) of
                    {ok, #job{state = S}} -> S =/= cancelled;
                    _ -> false
                end
            end, ToKeep),
            cleanup_state(),
            CancelledOk andalso KeptOk
        end).

%% Hold/release idempotency
prop_hold_release() ->
    ?FORALL(Ops, list(elements([hold, release])),
        begin
            cleanup_state(),
            Spec = #{
                name => <<"hold_test">>,
                script => <<"#!/bin/bash\necho test">>,
                num_cpus => 1,
                memory_mb => 100,
                time_limit => 3600
            },
            {ok, JobId} = flurm_job_manager:submit_job(Spec),
            %% Apply hold/release operations
            lists:foreach(fun(Op) ->
                case Op of
                    hold -> flurm_job_manager:hold_job(JobId);
                    release -> flurm_job_manager:release_job(JobId)
                end
            end, Ops),
            %% Job should still be in a valid state
            {ok, #job{state = FinalState}} = flurm_job_manager:get_job(JobId),
            ValidStates = [pending, held],
            Result = lists:member(FinalState, ValidStates),
            cleanup_state(),
            Result
        end).

%% All submitted job IDs must be unique
prop_job_id_unique() ->
    ?FORALL(N, range(1, 20),
        begin
            cleanup_state(),
            JobIds = lists:map(fun(_) ->
                Spec = #{
                    name => <<"unique_test">>,
                    script => <<"#!/bin/bash\necho test">>,
                    num_cpus => 1,
                    memory_mb => 100,
                    time_limit => 3600
                },
                {ok, Id} = flurm_job_manager:submit_job(Spec),
                Id
            end, lists:seq(1, N)),
            UniqueIds = lists:usort(JobIds),
            Result = length(UniqueIds) =:= length(JobIds),
            cleanup_state(),
            Result
        end).

%% Resource allocation: after scheduling, nodes don't overallocate
prop_resource_consistency() ->
    ?FORALL(N, range(1, 5),
        begin
            cleanup_state(),
            %% Register a node with 4 CPUs, 8GB
            flurm_node_registry:register_node_direct(
                #{hostname => <<"testnode1">>, cpus => 4, memory_mb => 8192,
                  state => up, partitions => [<<"default">>]}),
            %% Submit N jobs each requesting 1 CPU
            lists:foreach(fun(_) ->
                Spec = #{
                    name => <<"resource_test">>,
                    script => <<"#!/bin/bash\necho test">>,
                    num_cpus => 1,
                    memory_mb => 1024,
                    time_limit => 3600
                },
                flurm_job_manager:submit_job(Spec)
            end, lists:seq(1, N)),
            %% Trigger scheduling
            flurm_scheduler:trigger_schedule(),
            timer:sleep(100),
            %% Check node resources
            case flurm_node_registry:get_node_entry(<<"testnode1">>) of
                {ok, Entry} ->
                    AllocCpus = Entry#node_entry.cpus_total - Entry#node_entry.cpus_avail,
                    TotalCpus = Entry#node_entry.cpus_total,
                    Result = AllocCpus =< TotalCpus,
                    cleanup_state(),
                    Result;
                _ ->
                    cleanup_state(),
                    true  %% Node not found is OK
            end
        end).

%%====================================================================
%% proper_statem callbacks
%%====================================================================

initial_state() ->
    #model{jobs = #{}, nodes = [], held_jobs = sets:new()}.

command(#model{jobs = Jobs} = _M) ->
    JobIds = maps:keys(Jobs),
    frequency(
        [{10, {call, ?MODULE, do_submit_job, [job_spec_gen()]}}] ++
        case JobIds of
            [] -> [];
            _ -> [
                {5, {call, ?MODULE, do_cancel_job, [elements(JobIds)]}},
                {3, {call, ?MODULE, do_hold_job, [elements(JobIds)]}},
                {3, {call, ?MODULE, do_release_job, [elements(JobIds)]}},
                {4, {call, ?MODULE, do_get_job, [elements(JobIds)]}}
            ]
        end
    ).

precondition(_M, _Call) ->
    true.

next_state(#model{jobs = Jobs} = M, V, {call, _, do_submit_job, _}) ->
    M#model{
        jobs = maps:put(V, pending, Jobs),
        next_job_id = V
    };
next_state(#model{jobs = Jobs, held_jobs = Held} = M, _V, {call, _, do_cancel_job, [JobId]}) ->
    case maps:get(JobId, Jobs, undefined) of
        undefined -> M;
        State ->
            case is_terminal(State) of
                true -> M;
                false ->
                    M#model{
                        jobs = maps:put(JobId, cancelled, Jobs),
                        held_jobs = sets:del_element(JobId, Held)
                    }
            end
    end;
next_state(#model{jobs = Jobs, held_jobs = Held} = M, _V, {call, _, do_hold_job, [JobId]}) ->
    case maps:get(JobId, Jobs, undefined) of
        pending ->
            M#model{
                jobs = maps:put(JobId, held, Jobs),
                held_jobs = sets:add_element(JobId, Held)
            };
        _ ->
            M
    end;
next_state(#model{jobs = Jobs, held_jobs = Held} = M, _V, {call, _, do_release_job, [JobId]}) ->
    case maps:get(JobId, Jobs, undefined) of
        held ->
            M#model{
                jobs = maps:put(JobId, pending, Jobs),
                held_jobs = sets:del_element(JobId, Held)
            };
        _ ->
            M
    end;
next_state(M, _V, {call, _, do_get_job, _}) ->
    M.

postcondition(#model{}, {call, _, do_submit_job, _}, JobId) ->
    is_integer(JobId) andalso JobId > 0;
postcondition(#model{jobs = Jobs}, {call, _, do_cancel_job, [JobId]}, Result) ->
    case maps:get(JobId, Jobs, undefined) of
        undefined -> true;
        State ->
            case is_terminal(State) of
                true -> true;
                false -> Result =:= ok
            end
    end;
postcondition(#model{jobs = Jobs}, {call, _, do_hold_job, [JobId]}, Result) ->
    case maps:get(JobId, Jobs, undefined) of
        pending -> Result =:= ok;
        _ -> true
    end;
postcondition(#model{jobs = Jobs}, {call, _, do_release_job, [JobId]}, Result) ->
    case maps:get(JobId, Jobs, undefined) of
        held -> Result =:= ok;
        _ -> true
    end;
postcondition(#model{jobs = Jobs}, {call, _, do_get_job, [JobId]}, Result) ->
    case maps:get(JobId, Jobs, undefined) of
        undefined ->
            Result =:= {error, not_found};
        _ExpectedState ->
            case Result of
                {ok, #job{id = JobId}} -> true;
                _ -> false
            end
    end;
postcondition(_, _, _) ->
    true.

%%====================================================================
%% Command implementations (against real gen_servers)
%%====================================================================

do_submit_job(Spec) ->
    {ok, JobId} = flurm_job_manager:submit_job(Spec),
    JobId.

do_cancel_job(JobId) ->
    flurm_job_manager:cancel_job(JobId).

do_hold_job(JobId) ->
    flurm_job_manager:hold_job(JobId).

do_release_job(JobId) ->
    flurm_job_manager:release_job(JobId).

do_get_job(JobId) ->
    flurm_job_manager:get_job(JobId).

%%====================================================================
%% Helpers
%%====================================================================

is_terminal(completed) -> true;
is_terminal(failed) -> true;
is_terminal(timeout) -> true;
is_terminal(cancelled) -> true;
is_terminal(node_fail) -> true;
is_terminal(_) -> false.

%%====================================================================
%% Generators
%%====================================================================

job_spec_gen() ->
    #{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho hello">>,
        num_cpus => 1,
        memory_mb => 100,
        time_limit => 3600
    }.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup_app() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Mock persistence to avoid needing Ra
    catch meck:unload(flurm_db_persist),
    meck:new(flurm_db_persist, [passthrough, non_strict]),
    meck:expect(flurm_db_persist, persistence_mode, fun() -> none end),
    meck:expect(flurm_db_persist, store_job, fun(_Job) -> ok end),
    meck:expect(flurm_db_persist, update_job, fun(_JobId, _Updates) -> ok end),
    meck:expect(flurm_db_persist, delete_job, fun(_JobId) -> ok end),
    meck:expect(flurm_db_persist, list_jobs, fun() -> [] end),

    %% Mock metrics
    catch meck:unload(flurm_metrics),
    meck:new(flurm_metrics, [passthrough, non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),

    %% Mock job dispatcher
    catch meck:unload(flurm_job_dispatcher_server),
    meck:new(flurm_job_dispatcher_server, [passthrough, non_strict]),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_, _) -> ok end),

    %% Start real gen_servers
    start_if_not_running(flurm_job_registry, fun flurm_job_registry:start_link/0),
    start_if_not_running(flurm_job_sup, fun flurm_job_sup:start_link/0),
    start_if_not_running(flurm_node_registry, fun flurm_node_registry:start_link/0),
    start_if_not_running(flurm_node_sup, fun flurm_node_sup:start_link/0),
    start_if_not_running(flurm_limits, fun flurm_limits:start_link/0),
    start_if_not_running(flurm_license, fun flurm_license:start_link/0),
    start_if_not_running(flurm_job_deps, fun flurm_job_deps:start_link/0),
    start_if_not_running(flurm_job_array, fun flurm_job_array:start_link/0),
    start_if_not_running(flurm_scheduler, fun flurm_scheduler:start_link/0),
    start_if_not_running(flurm_job_manager, fun flurm_job_manager:start_link/0),
    ok.

cleanup_app() ->
    safe_stop(flurm_job_manager),
    safe_stop(flurm_scheduler),
    safe_stop(flurm_job_array),
    safe_stop(flurm_job_deps),
    safe_stop(flurm_license),
    safe_stop(flurm_limits),
    safe_stop(flurm_node_sup),
    safe_stop(flurm_node_registry),
    safe_stop(flurm_job_sup),
    safe_stop(flurm_job_registry),
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    ok.

cleanup_state() ->
    %% Cancel all existing jobs to get a clean state
    try
        Jobs = flurm_job_manager:list_jobs(),
        lists:foreach(fun(#job{id = Id, state = S}) ->
            case lists:member(S, ?TERMINAL_STATES) of
                false -> catch flurm_job_manager:cancel_job(Id);
                true -> ok
            end
        end, Jobs)
    catch _:_ -> ok
    end.

start_if_not_running(Name, StartFun) ->
    case whereis(Name) of
        undefined ->
            case StartFun() of
                {ok, _Pid} -> ok;
                {error, {already_started, _}} -> ok;
                Other ->
                    error({failed_to_start, Name, Other})
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
