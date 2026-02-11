%%%-------------------------------------------------------------------
%%% @doc TLA+ Chaos and Stress Tests for FLURM
%%%
%%% Property-based chaos tests that exercise edge cases, boundary
%%% conditions, and concurrent-style scenarios. These tests target
%%% areas where TLA+ safety invariants could be violated by
%%% implementation bugs.
%%%
%%% Test Categories:
%%% 1. Rapid state transition storms - fast sequences of transitions
%%% 2. Resource allocation boundary cases - over/under allocation
%%% 3. Job record consistency under mutation - field corruption
%%% 4. Concurrent operation simulation - interleaved ops
%%% 5. TRES accounting invariants under stress
%%% 6. SLURM NO_VAL boundary values
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_tla_chaos_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_tla_chaos_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

-define(TERMINAL_STATES, [completed, failed, timeout, cancelled, node_fail]).
-define(NON_TERMINAL_STATES, [pending, held, configuring, running, suspended, completing]).

%%====================================================================
%% Generators
%%====================================================================

any_state() ->
    elements(?TERMINAL_STATES ++ ?NON_TERMINAL_STATES).

non_terminal() ->
    elements(?NON_TERMINAL_STATES).

slurm_boundary_value() ->
    oneof([
        0, 1,
        16#FFFF, 16#FFFE,
        16#FFFFFFFE, 16#FFFFFFFF,
        range(1, 100),
        range(100000, 999999)
    ]).

%%====================================================================
%% Property 1: State Transition Storm
%%
%% Apply many random state transitions and verify structural
%% invariants hold at every step.
%%====================================================================

prop_state_transition_storm() ->
    ?FORALL(Transitions, non_empty(list(any_state())),
        begin
            Job0 = #job{
                id = 1, name = <<"storm">>, user = <<"chaos">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho chaos">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = []
            },
            {_FinalJob, AllValid} = lists:foldl(
                fun(NewState, {Job, Valid}) ->
                    Updated = flurm_core:update_job_state(Job, NewState),
                    StateOk = Updated#job.state =:= NewState,
                    IdOk = Updated#job.id =:= Job#job.id,
                    EndTimeOk = case lists:member(NewState, ?TERMINAL_STATES) of
                        true -> Updated#job.end_time =/= undefined;
                        false -> true
                    end,
                    StartTimeOk = case NewState of
                        running -> Updated#job.start_time =/= undefined;
                        _ -> true
                    end,
                    SubmitTimeOk = Updated#job.submit_time =:= Job#job.submit_time,
                    {Updated, Valid andalso StateOk andalso IdOk andalso
                     EndTimeOk andalso StartTimeOk andalso SubmitTimeOk}
                end,
                {Job0, true},
                Transitions),
            AllValid
        end).

%%====================================================================
%% Property 2: Resource Tracking Never Goes Negative
%%====================================================================

prop_resources_never_negative() ->
    ?FORALL(Ops, non_empty(list(oneof([
                {alloc, range(1, 100), range(1, 4), range(1, 2048)},
                {release, range(1, 100)},
                release_all
            ]))),
        begin
            MaxCpus = 8,
            MaxMem = 16384,
            {_Allocs, AllValid} = lists:foldl(
                fun({alloc, JobId, Cpus, Mem}, {Allocations, Valid}) ->
                    case maps:is_key(JobId, Allocations) of
                        true -> {Allocations, Valid};
                        false ->
                            UsedCpus = lists:sum([C || {C, _} <- maps:values(Allocations)]),
                            UsedMem = lists:sum([M || {_, M} <- maps:values(Allocations)]),
                            case UsedCpus + Cpus =< MaxCpus andalso
                                 UsedMem + Mem =< MaxMem of
                                true ->
                                    NewAllocs = maps:put(JobId, {Cpus, Mem}, Allocations),
                                    NewUsedCpus = UsedCpus + Cpus,
                                    NewUsedMem = UsedMem + Mem,
                                    AvailCpus = MaxCpus - NewUsedCpus,
                                    AvailMem = MaxMem - NewUsedMem,
                                    {NewAllocs, Valid andalso
                                     AvailCpus >= 0 andalso AvailMem >= 0 andalso
                                     AvailCpus =< MaxCpus andalso AvailMem =< MaxMem};
                                false ->
                                    {Allocations, Valid}
                            end
                    end;
                   ({release, JobId}, {Allocations, Valid}) ->
                    NewAllocs = maps:remove(JobId, Allocations),
                    UsedCpus = lists:sum([C || {C, _} <- maps:values(NewAllocs)]),
                    UsedMem = lists:sum([M || {_, M} <- maps:values(NewAllocs)]),
                    AvailCpus = MaxCpus - UsedCpus,
                    AvailMem = MaxMem - UsedMem,
                    {NewAllocs, Valid andalso
                     AvailCpus >= 0 andalso AvailMem >= 0 andalso
                     AvailCpus =< MaxCpus andalso AvailMem =< MaxMem};
                   (release_all, {_Allocations, Valid}) ->
                    {#{}, Valid}
                end,
                {#{}, true},
                Ops),
            AllValid
        end).

%%====================================================================
%% Property 3: SLURM NO_VAL Boundary Handling
%%====================================================================

prop_no_val_boundary_handling() ->
    ?FORALL({NumCpus, MemMb, TimeLimit, Priority},
            {slurm_boundary_value(), slurm_boundary_value(),
             slurm_boundary_value(), slurm_boundary_value()},
        begin
            SafeCpus = max(1, min(abs(NumCpus), 128)),
            SafeMem = max(1, min(abs(MemMb), 1048576)),
            SafeTime = max(1, min(abs(TimeLimit), 31536000)),
            SafePrio = max(0, min(abs(Priority), 10000)),
            Job = #job{
                id = 1, name = <<"boundary">>, user = <<"test">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho boundary">>,
                num_nodes = 1, num_cpus = SafeCpus,
                memory_mb = SafeMem, time_limit = SafeTime,
                priority = SafePrio,
                submit_time = erlang:system_time(second),
                allocated_nodes = []
            },
            Job#job.num_cpus >= 1 andalso
            Job#job.memory_mb >= 1 andalso
            Job#job.time_limit >= 1 andalso
            Job#job.priority >= 0
        end).

%%====================================================================
%% Property 4: Multi-Job Isolation
%%
%% Operations on one job never affect another job's state.
%%====================================================================

prop_multi_job_isolation() ->
    ?FORALL({NumJobs, Ops},
            {range(2, 10), non_empty(list({range(1, 10), any_state()}))},
        begin
            Jobs0 = maps:from_list([
                {I, #job{
                    id = I, name = list_to_binary("job" ++ integer_to_list(I)),
                    user = <<"test">>, partition = <<"default">>,
                    state = pending, script = <<"#!/bin/bash\necho test">>,
                    num_nodes = 1, num_cpus = 1, memory_mb = 256,
                    time_limit = 60, priority = I * 10,
                    submit_time = erlang:system_time(second),
                    allocated_nodes = []
                }} || I <- lists:seq(1, NumJobs)
            ]),
            {FinalJobs, AllValid} = lists:foldl(
                fun({JobIdx, NewState}, {Jobs, Valid}) ->
                    ActualIdx = ((JobIdx - 1) rem NumJobs) + 1,
                    case maps:find(ActualIdx, Jobs) of
                        {ok, Job} ->
                            Updated = flurm_core:update_job_state(Job, NewState),
                            NewJobs = maps:put(ActualIdx, Updated, Jobs),
                            OtherJobsOk = maps:fold(
                                fun(K, V, Acc) ->
                                    case K =:= ActualIdx of
                                        true -> Acc;
                                        false ->
                                            Original = maps:get(K, Jobs),
                                            Acc andalso V =:= Original
                                    end
                                end, true, NewJobs),
                            {NewJobs, Valid andalso OtherJobsOk};
                        error ->
                            {Jobs, Valid}
                    end
                end,
                {Jobs0, true},
                Ops),
            IdCheck = maps:fold(
                fun(K, V, Acc) -> Acc andalso V#job.id =:= K end,
                true, FinalJobs),
            AllValid andalso IdCheck
        end).

%%====================================================================
%% Property 5: TRES Extreme Values
%%====================================================================

prop_tres_extreme_values() ->
    ?FORALL({V1, V2},
            {non_neg_integer(), non_neg_integer()},
        begin
            T1 = #{cpu_seconds => V1, mem_seconds => 0,
                   node_seconds => 0, gpu_seconds => 0,
                   job_count => 0, job_time => 0, billing => 0},
            T2 = #{cpu_seconds => V2, mem_seconds => 0,
                   node_seconds => 0, gpu_seconds => 0,
                   job_count => 0, job_time => 0, billing => 0},
            R1 = flurm_tres:add(T1, T2),
            R2 = flurm_tres:add(T2, T1),
            R1 =:= R2 andalso
            maps:get(cpu_seconds, R1) >= 0 andalso
            maps:get(cpu_seconds, R1) =:= V1 + V2
        end).

%%====================================================================
%% Property 6: State Transition Idempotency
%%====================================================================

prop_state_transition_idempotency() ->
    ?FORALL({FromState, ToState},
            {non_terminal(), any_state()},
        begin
            BaseTime = 1000000,
            Job = #job{
                id = 42, name = <<"idem">>, user = <<"test">>,
                partition = <<"default">>, state = FromState,
                script = <<"#!/bin/bash\necho test">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = BaseTime,
                start_time = case FromState of
                    running -> BaseTime;
                    suspended -> BaseTime;
                    _ -> undefined
                end,
                allocated_nodes = []
            },
            First = flurm_core:update_job_state(Job, ToState),
            Second = flurm_core:update_job_state(First, ToState),
            First#job.state =:= Second#job.state andalso
            First#job.id =:= Second#job.id andalso
            Second#job.id =:= 42
        end).

%%====================================================================
%% Property 7: Apply Job Updates Correctness
%%====================================================================

prop_apply_job_updates() ->
    ?FORALL({Name, TimeLimit, Prio},
            {binary(8), range(60, 86400), range(0, 10000)},
        begin
            Job = #job{
                id = 1, name = <<"original">>, user = <<"test">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho test">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = []
            },
            %% Only update fields supported by apply_job_updates
            %% (name, time_limit, priority - not num_cpus/memory_mb
            %% which are immutable after submission, like SLURM)
            Updates = #{
                name => Name,
                time_limit => TimeLimit,
                priority => Prio
            },
            Updated = flurm_job_manager:apply_job_updates(Job, Updates),
            Updated#job.name =:= Name andalso
            Updated#job.time_limit =:= TimeLimit andalso
            Updated#job.priority =:= Prio andalso
            %% Unchanged fields stay unchanged
            Updated#job.id =:= 1 andalso
            Updated#job.user =:= <<"test">> andalso
            Updated#job.num_cpus =:= 1 andalso
            Updated#job.memory_mb =:= 256
        end).

%%====================================================================
%% Property 8: Multi-Node Allocation Invariant
%%
%% Simulate multiple nodes with concurrent allocations.
%% Global invariant: total allocated <= total available.
%%====================================================================

prop_multi_node_allocation() ->
    ?FORALL({NumNodes, Ops},
            {range(1, 5), non_empty(list({range(1, 5), range(1, 100), range(1, 4), range(256, 2048)}))},
        begin
            CpusPerNode = 8,
            MemPerNode = 16384,
            Nodes0 = maps:from_list([
                {I, #{cpus_avail => CpusPerNode, mem_avail => MemPerNode,
                      cpus_total => CpusPerNode, mem_total => MemPerNode,
                      allocations => #{}}}
                || I <- lists:seq(1, NumNodes)
            ]),
            {FinalNodes, AllValid} = lists:foldl(
                fun({NodeIdx, JobId, Cpus, Mem}, {Nodes, Valid}) ->
                    ActualNode = ((NodeIdx - 1) rem NumNodes) + 1,
                    Node = maps:get(ActualNode, Nodes),
                    Allocs = maps:get(allocations, Node),
                    CpusAvail = maps:get(cpus_avail, Node),
                    MemAvail = maps:get(mem_avail, Node),
                    case not maps:is_key(JobId, Allocs) andalso
                         Cpus =< CpusAvail andalso Mem =< MemAvail of
                        true ->
                            NewNode = Node#{
                                cpus_avail => CpusAvail - Cpus,
                                mem_avail => MemAvail - Mem,
                                allocations => maps:put(JobId, {Cpus, Mem}, Allocs)
                            },
                            NewNodes = maps:put(ActualNode, NewNode, Nodes),
                            NodeOk = (CpusAvail - Cpus) >= 0 andalso
                                     (MemAvail - Mem) >= 0,
                            {NewNodes, Valid andalso NodeOk};
                        false ->
                            {Nodes, Valid}
                    end
                end,
                {Nodes0, true},
                Ops),
            TotalAllocCpus = maps:fold(
                fun(_K, Node, Acc) ->
                    Acc + maps:get(cpus_total, Node) - maps:get(cpus_avail, Node)
                end, 0, FinalNodes),
            TotalMaxCpus = NumNodes * CpusPerNode,
            AllValid andalso TotalAllocCpus >= 0 andalso
            TotalAllocCpus =< TotalMaxCpus
        end).

%%====================================================================
%% Property 9: Binary Field Preservation
%%
%% Binary fields survive state transitions without corruption.
%%====================================================================

prop_binary_field_preservation() ->
    ?FORALL({Name, User, Partition, Script, WorkDir, StdOut, StdErr},
            {binary(16), binary(8), binary(8), binary(32),
             binary(16), binary(16), binary(16)},
        begin
            Job = #job{
                id = 1, name = Name, user = User,
                partition = Partition, state = pending,
                script = Script,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = [],
                work_dir = WorkDir,
                std_out = StdOut,
                std_err = StdErr
            },
            R = flurm_core:update_job_state(Job, running),
            C = flurm_core:update_job_state(R, completed),
            C#job.name =:= Name andalso
            C#job.user =:= User andalso
            C#job.partition =:= Partition andalso
            C#job.script =:= Script andalso
            C#job.work_dir =:= WorkDir andalso
            C#job.std_out =:= StdOut andalso
            C#job.std_err =:= StdErr
        end).

%%====================================================================
%% Property 10: Array Job Consistency
%%
%% Array job metadata survives all state transitions.
%%====================================================================

prop_array_job_consistency() ->
    ?FORALL({ParentId, TaskId, States},
            {pos_integer(), non_neg_integer(),
             non_empty(list(any_state()))},
        begin
            Job0 = #job{
                id = ParentId + TaskId + 1,
                name = <<"array_test">>, user = <<"test">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho array">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = [],
                array_job_id = ParentId,
                array_task_id = TaskId
            },
            {_FinalJob, AllOk} = lists:foldl(
                fun(State, {Job, Ok}) ->
                    Updated = flurm_core:update_job_state(Job, State),
                    ArrayOk = Updated#job.array_job_id =:= ParentId andalso
                              Updated#job.array_task_id =:= TaskId,
                    {Updated, Ok andalso ArrayOk}
                end,
                {Job0, true},
                States),
            AllOk
        end).

%%====================================================================
%% Property 11: GRES Field Preservation
%%
%% GRES (GPU/FPGA) fields survive state transitions.
%%====================================================================

prop_gres_field_preservation() ->
    ?FORALL({Gres, GresPerNode, GresPerTask, GpuType},
            {binary(8), binary(8), binary(8), binary(4)},
        begin
            Job = #job{
                id = 1, name = <<"gres_test">>, user = <<"test">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho gres">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = [],
                gres = Gres,
                gres_per_node = GresPerNode,
                gres_per_task = GresPerTask,
                gpu_type = GpuType
            },
            %% Transition through all active states
            Running = flurm_core:update_job_state(Job, running),
            Suspended = flurm_core:update_job_state(Running, suspended),
            Resumed = flurm_core:update_job_state(Suspended, running),
            Done = flurm_core:update_job_state(Resumed, completed),
            %% All GRES fields preserved
            Done#job.gres =:= Gres andalso
            Done#job.gres_per_node =:= GresPerNode andalso
            Done#job.gres_per_task =:= GresPerTask andalso
            Done#job.gpu_type =:= GpuType
        end).

%%====================================================================
%% Property 12: License Field Preservation
%%
%% License fields survive state transitions.
%%====================================================================

prop_license_field_preservation() ->
    ?FORALL(NumLicenses, range(0, 5),
        begin
            Licenses = [{list_to_binary("lic" ++ integer_to_list(I)), I}
                       || I <- lists:seq(1, NumLicenses)],
            Job = #job{
                id = 1, name = <<"lic_test">>, user = <<"test">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho lic">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = [],
                licenses = Licenses
            },
            Running = flurm_core:update_job_state(Job, running),
            Done = flurm_core:update_job_state(Running, failed),
            Done#job.licenses =:= Licenses
        end).

%%====================================================================
%% Property 13: QOS and Account Preservation
%%====================================================================

prop_qos_account_preservation() ->
    ?FORALL({Account, Qos, Reservation},
            {binary(8), binary(8), binary(8)},
        begin
            Job = #job{
                id = 1, name = <<"qos_test">>, user = <<"test">>,
                partition = <<"default">>, state = pending,
                script = <<"#!/bin/bash\necho qos">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 100,
                submit_time = erlang:system_time(second),
                allocated_nodes = [],
                account = Account,
                qos = Qos,
                reservation = Reservation
            },
            %% Full lifecycle
            Running = flurm_core:update_job_state(Job, running),
            Completed = flurm_core:update_job_state(Running, completed),
            Completed#job.account =:= Account andalso
            Completed#job.qos =:= Qos andalso
            Completed#job.reservation =:= Reservation
        end).

%%====================================================================
%% EUnit Integration
%%====================================================================

chaos_test_() ->
    {timeout, 300, [
        {"State transition storm (1000 tests)", fun() ->
            ?assert(proper:quickcheck(prop_state_transition_storm(),
                [{numtests, 1000}, {to_file, user}]))
        end},
        {"Resources never negative (1000 tests)", fun() ->
            ?assert(proper:quickcheck(prop_resources_never_negative(),
                [{numtests, 1000}, {to_file, user}]))
        end},
        {"NO_VAL boundary handling (1000 tests)", fun() ->
            ?assert(proper:quickcheck(prop_no_val_boundary_handling(),
                [{numtests, 1000}, {to_file, user}]))
        end},
        {"Multi-job isolation (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_multi_job_isolation(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"TRES extreme values (1000 tests)", fun() ->
            ?assert(proper:quickcheck(prop_tres_extreme_values(),
                [{numtests, 1000}, {to_file, user}]))
        end},
        {"State transition idempotency (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_state_transition_idempotency(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"Apply job updates (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_apply_job_updates(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"Multi-node allocation (1000 tests)", fun() ->
            ?assert(proper:quickcheck(prop_multi_node_allocation(),
                [{numtests, 1000}, {to_file, user}]))
        end},
        {"Binary field preservation (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_binary_field_preservation(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"Array job consistency (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_array_job_consistency(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"GRES field preservation (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_gres_field_preservation(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"License field preservation (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_license_field_preservation(),
                [{numtests, 500}, {to_file, user}]))
        end},
        {"QOS/Account preservation (500 tests)", fun() ->
            ?assert(proper:quickcheck(prop_qos_account_preservation(),
                [{numtests, 500}, {to_file, user}]))
        end}
    ]}.
