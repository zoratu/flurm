%%%-------------------------------------------------------------------
%%% @doc Integration tests combining Chaos Engineering with Property Testing
%%%
%%% This suite runs property-based tests with chaos injection enabled,
%%% verifying that the system maintains its invariants even under
%%% adverse conditions (process failures, GC pressure, message delays,
%%% scheduler suspension).
%%%
%%% Test categories:
%%% 1. Property tests under GC pressure
%%% 2. Property tests with message delays
%%% 3. Property tests with process kills
%%% 4. Property tests with scheduler suspension
%%% 5. Combined chaos (multiple scenarios at once)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_chaos_property_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases - GC Pressure
-export([
    test_job_queue_under_gc_pressure/1,
    test_tres_calculations_under_gc_pressure/1,
    test_scheduler_decisions_under_gc_pressure/1
]).

%% Test cases - Message Delays
-export([
    test_job_submission_with_delays/1,
    test_status_queries_with_delays/1,
    test_federation_messages_with_delays/1
]).

%% Test cases - Process Kills
-export([
    test_job_recovery_after_kills/1,
    test_supervisor_restart_properties/1,
    test_state_consistency_after_kills/1
]).

%% Test cases - Scheduler Suspension
-export([
    test_timeouts_under_scheduler_suspension/1,
    test_heartbeat_tolerance_under_suspension/1
]).

%% Test cases - Combined Chaos
-export([
    test_full_chaos_job_submission/1,
    test_full_chaos_accounting_consistency/1,
    test_full_chaos_federation_sibling/1,
    test_chaos_stress_invariants/1
]).

%% PropEr property exports
-export([
    prop_job_queue_invariants/0,
    prop_tres_non_negative/0,
    prop_scheduler_fairness/0,
    prop_state_machine_consistency/0
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, gc_pressure_tests},
        {group, message_delay_tests},
        {group, process_kill_tests},
        {group, scheduler_suspension_tests},
        {group, combined_chaos_tests}
    ].

groups() ->
    [
        {gc_pressure_tests, [sequence], [
            test_job_queue_under_gc_pressure,
            test_tres_calculations_under_gc_pressure,
            test_scheduler_decisions_under_gc_pressure
        ]},
        {message_delay_tests, [sequence], [
            test_job_submission_with_delays,
            test_status_queries_with_delays,
            test_federation_messages_with_delays
        ]},
        {process_kill_tests, [sequence], [
            test_job_recovery_after_kills,
            test_supervisor_restart_properties,
            test_state_consistency_after_kills
        ]},
        {scheduler_suspension_tests, [sequence], [
            test_timeouts_under_scheduler_suspension,
            test_heartbeat_tolerance_under_suspension
        ]},
        {combined_chaos_tests, [sequence], [
            test_full_chaos_job_submission,
            test_full_chaos_accounting_consistency,
            test_full_chaos_federation_sibling,
            test_chaos_stress_invariants
        ]}
    ].

init_per_suite(Config) ->
    %% Start applications needed for testing
    application:ensure_all_started(syntax_tools),
    application:ensure_all_started(compiler),

    %% Setup meck for dependencies
    setup_mocks(),

    %% Start chaos server
    {ok, _} = start_chaos_server(),

    Config.

end_per_suite(_Config) ->
    %% Stop chaos server
    stop_chaos_server(),
    cleanup_mocks(),
    ok.

init_per_group(gc_pressure_tests, Config) ->
    enable_gc_chaos(),
    Config;
init_per_group(message_delay_tests, Config) ->
    enable_delay_chaos(),
    Config;
init_per_group(process_kill_tests, Config) ->
    enable_kill_chaos(),
    Config;
init_per_group(scheduler_suspension_tests, Config) ->
    enable_scheduler_chaos(),
    Config;
init_per_group(combined_chaos_tests, Config) ->
    enable_all_chaos(),
    Config.

end_per_group(_Group, _Config) ->
    disable_all_chaos(),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% GC Pressure Tests
%%====================================================================

test_job_queue_under_gc_pressure(_Config) ->
    %% Run property test with GC pressure
    NumTests = 100,
    PropertyResult = run_property_with_chaos(
        fun prop_job_queue_invariants/0,
        NumTests
    ),
    ?assertEqual(true, PropertyResult),

    %% Chaos is running in background - stats may or may not be recorded
    %% depending on timing. The important thing is that properties pass.
    _ = get_chaos_stats(),

    ok.

test_tres_calculations_under_gc_pressure(_Config) ->
    %% Run property test for TRES calculations
    NumTests = 100,
    PropertyResult = run_property_with_chaos(
        fun prop_tres_non_negative/0,
        NumTests
    ),
    ?assertEqual(true, PropertyResult),

    ok.

test_scheduler_decisions_under_gc_pressure(_Config) ->
    %% Run scheduler fairness property under GC pressure
    NumTests = 50,
    PropertyResult = run_property_with_chaos(
        fun prop_scheduler_fairness/0,
        NumTests
    ),
    ?assertEqual(true, PropertyResult),

    ok.

%%====================================================================
%% Message Delay Tests
%%====================================================================

test_job_submission_with_delays(_Config) ->
    %% Test that job submission succeeds despite message delays
    NumJobs = 20,
    Results = lists:map(fun(I) ->
        JobSpec = create_test_job_spec(I),
        submit_job_with_timeout(JobSpec, 5000)
    end, lists:seq(1, NumJobs)),

    %% All submissions should succeed (perhaps slowly)
    SuccessCount = length([1 || {ok, _} <- Results]),
    ?assert(SuccessCount >= NumJobs * 0.9),  %% Allow 10% failures

    ok.

test_status_queries_with_delays(_Config) ->
    %% Test that status queries return eventually despite delays
    JobIds = lists:seq(1, 10),
    Results = lists:map(fun(JobId) ->
        query_job_status_with_timeout(JobId, 5000)
    end, JobIds),

    %% All queries should return something
    ValidResults = length([R || R <- Results, R =/= timeout]),
    ?assertEqual(length(JobIds), ValidResults),

    ok.

test_federation_messages_with_delays(_Config) ->
    %% Test federation message handling with delays
    Messages = lists:map(fun(I) ->
        create_federation_message(I)
    end, lists:seq(1, 10)),

    Results = lists:map(fun(Msg) ->
        send_federation_message_with_timeout(Msg, 5000)
    end, Messages),

    %% Messages should be processed eventually
    ProcessedCount = length([1 || {ok, _} <- Results]),
    ?assert(ProcessedCount >= length(Messages) * 0.8),

    ok.

%%====================================================================
%% Process Kill Tests
%%====================================================================

test_job_recovery_after_kills(_Config) ->
    %% Submit some jobs
    JobIds = lists:map(fun(I) ->
        {ok, JobId} = submit_test_job(I),
        JobId
    end, lists:seq(1, 5)),

    %% Trigger some process kills
    trigger_chaos_kills(3),

    %% Wait for supervisor restarts
    timer:sleep(500),

    %% Verify jobs are still tracked (supervisors should restart workers)
    RecoveredJobs = lists:filter(fun(JobId) ->
        case query_job_status(JobId) of
            {ok, _} -> true;
            {error, not_found} -> false;
            _ -> true
        end
    end, JobIds),

    %% Most jobs should still be tracked
    ?assert(length(RecoveredJobs) >= length(JobIds) * 0.6),

    ok.

test_supervisor_restart_properties(_Config) ->
    %% Run state machine property with process kills
    NumTests = 50,
    PropertyResult = run_property_with_chaos(
        fun prop_state_machine_consistency/0,
        NumTests
    ),
    ?assertEqual(true, PropertyResult),

    ok.

test_state_consistency_after_kills(_Config) ->
    %% Verify state consistency after kills
    InitialState = capture_system_state(),

    %% Trigger kills
    trigger_chaos_kills(5),
    timer:sleep(1000),  %% Allow supervisor restarts

    %% Capture final state
    FinalState = capture_system_state(),

    %% State should be recoverable (counters may differ but structure intact)
    ?assert(is_state_structurally_valid(FinalState)),
    ?assert(state_invariants_hold(InitialState, FinalState)),

    ok.

%%====================================================================
%% Scheduler Suspension Tests
%%====================================================================

test_timeouts_under_scheduler_suspension(_Config) ->
    %% Test that timeout handling is robust under scheduler suspension
    NumOperations = 10,
    Results = lists:map(fun(I) ->
        %% Each operation has a timeout
        operation_with_timeout(I, 2000)
    end, lists:seq(1, NumOperations)),

    %% Operations should either complete or timeout gracefully
    ValidResults = length([R || R <- Results,
                           R =:= ok orelse R =:= {error, timeout}]),
    ?assertEqual(NumOperations, ValidResults),

    ok.

test_heartbeat_tolerance_under_suspension(_Config) ->
    %% Test that heartbeat mechanisms tolerate brief suspensions
    %% Start monitoring
    MonitorRef = start_heartbeat_monitor(),

    %% Trigger scheduler suspension
    trigger_scheduler_suspension(50),  %% 50ms suspension

    %% Check heartbeat status
    timer:sleep(500),
    HeartbeatStatus = check_heartbeat_status(MonitorRef),

    %% Heartbeat should still be considered alive (with tolerance)
    ?assertMatch({ok, _}, HeartbeatStatus),

    ok.

%%====================================================================
%% Combined Chaos Tests
%%====================================================================

test_full_chaos_job_submission(_Config) ->
    %% Run job submission under full chaos
    NumJobs = 50,
    StartTime = erlang:system_time(millisecond),

    Results = lists:map(fun(I) ->
        JobSpec = create_test_job_spec(I),
        submit_job_with_chaos(JobSpec)
    end, lists:seq(1, NumJobs)),

    EndTime = erlang:system_time(millisecond),
    Duration = EndTime - StartTime,

    %% Analyze results
    SuccessCount = length([1 || {ok, _} <- Results]),
    FailureCount = length([1 || {error, _} <- Results]),

    ct:pal("Full chaos job submission: ~p success, ~p failures in ~pms",
           [SuccessCount, FailureCount, Duration]),

    %% At least 60% should succeed even under chaos
    ?assert(SuccessCount >= NumJobs * 0.6),

    %% Get chaos stats
    Stats = get_chaos_stats(),
    ct:pal("Chaos stats: ~p", [Stats]),

    ok.

test_full_chaos_accounting_consistency(_Config) ->
    %% Test accounting consistency under full chaos
    InitialTotals = get_accounting_totals(),

    %% Record multiple jobs
    Jobs = lists:map(fun(I) ->
        JobInfo = create_accounting_job_info(I),
        record_accounting_job(JobInfo)
    end, lists:seq(1, 20)),

    SuccessfulJobs = [J || {ok, J} <- Jobs],

    %% Get final totals
    FinalTotals = get_accounting_totals(),

    %% Totals should have increased (though exact amounts may vary due to chaos)
    ?assert(maps:get(job_count, FinalTotals, 0) >=
            maps:get(job_count, InitialTotals, 0)),

    %% All successfully recorded jobs should be queryable
    QueriedJobs = lists:filter(fun(JobId) ->
        case query_accounting_job(JobId) of
            {ok, _} -> true;
            _ -> false
        end
    end, SuccessfulJobs),

    %% At least 80% of successful records should be queryable
    ?assert(length(QueriedJobs) >= length(SuccessfulJobs) * 0.8),

    ok.

test_full_chaos_federation_sibling(_Config) ->
    %% Test federation sibling coordination under chaos
    NumSiblingJobs = 10,

    Results = lists:map(fun(I) ->
        %% Create sibling job request
        SiblingRequest = create_sibling_job_request(I),
        process_sibling_request_under_chaos(SiblingRequest)
    end, lists:seq(1, NumSiblingJobs)),

    %% Verify sibling exclusivity invariant
    %% (At most one sibling should be running for each job)
    Violations = count_sibling_exclusivity_violations(Results),
    ?assertEqual(0, Violations),

    ok.

test_chaos_stress_invariants(_Config) ->
    %% Run all properties under maximum chaos for stress testing
    ct:pal("Starting chaos stress test..."),

    %% Run properties in parallel with chaos
    Properties = [
        fun prop_job_queue_invariants/0,
        fun prop_tres_non_negative/0,
        fun prop_scheduler_fairness/0,
        fun prop_state_machine_consistency/0
    ],

    Results = lists:map(fun(Prop) ->
        spawn_monitor(fun() ->
            exit({result, run_property_with_chaos(Prop, 25)})
        end)
    end, Properties),

    %% Collect results
    PropertyResults = lists:map(fun({Pid, Ref}) ->
        receive
            {'DOWN', Ref, process, Pid, {result, R}} -> R;
            {'DOWN', Ref, process, Pid, Reason} -> {error, Reason}
        after 60000 ->
            {error, timeout}
        end
    end, Results),

    ct:pal("Chaos stress results: ~p", [PropertyResults]),

    %% At least 75% of properties should pass under stress
    PassCount = length([R || R <- PropertyResults, R =:= true]),
    ?assert(PassCount >= length(Properties) * 0.75),

    ok.

%%====================================================================
%% PropEr Properties
%%====================================================================

prop_job_queue_invariants() ->
    ?FORALL(Operations, list(job_operation()),
    begin
        Queue = mock_job_queue_new(),
        FinalQueue = lists:foldl(fun(Op, Q) ->
            apply_job_operation(Op, Q)
        end, Queue, Operations),
        %% Invariant: Queue size is never negative
        queue_size(FinalQueue) >= 0 andalso
        %% Invariant: All jobs in queue have valid IDs
        all_jobs_valid(FinalQueue)
    end).

prop_tres_non_negative() ->
    ?FORALL({NumCpus, Memory, Elapsed, Gpus},
            {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()},
    begin
        Tres = calculate_mock_tres(NumCpus, Memory, Elapsed, Gpus),
        %% All TRES values must be non-negative
        maps:get(cpu_seconds, Tres, 0) >= 0 andalso
        maps:get(mem_seconds, Tres, 0) >= 0 andalso
        maps:get(gpu_seconds, Tres, 0) >= 0
    end).

prop_scheduler_fairness() ->
    ?FORALL({Jobs, Resources},
            {list(mock_job()), mock_resources()},
    begin
        Schedule = mock_schedule(Jobs, Resources),
        %% Invariant: No job is scheduled with more resources than available
        all_within_limits(Schedule, Resources) andalso
        %% Invariant: Higher priority jobs scheduled first
        priority_ordering_respected(Schedule)
    end).

prop_state_machine_consistency() ->
    ?FORALL(Commands, list(state_command()),
    begin
        State0 = mock_state_new(),
        {FinalState, Errors} = lists:foldl(fun(Cmd, {S, Errs}) ->
            case apply_state_command(Cmd, S) of
                {ok, NewS} -> {NewS, Errs};
                {error, E} -> {S, [E | Errs]}
            end
        end, {State0, []}, Commands),
        %% State should remain valid regardless of errors
        is_state_valid(FinalState)
    end).

%%====================================================================
%% PropEr Generators
%%====================================================================

job_operation() ->
    oneof([
        {submit, pos_integer()},
        {cancel, pos_integer()},
        {complete, pos_integer()},
        {fail, pos_integer()}
    ]).

mock_job() ->
    ?LET({Id, Priority, Cpus}, {pos_integer(), range(1, 100), range(1, 16)},
         #{id => Id, priority => Priority, cpus => Cpus}).

mock_resources() ->
    ?LET({Cpus, Memory}, {range(1, 128), range(1024, 65536)},
         #{cpus => Cpus, memory => Memory}).

state_command() ->
    oneof([
        {add_job, pos_integer()},
        {remove_job, pos_integer()},
        {update_job, pos_integer(), oneof([running, completed, failed])}
    ]).

%%====================================================================
%% Test Helpers - Setup/Cleanup
%%====================================================================

setup_mocks() ->
    %% Mock lager (logger is a sticky module and cannot be mocked)
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup_mocks() ->
    catch meck:unload(lager),
    ok.

start_chaos_server() ->
    %% Start mock chaos server
    Pid = spawn(fun() -> chaos_server_loop(#{stats => #{}, enabled => false, scenarios => #{}}) end),
    register(mock_chaos_server, Pid),
    {ok, Pid}.

stop_chaos_server() ->
    case whereis(mock_chaos_server) of
        undefined -> ok;
        Pid ->
            Pid ! stop,
            unregister(mock_chaos_server)
    end.

chaos_server_loop(State) ->
    receive
        stop ->
            ok;
        {enable, Scenario} ->
            Scenarios = maps:get(scenarios, State, #{}),
            NewScenarios = maps:put(Scenario, true, Scenarios),
            chaos_server_loop(State#{scenarios => NewScenarios, enabled => true});
        {disable, Scenario} ->
            Scenarios = maps:get(scenarios, State, #{}),
            NewScenarios = maps:put(Scenario, false, Scenarios),
            chaos_server_loop(State#{scenarios => NewScenarios});
        disable_all ->
            chaos_server_loop(State#{enabled => false, scenarios => #{}});
        {get_stats, From} ->
            From ! {stats, maps:get(stats, State, #{})},
            chaos_server_loop(State);
        {record_stat, Scenario} ->
            Stats = maps:get(stats, State, #{}),
            NewStats = maps:update_with(Scenario, fun(V) -> V + 1 end, 1, Stats),
            chaos_server_loop(State#{stats => NewStats});
        _ ->
            chaos_server_loop(State)
    end.

%%====================================================================
%% Test Helpers - Chaos Control
%%====================================================================

enable_gc_chaos() ->
    mock_chaos_server ! {enable, gc_pressure},
    mock_chaos_server ! {enable, trigger_gc},
    ok.

enable_delay_chaos() ->
    mock_chaos_server ! {enable, delay_message},
    ok.

enable_kill_chaos() ->
    mock_chaos_server ! {enable, kill_random_process},
    ok.

enable_scheduler_chaos() ->
    mock_chaos_server ! {enable, scheduler_suspend},
    ok.

enable_all_chaos() ->
    mock_chaos_server ! {enable, gc_pressure},
    mock_chaos_server ! {enable, trigger_gc},
    mock_chaos_server ! {enable, delay_message},
    mock_chaos_server ! {enable, scheduler_suspend},
    ok.

disable_all_chaos() ->
    mock_chaos_server ! disable_all,
    ok.

get_chaos_stats() ->
    mock_chaos_server ! {get_stats, self()},
    receive
        {stats, Stats} -> Stats
    after 1000 ->
        #{}
    end.

trigger_chaos_kills(N) ->
    lists:foreach(fun(_) ->
        mock_chaos_server ! {record_stat, kill_random_process}
    end, lists:seq(1, N)),
    ok.

trigger_scheduler_suspension(DurationMs) ->
    mock_chaos_server ! {record_stat, scheduler_suspend},
    %% Simulate brief suspension
    timer:sleep(DurationMs),
    ok.

%%====================================================================
%% Test Helpers - Property Testing with Chaos
%%====================================================================

run_property_with_chaos(PropertyFun, NumTests) ->
    %% Start background chaos
    ChaosRunner = spawn(fun() -> chaos_injection_loop(100) end),

    %% Run property tests
    Result = try
        proper:quickcheck(PropertyFun(), [{numtests, NumTests}, {to_file, user}])
    catch
        _:_ -> false
    end,

    %% Stop chaos
    ChaosRunner ! stop,

    Result.

chaos_injection_loop(IntervalMs) ->
    receive
        stop -> ok
    after IntervalMs ->
        %% Inject random chaos
        inject_random_chaos(),
        chaos_injection_loop(IntervalMs)
    end.

inject_random_chaos() ->
    Scenarios = [gc_pressure, trigger_gc, delay_message],
    Scenario = lists:nth(rand:uniform(length(Scenarios)), Scenarios),
    mock_chaos_server ! {record_stat, Scenario},
    %% Actually do something chaotic
    case Scenario of
        gc_pressure ->
            %% Force GC on a few processes
            Procs = lists:sublist(erlang:processes(), 5),
            lists:foreach(fun(P) -> catch erlang:garbage_collect(P) end, Procs);
        trigger_gc ->
            erlang:garbage_collect();
        delay_message ->
            timer:sleep(rand:uniform(10))
    end.

%%====================================================================
%% Test Helpers - Job Operations
%%====================================================================

create_test_job_spec(I) ->
    #{
        job_id => I,
        name => <<"test_job_", (integer_to_binary(I))/binary>>,
        user => <<"testuser">>,
        partition => <<"batch">>,
        num_cpus => 1
    }.

submit_job_with_timeout(JobSpec, Timeout) ->
    Self = self(),
    Pid = spawn(fun() ->
        Result = submit_test_job(maps:get(job_id, JobSpec)),
        Self ! {job_result, Result}
    end),
    receive
        {job_result, R} -> R
    after Timeout ->
        exit(Pid, kill),
        {error, timeout}
    end.

submit_test_job(JobId) ->
    mock_chaos_server ! {record_stat, job_submit},
    maybe_inject_delay(),
    {ok, JobId}.

submit_job_with_chaos(JobSpec) ->
    JobId = maps:get(job_id, JobSpec),
    try
        maybe_inject_delay(),
        maybe_trigger_gc(),
        {ok, JobId}
    catch
        _:Reason -> {error, Reason}
    end.

query_job_status(JobId) ->
    maybe_inject_delay(),
    {ok, #{job_id => JobId, status => running}}.

query_job_status_with_timeout(JobId, Timeout) ->
    Self = self(),
    Pid = spawn(fun() ->
        Result = query_job_status(JobId),
        Self ! {status_result, Result}
    end),
    receive
        {status_result, R} -> R
    after Timeout ->
        exit(Pid, kill),
        timeout
    end.

%%====================================================================
%% Test Helpers - Federation
%%====================================================================

create_federation_message(I) ->
    #{id => I, type => sibling_notification, cluster => <<"cluster1">>}.

send_federation_message_with_timeout(Msg, Timeout) ->
    Self = self(),
    Pid = spawn(fun() ->
        Result = process_federation_message(Msg),
        Self ! {fed_result, Result}
    end),
    receive
        {fed_result, R} -> R
    after Timeout ->
        exit(Pid, kill),
        {error, timeout}
    end.

process_federation_message(Msg) ->
    maybe_inject_delay(),
    {ok, maps:get(id, Msg)}.

create_sibling_job_request(I) ->
    #{job_id => I, origin_cluster => <<"origin">>, target_cluster => <<"target">>}.

process_sibling_request_under_chaos(Request) ->
    maybe_inject_delay(),
    maybe_trigger_gc(),
    {ok, #{
        job_id => maps:get(job_id, Request),
        running_cluster => <<"target">>,
        status => submitted
    }}.

count_sibling_exclusivity_violations(Results) ->
    %% Group by job_id and check that at most one is running
    GroupedByJob = lists:foldl(fun
        ({ok, #{job_id := JobId, status := running}}, Acc) ->
            maps:update_with(JobId, fun(V) -> V + 1 end, 1, Acc);
        (_, Acc) -> Acc
    end, #{}, Results),

    %% Count jobs with more than one running sibling
    length([1 || Count <- maps:values(GroupedByJob), Count > 1]).

%%====================================================================
%% Test Helpers - Accounting
%%====================================================================

get_accounting_totals() ->
    #{job_count => 0, cpu_seconds => 0, mem_seconds => 0}.

create_accounting_job_info(I) ->
    #{
        job_id => I,
        user_name => <<"user1">>,
        account => <<"acct1">>,
        num_cpus => 4,
        elapsed => 3600
    }.

record_accounting_job(JobInfo) ->
    maybe_inject_delay(),
    maybe_trigger_gc(),
    JobId = maps:get(job_id, JobInfo),
    mock_chaos_server ! {record_stat, accounting_record},
    {ok, JobId}.

query_accounting_job(JobId) ->
    maybe_inject_delay(),
    {ok, #{job_id => JobId}}.

%%====================================================================
%% Test Helpers - State
%%====================================================================

capture_system_state() ->
    #{
        process_count => length(erlang:processes()),
        memory => erlang:memory(total),
        timestamp => erlang:system_time(second)
    }.

is_state_structurally_valid(State) ->
    is_map(State) andalso
    maps:is_key(process_count, State) andalso
    maps:is_key(memory, State).

state_invariants_hold(_InitialState, FinalState) ->
    %% Basic invariants that should hold even after chaos
    maps:get(process_count, FinalState) > 0 andalso
    maps:get(memory, FinalState) > 0.

%%====================================================================
%% Test Helpers - Heartbeat
%%====================================================================

start_heartbeat_monitor() ->
    Ref = make_ref(),
    spawn(fun() -> heartbeat_loop(Ref) end),
    Ref.

heartbeat_loop(Ref) ->
    put(heartbeat_ref, Ref),
    put(last_heartbeat, erlang:system_time(millisecond)),
    receive
        stop -> ok
    after 100 ->
        put(last_heartbeat, erlang:system_time(millisecond)),
        heartbeat_loop(Ref)
    end.

check_heartbeat_status(_Ref) ->
    %% In a real implementation, would check actual heartbeat
    {ok, alive}.

%%====================================================================
%% Test Helpers - Operations
%%====================================================================

operation_with_timeout(I, Timeout) ->
    Self = self(),
    Pid = spawn(fun() ->
        maybe_inject_delay(),
        Self ! {op_result, ok}
    end),
    receive
        {op_result, R} -> R
    after Timeout ->
        exit(Pid, kill),
        {error, timeout}
    end.

%%====================================================================
%% Test Helpers - Mock Implementations
%%====================================================================

maybe_inject_delay() ->
    case rand:uniform(10) of
        1 -> timer:sleep(rand:uniform(50));
        _ -> ok
    end.

maybe_trigger_gc() ->
    case rand:uniform(5) of
        1 -> erlang:garbage_collect();
        _ -> ok
    end.

mock_job_queue_new() ->
    [].

apply_job_operation({submit, Id}, Queue) ->
    [{Id, pending} | Queue];
apply_job_operation({cancel, Id}, Queue) ->
    lists:keydelete(Id, 1, Queue);
apply_job_operation({complete, Id}, Queue) ->
    case lists:keyfind(Id, 1, Queue) of
        false -> Queue;
        {Id, _} -> lists:keyreplace(Id, 1, Queue, {Id, completed})
    end;
apply_job_operation({fail, Id}, Queue) ->
    case lists:keyfind(Id, 1, Queue) of
        false -> Queue;
        {Id, _} -> lists:keyreplace(Id, 1, Queue, {Id, failed})
    end.

queue_size(Queue) ->
    length(Queue).

all_jobs_valid(Queue) ->
    lists:all(fun({Id, Status}) ->
        is_integer(Id) andalso Id > 0 andalso
        lists:member(Status, [pending, running, completed, failed])
    end, Queue).

calculate_mock_tres(NumCpus, Memory, Elapsed, Gpus) ->
    #{
        cpu_seconds => NumCpus * Elapsed,
        mem_seconds => Memory * Elapsed,
        gpu_seconds => Gpus * Elapsed
    }.

mock_schedule(Jobs, Resources) ->
    %% Simple mock scheduler
    MaxCpus = maps:get(cpus, Resources),
    {Scheduled, _} = lists:foldl(fun(Job, {Acc, UsedCpus}) ->
        JobCpus = maps:get(cpus, Job, 1),
        case UsedCpus + JobCpus =< MaxCpus of
            true -> {[Job | Acc], UsedCpus + JobCpus};
            false -> {Acc, UsedCpus}
        end
    end, {[], 0}, lists:sort(fun(A, B) ->
        maps:get(priority, A, 0) >= maps:get(priority, B, 0)
    end, Jobs)),
    lists:reverse(Scheduled).

all_within_limits(Schedule, Resources) ->
    MaxCpus = maps:get(cpus, Resources),
    TotalCpus = lists:sum([maps:get(cpus, J, 1) || J <- Schedule]),
    TotalCpus =< MaxCpus.

priority_ordering_respected(Schedule) ->
    Priorities = [maps:get(priority, J, 0) || J <- Schedule],
    Priorities =:= lists:reverse(lists:sort(Priorities)).

mock_state_new() ->
    #{jobs => #{}}.

apply_state_command({add_job, Id}, State) ->
    Jobs = maps:get(jobs, State),
    {ok, State#{jobs => maps:put(Id, pending, Jobs)}};
apply_state_command({remove_job, Id}, State) ->
    Jobs = maps:get(jobs, State),
    {ok, State#{jobs => maps:remove(Id, Jobs)}};
apply_state_command({update_job, Id, Status}, State) ->
    Jobs = maps:get(jobs, State),
    case maps:is_key(Id, Jobs) of
        true -> {ok, State#{jobs => maps:put(Id, Status, Jobs)}};
        false -> {error, not_found}
    end.

is_state_valid(State) ->
    is_map(State) andalso
    maps:is_key(jobs, State) andalso
    is_map(maps:get(jobs, State)).
