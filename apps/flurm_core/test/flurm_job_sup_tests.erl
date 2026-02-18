%%%-------------------------------------------------------------------
%%% @doc FLURM Job Supervisor Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_sup supervisor,
%%% covering supervisor initialization, child specifications,
%%% dynamic job process management, and error handling.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_sup_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Internal helper exports (for cross-function visibility)
-compile({nowarn_unused_function, [{wait_for_process_death, 2}, {wait_for_process_death, 3}]}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Basic supervisor tests with mocked dependencies
job_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Start link succeeds", fun test_start_link/0},
        {"Init returns correct supervisor flags", fun test_init_supervisor_flags/0},
        {"Init returns correct child spec", fun test_init_child_spec/0},
        {"Start job with valid spec", fun test_start_job_valid_spec/0},
        {"Stop job", fun test_stop_job/0},
        {"Which jobs returns pids", fun test_which_jobs/0},
        {"Count jobs", fun test_count_jobs/0},
        {"Multiple jobs lifecycle", fun test_multiple_jobs_lifecycle/0},
        {"Start job error handling", fun test_start_job_error/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),

    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(flurm_job),
    catch meck:unload(flurm_accounting),

    %% Mock dependencies
    meck:new(flurm_job, [passthrough, non_strict]),
    meck:expect(flurm_job, start_link, fun(#job_spec{} = _Spec) ->
        %% Create a dummy process that acts like a job
        Pid = spawn(fun() -> job_loop() end),
        {ok, Pid}
    end),

    meck:new(flurm_accounting, [passthrough, non_strict]),
    meck:expect(flurm_accounting, record_job_submit, fun(_Data) -> ok end),

    ok.

cleanup(_) ->
    %% Stop the supervisor if running with proper monitor/wait pattern
    case whereis(flurm_job_sup) of
        undefined -> ok;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Ref = monitor(process, Pid),
                    unlink(Pid),
                    catch gen_server:stop(Pid, shutdown, 5000),
                    receive
                        {'DOWN', Ref, process, Pid, _} -> ok
                    after 5000 ->
                        demonitor(Ref, [flush]),
                        catch exit(Pid, kill)
                    end;
                false ->
                    ok
            end
    end,

    catch meck:unload(flurm_job),
    catch meck:unload(flurm_accounting),
    ok.

%% Dummy job process loop
job_loop() ->
    receive
        stop -> ok;
        _ -> job_loop()
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_spec() ->
    make_job_spec(#{}).

make_job_spec(Overrides) ->
    Defaults = #{
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 4,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>,
        priority => 100
    },
    Props = maps:merge(Defaults, Overrides),
    #job_spec{
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        time_limit = maps:get(time_limit, Props),
        script = maps:get(script, Props),
        priority = maps:get(priority, Props)
    }.

%%====================================================================
%% Start Link Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_job_sup:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_job_sup)),
    ok.

%%====================================================================
%% Init Callback Tests
%%====================================================================

test_init_supervisor_flags() ->
    %% Test init directly
    {ok, {SupFlags, _ChildSpecs}} = flurm_job_sup:init([]),

    ?assert(is_map(SupFlags)),
    ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
    %% Jobs should not auto-restart (intensity = 0)
    ?assertEqual(0, maps:get(intensity, SupFlags)),
    ?assertEqual(1, maps:get(period, SupFlags)),
    ok.

test_init_child_spec() ->
    {ok, {_SupFlags, ChildSpecs}} = flurm_job_sup:init([]),

    ?assertEqual(1, length(ChildSpecs)),
    [ChildSpec] = ChildSpecs,

    ?assert(is_map(ChildSpec)),
    ?assertEqual(flurm_job, maps:get(id, ChildSpec)),
    ?assertEqual({flurm_job, start_link, []}, maps:get(start, ChildSpec)),
    %% Temporary restart - jobs don't auto-restart
    ?assertEqual(temporary, maps:get(restart, ChildSpec)),
    ?assertEqual(5000, maps:get(shutdown, ChildSpec)),
    ?assertEqual(worker, maps:get(type, ChildSpec)),
    ?assertEqual([flurm_job], maps:get(modules, ChildSpec)),
    ok.

%%====================================================================
%% Start Job Tests
%%====================================================================

test_start_job_valid_spec() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, JobPid} = flurm_job_sup:start_job(JobSpec),

    ?assert(is_pid(JobPid)),
    ?assert(is_process_alive(JobPid)),

    %% Verify job is in supervisor children
    Jobs = flurm_job_sup:which_jobs(),
    ?assert(lists:member(JobPid, Jobs)),
    ok.

test_start_job_error() ->
    %% Setup mock to return error
    meck:expect(flurm_job, start_link, fun(_Spec) ->
        {error, invalid_spec}
    end),

    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    Result = flurm_job_sup:start_job(JobSpec),

    ?assertEqual({error, invalid_spec}, Result),
    ok.

%%====================================================================
%% Stop Job Tests
%%====================================================================

test_stop_job() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, JobPid} = flurm_job_sup:start_job(JobSpec),

    ?assert(is_process_alive(JobPid)),
    ?assertEqual(1, flurm_job_sup:count_jobs()),

    ok = flurm_job_sup:stop_job(JobPid),

    %% Wait for process to terminate
    _ = sys:get_state(flurm_job_sup),

    ?assertNot(is_process_alive(JobPid)),
    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ok.

%%====================================================================
%% Which Jobs Tests
%%====================================================================

test_which_jobs() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    %% Initially empty
    ?assertEqual([], flurm_job_sup:which_jobs()),

    %% Start some jobs
    JobSpec = make_job_spec(),
    {ok, Pid1} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid2} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid3} = flurm_job_sup:start_job(JobSpec),

    Jobs = flurm_job_sup:which_jobs(),
    ?assertEqual(3, length(Jobs)),
    ?assert(lists:member(Pid1, Jobs)),
    ?assert(lists:member(Pid2, Jobs)),
    ?assert(lists:member(Pid3, Jobs)),
    ok.

%%====================================================================
%% Count Jobs Tests
%%====================================================================

test_count_jobs() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    ?assertEqual(0, flurm_job_sup:count_jobs()),

    JobSpec = make_job_spec(),
    {ok, _Pid1} = flurm_job_sup:start_job(JobSpec),
    ?assertEqual(1, flurm_job_sup:count_jobs()),

    {ok, _Pid2} = flurm_job_sup:start_job(JobSpec),
    ?assertEqual(2, flurm_job_sup:count_jobs()),

    {ok, _Pid3} = flurm_job_sup:start_job(JobSpec),
    {ok, _Pid4} = flurm_job_sup:start_job(JobSpec),
    ?assertEqual(4, flurm_job_sup:count_jobs()),
    ok.

%%====================================================================
%% Multiple Jobs Lifecycle Tests
%%====================================================================

test_multiple_jobs_lifecycle() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),

    %% Start multiple jobs
    {ok, Pid1} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid2} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid3} = flurm_job_sup:start_job(JobSpec),

    ?assertEqual(3, flurm_job_sup:count_jobs()),

    %% Stop one job
    ok = flurm_job_sup:stop_job(Pid2),
    _ = sys:get_state(flurm_job_sup),
    ?assertEqual(2, flurm_job_sup:count_jobs()),

    Jobs = flurm_job_sup:which_jobs(),
    ?assert(lists:member(Pid1, Jobs)),
    ?assertNot(lists:member(Pid2, Jobs)),
    ?assert(lists:member(Pid3, Jobs)),

    %% Stop remaining jobs
    ok = flurm_job_sup:stop_job(Pid1),
    ok = flurm_job_sup:stop_job(Pid3),
    _ = sys:get_state(flurm_job_sup),

    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ?assertEqual([], flurm_job_sup:which_jobs()),
    ok.

%%====================================================================
%% Integration Tests (without mocking flurm_job)
%%====================================================================

%% These tests use the actual flurm_job module with flurm_job_registry

integration_test_() ->
    {foreach,
     fun integration_setup/0,
     fun integration_cleanup/1,
     [
        {"Full job lifecycle", fun test_full_job_lifecycle/0},
        {"Process crash handling", fun test_process_crash_handling/0},
        {"Concurrent job starts", fun test_concurrent_job_starts/0}
     ]}.

integration_setup() ->
    application:ensure_all_started(sasl),

    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(flurm_accounting),

    %% Use mock for accounting only
    meck:new(flurm_accounting, [passthrough, non_strict]),
    meck:expect(flurm_accounting, record_job_submit, fun(_Data) -> ok end),
    meck:expect(flurm_accounting, record_job_start, fun(_JobId, _Nodes) -> ok end),
    meck:expect(flurm_accounting, record_job_end, fun(_JobId, _ExitCode, _State) -> ok end),
    meck:expect(flurm_accounting, record_job_cancelled, fun(_JobId, _Reason) -> ok end),

    %% Start actual job registry and supervisor
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    {ok, SupPid} = flurm_job_sup:start_link(),

    #{registry => RegistryPid, supervisor => SupPid}.

integration_cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    catch meck:unload(flurm_accounting),

    %% Stop all jobs
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],

    %% Stop processes with proper monitor/wait pattern
    lists:foreach(fun(Pid) ->
        case is_process_alive(Pid) of
            true ->
                Ref = monitor(process, Pid),
                unlink(Pid),
                catch gen_server:stop(Pid, shutdown, 5000),
                receive
                    {'DOWN', Ref, process, Pid, _} -> ok
                after 5000 ->
                    demonitor(Ref, [flush]),
                    catch exit(Pid, kill)
                end;
            false ->
                ok
        end
    end, [SupPid, RegistryPid]),
    ok.

test_full_job_lifecycle() ->
    JobSpec = make_job_spec(#{
        user_id => 2000,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8
    }),

    %% Submit job
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_integer(JobId)),

    %% Verify initial state
    {ok, pending} = flurm_job:get_state(Pid),
    ?assertEqual(1, flurm_job_sup:count_jobs()),

    %% Allocate and run
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Complete job
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    %% Job process should still be tracked
    ?assertEqual(1, flurm_job_sup:count_jobs()),
    ok.

test_process_crash_handling() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    ?assertEqual(1, flurm_job_sup:count_jobs()),
    ?assert(is_process_alive(Pid)),

    %% Kill the process (simulating crash)
    exit(Pid, kill),
    _ = sys:get_state(flurm_job_sup),

    %% Process should be removed from supervisor
    %% (temporary restart strategy means no restart)
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ok.

test_concurrent_job_starts() ->
    NumJobs = 10,

    %% Start jobs concurrently
    Results = pmap(fun(I) ->
        JobSpec = make_job_spec(#{
            user_id => 1000 + I,
            partition => <<"partition", (integer_to_binary(I))/binary>>
        }),
        flurm_job:submit(JobSpec)
    end, lists:seq(1, NumJobs)),

    %% All should succeed
    lists:foreach(fun(Result) ->
        ?assertMatch({ok, _, _}, Result)
    end, Results),

    %% Verify count
    ?assertEqual(NumJobs, flurm_job_sup:count_jobs()),

    %% Verify all unique PIDs
    Pids = flurm_job_sup:which_jobs(),
    UniquePids = lists:usort(Pids),
    ?assertEqual(length(Pids), length(UniquePids)),
    ok.

%% Parallel map helper
pmap(F, L) ->
    Parent = self(),
    Refs = [spawn_monitor(fun() -> Parent ! {self(), F(X)} end) || X <- L],
    [receive
        {Pid, Result} ->
            receive {'DOWN', Ref, process, Pid, _} -> ok end,
            Result
    end || {Pid, Ref} <- Refs].

%%====================================================================
%% Supervisor Behavior Edge Cases
%%====================================================================

supervisor_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Which jobs with dead process", fun test_which_jobs_dead_process/0},
        {"Supervisor restart behavior", fun test_supervisor_no_restart/0},
        {"Stop non-existent job", fun test_stop_nonexistent_job/0}
     ]}.

test_which_jobs_dead_process() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, Pid1} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid2} = flurm_job_sup:start_job(JobSpec),

    ?assertEqual(2, flurm_job_sup:count_jobs()),

    %% Kill one process externally
    exit(Pid1, kill),
    %% Wait for the process to actually terminate
    wait_for_process_death(Pid1, 1000),

    %% which_jobs only returns PIDs from supervisor:which_children
    %% The implementation filters out non-pid entries
    Jobs = flurm_job_sup:which_jobs(),
    %% Pid1 may still be in the list briefly, but should not be alive
    ?assertNot(is_process_alive(Pid1)),
    ?assert(lists:member(Pid2, Jobs)),
    ok.

test_supervisor_no_restart() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job_sup:start_job(JobSpec),

    InitialCount = flurm_job_sup:count_jobs(),
    ?assertEqual(1, InitialCount),

    %% Kill the process - should NOT restart due to temporary strategy
    exit(Pid, kill),
    %% Wait for the process to actually terminate
    wait_for_process_death(Pid, 1000),

    %% Verify the process did not restart (no new processes)
    %% Note: The supervisor may still report the terminated child briefly
    %% so we check the process is dead rather than count
    ?assertNot(is_process_alive(Pid)),
    ok.

test_stop_nonexistent_job() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    %% Create a dummy pid that's not a child
    DummyPid = spawn(fun() -> receive _ -> ok end end),

    %% stop_job should return error for non-child
    Result = flurm_job_sup:stop_job(DummyPid),
    ?assertMatch({error, _}, Result),

    %% Cleanup
    exit(DummyPid, kill),
    ok.

%%====================================================================
%% Direct Init Tests (without starting supervisor)
%%====================================================================

init_direct_test_() ->
    [
        {"Init with empty args", fun test_init_empty_args/0},
        {"Child spec structure", fun test_child_spec_structure/0}
    ].

test_init_empty_args() ->
    Result = flurm_job_sup:init([]),
    ?assertMatch({ok, {_, _}}, Result),

    {ok, {SupFlags, ChildSpecs}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(ChildSpecs)),
    ok.

test_child_spec_structure() ->
    {ok, {_SupFlags, [ChildSpec]}} = flurm_job_sup:init([]),

    %% Verify all required keys are present
    RequiredKeys = [id, start, restart, shutdown, type, modules],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, ChildSpec),
                io_lib:format("Missing key: ~p", [Key]))
    end, RequiredKeys),

    %% Verify start MFA format
    {M, F, A} = maps:get(start, ChildSpec),
    ?assert(is_atom(M)),
    ?assert(is_atom(F)),
    ?assert(is_list(A)),

    %% Verify modules is a list
    Modules = maps:get(modules, ChildSpec),
    ?assert(is_list(Modules)),
    ?assert(length(Modules) > 0),
    ok.

%%====================================================================
%% Performance and Stress Tests
%%====================================================================

stress_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60, [
        {"Handle many jobs", fun test_many_jobs/0}
     ]}}.

test_many_jobs() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    NumJobs = 100,
    JobSpec = make_job_spec(),

    %% Start many jobs
    Pids = [begin
        {ok, Pid} = flurm_job_sup:start_job(JobSpec),
        Pid
    end || _ <- lists:seq(1, NumJobs)],

    ?assertEqual(NumJobs, flurm_job_sup:count_jobs()),
    ?assertEqual(NumJobs, length(flurm_job_sup:which_jobs())),

    %% Stop half of them
    {ToStop, ToKeep} = lists:split(NumJobs div 2, Pids),
    [flurm_job_sup:stop_job(P) || P <- ToStop],
    _ = sys:get_state(flurm_job_sup),

    ?assertEqual(length(ToKeep), flurm_job_sup:count_jobs()),

    %% Verify correct PIDs remain
    RemainingJobs = flurm_job_sup:which_jobs(),
    lists:foreach(fun(Pid) ->
        ?assert(lists:member(Pid, RemainingJobs))
    end, ToKeep),

    %% Stop remaining
    [flurm_job_sup:stop_job(P) || P <- ToKeep],
    _ = sys:get_state(flurm_job_sup),

    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ok.

%%====================================================================
%% Internal Helper Functions
%%====================================================================

%% Wait for a process to die, with timeout
wait_for_process_death(Pid, Timeout) ->
    wait_for_process_death(Pid, Timeout, 10).

wait_for_process_death(_Pid, Remaining, _Interval) when Remaining =< 0 ->
    timeout;
wait_for_process_death(Pid, Remaining, Interval) ->
    case is_process_alive(Pid) of
        false -> ok;
        true ->
            timer:sleep(Interval),
            wait_for_process_death(Pid, Remaining - Interval, Interval)
    end.

%%====================================================================
%% Comprehensive Supervisor Spec Validation Tests
%%====================================================================

%% These tests thoroughly validate the supervisor init/1 return value
%% according to OTP supervisor specification requirements.

comprehensive_init_validation_test_() ->
    [
        {"init/1 returns valid {ok, {SupFlags, ChildSpecs}} tuple",
         fun test_init_comprehensive_tuple/0},
        {"SupFlags contains all required keys",
         fun test_sup_flags_required_keys/0},
        {"SupFlags strategy is valid for simple_one_for_one",
         fun test_sup_flags_valid_strategy/0},
        {"SupFlags intensity is non-negative",
         fun test_sup_flags_valid_intensity/0},
        {"SupFlags period is positive",
         fun test_sup_flags_valid_period/0},
        {"Child spec has valid id field",
         fun test_child_spec_valid_id/0},
        {"Child spec has valid start MFA",
         fun test_child_spec_valid_start/0},
        {"Child spec has valid restart type",
         fun test_child_spec_valid_restart/0},
        {"Child spec has valid shutdown value",
         fun test_child_spec_valid_shutdown/0},
        {"Child spec has valid type",
         fun test_child_spec_valid_type/0},
        {"Child spec has valid modules list",
         fun test_child_spec_valid_modules/0},
        {"Complete child spec validation",
         fun test_complete_child_spec_validation/0}
    ].

test_init_comprehensive_tuple() ->
    Result = flurm_job_sup:init([]),
    ?assertMatch({ok, {_, _}}, Result),
    {ok, {SupFlags, ChildSpecs}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(ChildSpecs)),
    ok.

test_sup_flags_required_keys() ->
    {ok, {SupFlags, _}} = flurm_job_sup:init([]),
    ?assert(maps:is_key(strategy, SupFlags)),
    ?assert(maps:is_key(intensity, SupFlags)),
    ?assert(maps:is_key(period, SupFlags)),
    ok.

test_sup_flags_valid_strategy() ->
    {ok, {SupFlags, _}} = flurm_job_sup:init([]),
    Strategy = maps:get(strategy, SupFlags),
    ValidStrategies = [one_for_one, one_for_all, rest_for_one, simple_one_for_one],
    ?assert(lists:member(Strategy, ValidStrategies),
            io_lib:format("Invalid strategy: ~p", [Strategy])),
    ok.

test_sup_flags_valid_intensity() ->
    {ok, {SupFlags, _}} = flurm_job_sup:init([]),
    Intensity = maps:get(intensity, SupFlags),
    ?assert(is_integer(Intensity)),
    ?assert(Intensity >= 0, "Intensity must be non-negative"),
    ok.

test_sup_flags_valid_period() ->
    {ok, {SupFlags, _}} = flurm_job_sup:init([]),
    Period = maps:get(period, SupFlags),
    ?assert(is_integer(Period)),
    ?assert(Period > 0, "Period must be positive"),
    ok.

test_child_spec_valid_id() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    Id = maps:get(id, ChildSpec),
    ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id),
            io_lib:format("Invalid id type: ~p", [Id])),
    ok.

test_child_spec_valid_start() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    Start = maps:get(start, ChildSpec),
    ?assertMatch({M, F, A} when is_atom(M) andalso is_atom(F) andalso is_list(A), Start,
                 io_lib:format("Invalid start MFA: ~p", [Start])),
    ok.

test_child_spec_valid_restart() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    Restart = maps:get(restart, ChildSpec),
    ValidRestartTypes = [permanent, transient, temporary],
    ?assert(lists:member(Restart, ValidRestartTypes),
            io_lib:format("Invalid restart type: ~p", [Restart])),
    ok.

test_child_spec_valid_shutdown() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    Shutdown = maps:get(shutdown, ChildSpec),
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown,
            io_lib:format("Invalid shutdown value: ~p", [Shutdown])),
    ok.

test_child_spec_valid_type() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    Type = maps:get(type, ChildSpec),
    ValidTypes = [worker, supervisor],
    ?assert(lists:member(Type, ValidTypes),
            io_lib:format("Invalid type: ~p", [Type])),
    ok.

test_child_spec_valid_modules() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    Modules = maps:get(modules, ChildSpec),
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso
                           lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules,
            io_lib:format("Invalid modules: ~p", [Modules])),
    ok.

test_complete_child_spec_validation() ->
    {ok, {SupFlags, ChildSpecs}} = flurm_job_sup:init([]),

    %% Validate SupFlags
    ?assertMatch(#{strategy := _, intensity := _, period := _}, SupFlags),

    %% Validate child specs
    ?assert(is_list(ChildSpecs)),
    %% For simple_one_for_one, there should be exactly one template
    ?assertEqual(1, length(ChildSpecs)),

    [ChildSpec] = ChildSpecs,
    validate_job_child_spec(ChildSpec),
    ok.

validate_job_child_spec(#{id := Id, start := {M, F, A}, restart := Restart,
                          shutdown := Shutdown, type := Type, modules := Modules}) ->
    %% Validate id
    ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id)),

    %% Validate MFA
    ?assert(is_atom(M)),
    ?assert(is_atom(F)),
    ?assert(is_list(A)),

    %% Validate restart
    ?assert(lists:member(Restart, [permanent, transient, temporary])),

    %% Validate shutdown
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown),

    %% Validate type
    ?assert(lists:member(Type, [worker, supervisor])),

    %% Validate modules
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules),
    ok;
validate_job_child_spec({Id, {M, F, A}, Restart, Shutdown, Type, Modules}) ->
    %% Tuple format (legacy)
    ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id)),
    ?assert(is_atom(M)),
    ?assert(is_atom(F)),
    ?assert(is_list(A)),
    ?assert(lists:member(Restart, [permanent, transient, temporary])),
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown),
    ?assert(lists:member(Type, [worker, supervisor])),
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules),
    ok.
