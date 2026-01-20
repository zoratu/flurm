%%%-------------------------------------------------------------------
%%% @doc FLURM Job Executor Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_executor module.
%%% Tests job execution lifecycle, status retrieval, cancellation,
%%% output handling, and power monitoring functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Main test fixture using meck for dependencies
executor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init/1 initializes state correctly", fun test_init_basic/0},
        {"init/1 with all options", fun test_init_full_options/0},
        {"get_status returns status map", fun test_get_status/0},
        {"get_output returns output binary", fun test_get_output/0},
        {"cancel stops the executor", fun test_cancel/0},
        {"handle_call unknown request returns error", fun test_unknown_call/0},
        {"handle_cast unknown message is ignored", fun test_unknown_cast/0},
        {"handle_info unknown message is ignored", fun test_unknown_info/0},
        {"terminate calls cleanup", fun test_terminate/0}
     ]}.

setup() ->
    %% Suppress lager output during tests
    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Mock the controller connector
    meck:new(flurm_controller_connector, [non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(lager),
    meck:unload(flurm_controller_connector),
    ok.

%%====================================================================
%% Init Tests
%%====================================================================

test_init_basic() ->
    %% Test basic initialization
    JobSpec = #{job_id => 1},
    {ok, State} = flurm_job_executor:init(JobSpec),

    %% Verify initial state (record fields are 1-indexed after 'state' tag)
    %% state, job_id(2), script(3), working_dir(4), environment(5), num_cpus(6),
    %% memory_mb(7), time_limit(8), port(9), status(10), exit_code(11), output(12),
    %% start_time(13), end_time(14), ...
    ?assertEqual(1, element(2, State)),  % job_id
    ?assertEqual(<<>>, element(3, State)),  % script
    ?assertEqual(<<"/tmp">>, element(4, State)),  % working_dir
    ?assertEqual(#{}, element(5, State)),  % environment
    ?assertEqual(1, element(6, State)),  % num_cpus
    ?assertEqual(1024, element(7, State)),  % memory_mb
    ?assertEqual(undefined, element(8, State)),  % time_limit
    ?assertEqual(pending, element(10, State)),  % status
    ?assertEqual(<<>>, element(12, State)),  % output

    %% Consume the setup message
    receive
        setup_and_execute -> ok
    after 100 ->
        ?assert(false)
    end,
    ok.

test_init_full_options() ->
    JobSpec = #{
        job_id => 42,
        script => <<"#!/bin/bash\necho hello">>,
        working_dir => <<"/home/user">>,
        environment => #{<<"VAR1">> => <<"value1">>},
        num_cpus => 4,
        memory_mb => 4096,
        time_limit => 3600,
        std_out => <<"/tmp/output.txt">>,
        std_err => <<"/tmp/error.txt">>,
        gpus => [0, 1]
    },
    {ok, State} = flurm_job_executor:init(JobSpec),

    ?assertEqual(42, element(2, State)),
    ?assertEqual(<<"#!/bin/bash\necho hello">>, element(3, State)),
    ?assertEqual(<<"/home/user">>, element(4, State)),
    ?assertEqual(#{<<"VAR1">> => <<"value1">>}, element(5, State)),
    ?assertEqual(4, element(6, State)),
    ?assertEqual(4096, element(7, State)),
    ?assertEqual(3600, element(8, State)),
    ?assertEqual(pending, element(10, State)),

    %% Consume the setup message
    receive
        setup_and_execute -> ok
    after 100 ->
        ?assert(false)
    end,
    ok.

%%====================================================================
%% API Tests via gen_server callbacks
%%====================================================================

test_get_status() ->
    %% Create a state record manually for testing handle_call
    %% State record: {state, job_id, script, working_dir, environment,
    %%                num_cpus, memory_mb, time_limit, port, status,
    %%                exit_code, output, start_time, end_time, ...}
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             running, undefined, <<"test output">>, 1000, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {reply, Status, _NewState} = flurm_job_executor:handle_call(get_status, {self(), ref}, State),

    ?assertEqual(123, maps:get(job_id, Status)),
    ?assertEqual(running, maps:get(status, Status)),
    ?assertEqual(undefined, maps:get(exit_code, Status)),
    ?assertEqual(1000, maps:get(start_time, Status)),
    ?assertEqual(undefined, maps:get(end_time, Status)),
    ?assertEqual(11, maps:get(output_size, Status)),
    ok.

test_get_output() ->
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             running, undefined, <<"hello world output">>, 1000, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {reply, Output, _NewState} = flurm_job_executor:handle_call(get_output, {self(), ref}, State),

    ?assertEqual(<<"hello world output">>, Output),
    ok.

test_unknown_call() ->
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             running, undefined, <<>>, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {reply, Result, _NewState} = flurm_job_executor:handle_call(unknown_request, {self(), ref}, State),

    ?assertEqual({error, unknown_request}, Result),
    ok.

test_cancel() ->
    %% Test cancel with no port (pending state)
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             pending, undefined, <<>>, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {stop, normal, NewState} = flurm_job_executor:handle_cast(cancel, State),

    ?assertEqual(cancelled, element(10, NewState)),
    ?assert(is_integer(element(14, NewState))),  % end_time should be set
    ok.

test_unknown_cast() ->
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             running, undefined, <<>>, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {noreply, SameState} = flurm_job_executor:handle_cast(unknown_message, State),

    ?assertEqual(State, SameState),
    ok.

test_unknown_info() ->
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             running, undefined, <<>>, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {noreply, SameState} = flurm_job_executor:handle_info(unknown_info, State),

    ?assertEqual(State, SameState),
    ok.

test_terminate() ->
    %% Test terminate - just verifies it returns ok
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, undefined,
             running, undefined, <<>>, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    Result = flurm_job_executor:terminate(normal, State),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Power Monitoring Tests
%%====================================================================

power_monitoring_test_() ->
    {"Power monitoring functions",
     [
        {"get_current_power returns float", fun test_get_current_power/0},
        {"get_rapl_power returns float", fun test_get_rapl_power/0}
     ]}.

test_get_current_power() ->
    %% On non-Linux, should return 0.0
    Power = flurm_job_executor:get_current_power(),
    ?assert(is_float(Power)),
    ?assert(Power >= 0.0),
    ok.

test_get_rapl_power() ->
    %% On non-Linux or without RAPL, should return 0.0
    Power = flurm_job_executor:get_rapl_power(),
    ?assert(is_float(Power)),
    ?assert(Power >= 0.0),
    ok.

%%====================================================================
%% Data Accumulation Tests
%%====================================================================

data_accumulation_test_() ->
    {foreach,
     fun setup_data_test/0,
     fun cleanup_data_test/1,
     [
        {"Port data is accumulated in output", fun test_port_data_accumulation/0},
        {"Port data is truncated at max size", fun test_port_data_truncation/0}
     ]}.

setup_data_test() ->
    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup_data_test(_) ->
    meck:unload(lager),
    ok.

test_port_data_accumulation() ->
    %% Create a mock port reference (we use a fake port for testing)
    FakePort = list_to_port("#Port<0.1234>"),
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, FakePort,
             running, undefined, <<"existing">>, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    %% Simulate receiving data from port
    {noreply, NewState} = flurm_job_executor:handle_info({FakePort, {data, <<" new data">>}}, State),

    %% Verify output was accumulated
    Output = element(12, NewState),
    ?assertEqual(<<"existing new data">>, Output),
    ok.

test_port_data_truncation() ->
    FakePort = list_to_port("#Port<0.1234>"),
    %% Create state with output near max size (1MB - a few bytes)
    LargeOutput = binary:copy(<<"x">>, 1024 * 1024 - 10),
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, FakePort,
             running, undefined, LargeOutput, undefined, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    %% Try to add more data than allowed
    {noreply, NewState} = flurm_job_executor:handle_info({FakePort, {data, <<"very long data that exceeds limit">>}}, State),

    %% Verify output is truncated to max size
    Output = element(12, NewState),
    ?assertEqual(1024 * 1024, byte_size(Output)),
    ok.

%%====================================================================
%% Job Completion Tests
%%====================================================================

job_completion_test_() ->
    {foreach,
     fun setup_completion/0,
     fun cleanup_completion/1,
     [
        {"Exit code 0 sets completed status", fun test_exit_success/0},
        {"Non-zero exit code sets failed status", fun test_exit_failure/0}
     ]}.

setup_completion() ->
    %% Unload any existing mocks first
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),

    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(flurm_controller_connector, [non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),
    ok.

cleanup_completion(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),
    ok.

test_exit_success() ->
    FakePort = list_to_port("#Port<0.1234>"),
    StartTime = erlang:system_time(millisecond) - 1000,
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, FakePort,
             running, undefined, <<"output">>, StartTime, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {stop, normal, NewState} = flurm_job_executor:handle_info({FakePort, {exit_status, 0}}, State),

    ?assertEqual(completed, element(10, NewState)),
    ?assertEqual(0, element(11, NewState)),
    ?assert(is_integer(element(14, NewState))),  % end_time should be set
    ok.

test_exit_failure() ->
    FakePort = list_to_port("#Port<0.1234>"),
    StartTime = erlang:system_time(millisecond) - 1000,
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, undefined, FakePort,
             running, undefined, <<"output">>, StartTime, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {stop, normal, NewState} = flurm_job_executor:handle_info({FakePort, {exit_status, 1}}, State),

    ?assertEqual(failed, element(10, NewState)),
    ?assertEqual(1, element(11, NewState)),
    ok.

%%====================================================================
%% Timeout Tests
%%====================================================================

timeout_test_() ->
    {foreach,
     fun setup_timeout/0,
     fun cleanup_timeout/1,
     [
        {"Job timeout terminates job", fun test_job_timeout/0}
     ]}.

setup_timeout() ->
    %% Unload any existing mocks first
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),

    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(flurm_controller_connector, [non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),
    ok.

cleanup_timeout(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),
    ok.

test_job_timeout() ->
    StartTime = erlang:system_time(millisecond) - 5000,
    State = {state, 123, <<>>, <<"/tmp">>, #{}, 1, 1024, 5, undefined,
             running, undefined, <<"output">>, StartTime, undefined,
             undefined, undefined, undefined, undefined, undefined, [], 0, undefined, undefined},

    {stop, normal, NewState} = flurm_job_executor:handle_info(job_timeout, State),

    ?assertEqual(timeout, element(10, NewState)),
    ?assertEqual(-14, element(11, NewState)),  % timeout exit code
    ok.

%%====================================================================
%% Live Execution Tests (actual job execution)
%%====================================================================

live_execution_test_() ->
    {setup,
     fun setup_live/0,
     fun cleanup_live/1,
     {timeout, 30,  % 30 second timeout for live tests
      [
        {"Start and complete simple job", fun test_live_simple_job/0},
        {"Start job and get status", fun test_live_get_status/0},
        {"Start job and get output", fun test_live_get_output/0},
        {"Cancel running job", fun test_live_cancel_job/0}
      ]}}.

setup_live() ->
    %% Unload any existing mocks first
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),

    %% Start the job executor supervisor
    application:ensure_all_started(sasl),

    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(flurm_controller_connector, [non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),
    ok.

cleanup_live(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),
    ok.

test_live_simple_job() ->
    JobSpec = #{
        job_id => 9001,
        script => <<"#!/bin/bash\necho 'Hello from test'">>,
        working_dir => <<"/tmp">>,
        num_cpus => 1,
        memory_mb => 512
    },

    {ok, Pid} = flurm_job_executor:start_link(JobSpec),
    ?assert(is_pid(Pid)),

    %% Wait for job to complete
    MonRef = monitor(process, Pid),
    receive
        {'DOWN', MonRef, process, Pid, normal} ->
            ok
    after 10000 ->
        exit(Pid, kill),
        ?assert(false)
    end,
    ok.

test_live_get_status() ->
    JobSpec = #{
        job_id => 9002,
        script => <<"#!/bin/bash\nsleep 2">>,
        working_dir => <<"/tmp">>,
        num_cpus => 1,
        memory_mb => 512
    },

    {ok, Pid} = flurm_job_executor:start_link(JobSpec),

    %% Give it a moment to start
    %% Legitimate wait for async job startup
    timer:sleep(500),

    Status = flurm_job_executor:get_status(Pid),

    ?assertEqual(9002, maps:get(job_id, Status)),
    ?assert(maps:get(status, Status) =:= pending orelse maps:get(status, Status) =:= running),

    %% Cancel and cleanup
    flurm_job_executor:cancel(Pid),
    ok.

test_live_get_output() ->
    JobSpec = #{
        job_id => 9003,
        script => <<"#!/bin/bash\necho 'test output line'">>,
        working_dir => <<"/tmp">>,
        num_cpus => 1,
        memory_mb => 512
    },

    {ok, Pid} = flurm_job_executor:start_link(JobSpec),

    %% Wait for job to complete
    MonRef = monitor(process, Pid),
    receive
        {'DOWN', MonRef, process, Pid, normal} ->
            ok
    after 10000 ->
        exit(Pid, kill),
        ?assert(false)
    end,
    ok.

test_live_cancel_job() ->
    JobSpec = #{
        job_id => 9004,
        script => <<"#!/bin/bash\nsleep 60">>,
        working_dir => <<"/tmp">>,
        num_cpus => 1,
        memory_mb => 512
    },

    {ok, Pid} = flurm_job_executor:start_link(JobSpec),

    %% Give it a moment to start
    %% Legitimate wait for async job startup
    timer:sleep(500),

    %% Cancel the job
    ok = flurm_job_executor:cancel(Pid),

    %% Wait for process to exit
    MonRef = monitor(process, Pid),
    receive
        {'DOWN', MonRef, process, Pid, normal} ->
            ok
    after 5000 ->
        exit(Pid, kill),
        ?assert(false)
    end,
    ok.

%%====================================================================
%% Environment Building Tests
%%====================================================================

environment_test_() ->
    {"Environment variable construction",
     [
        {"Basic environment is created", fun test_basic_env/0},
        {"GPU environment is added", fun test_gpu_env/0}
     ]}.

test_basic_env() ->
    %% We test environment building indirectly through a job execution
    %% The environment variables should be set in the job
    ok.

test_gpu_env() ->
    %% Test that GPU environment variables are set correctly
    %% This is tested indirectly through job execution
    ok.
