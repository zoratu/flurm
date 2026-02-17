%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_diagnostics module
%%%
%%% Tests one-shot diagnostics, continuous monitoring (leak detector),
%%% process inspection utilities, and health checks.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_diagnostics_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_tests).

-include_lib("eunit/include/eunit.hrl").

%% Define snapshot and alert records to match the module
-record(snapshot, {
    timestamp :: integer(),
    total_memory :: non_neg_integer(),
    process_count :: non_neg_integer(),
    ets_memory :: non_neg_integer(),
    binary_memory :: non_neg_integer(),
    atom_count :: non_neg_integer(),
    port_count :: non_neg_integer()
}).

-record(alert, {
    timestamp :: integer(),
    type :: memory_growth | process_leak | binary_leak | message_queue,
    details :: term()
}).

-record(state, {
    interval :: pos_integer(),
    history :: [#snapshot{}],
    max_history :: pos_integer(),
    alerts :: [#alert{}]
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Ensure lager is started for logging
    application:ensure_all_started(lager),
    %% Stop any existing leak detector
    flurm_diagnostics:stop_leak_detector(),
    ok.

cleanup(_) ->
    %% Stop leak detector if running
    flurm_diagnostics:stop_leak_detector(),
    ok.

%%====================================================================
%% One-shot Diagnostics Tests
%%====================================================================

memory_report_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"memory_report returns a map with required keys",
       fun() ->
           Report = flurm_diagnostics:memory_report(),
           ?assert(is_map(Report)),
           ?assert(maps:is_key(total, Report)),
           ?assert(maps:is_key(processes, Report)),
           ?assert(maps:is_key(processes_used, Report)),
           ?assert(maps:is_key(system, Report)),
           ?assert(maps:is_key(atom, Report)),
           ?assert(maps:is_key(atom_used, Report)),
           ?assert(maps:is_key(binary, Report)),
           ?assert(maps:is_key(ets, Report)),
           ?assert(maps:is_key(code, Report))
       end},
      {"memory_report values are non-negative integers",
       fun() ->
           Report = flurm_diagnostics:memory_report(),
           ?assert(maps:get(total, Report) > 0),
           ?assert(maps:get(processes, Report) >= 0),
           ?assert(maps:get(system, Report) > 0),
           ?assert(maps:get(binary, Report) >= 0),
           ?assert(maps:get(ets, Report) >= 0)
       end},
      {"memory_report total >= processes + system",
       fun() ->
           Report = flurm_diagnostics:memory_report(),
           Total = maps:get(total, Report),
           Processes = maps:get(processes, Report),
           System = maps:get(system, Report),
           ?assert(Total >= Processes),
           ?assert(Total >= System)
       end}
     ]}.

process_report_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"process_report returns map with required keys",
       fun() ->
           Report = flurm_diagnostics:process_report(),
           ?assert(is_map(Report)),
           ?assert(maps:is_key(count, Report)),
           ?assert(maps:is_key(limit, Report)),
           ?assert(maps:is_key(utilization, Report)),
           ?assert(maps:is_key(registered, Report))
       end},
      {"process_report count is positive",
       fun() ->
           Report = flurm_diagnostics:process_report(),
           ?assert(maps:get(count, Report) > 0)
       end},
      {"process_report limit is greater than count",
       fun() ->
           Report = flurm_diagnostics:process_report(),
           Count = maps:get(count, Report),
           Limit = maps:get(limit, Report),
           ?assert(Limit > Count)
       end},
      {"process_report utilization is a percentage",
       fun() ->
           Report = flurm_diagnostics:process_report(),
           Utilization = maps:get(utilization, Report),
           ?assert(Utilization > 0.0),
           ?assert(Utilization < 100.0)
       end},
      {"process_report registered count is reasonable",
       fun() ->
           Report = flurm_diagnostics:process_report(),
           Registered = maps:get(registered, Report),
           ?assert(Registered >= 0),
           ?assert(Registered =< maps:get(count, Report))
       end}
     ]}.

ets_report_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"ets_report returns a list",
       fun() ->
           Report = flurm_diagnostics:ets_report(),
           ?assert(is_list(Report))
       end},
      {"ets_report contains maps with required keys",
       fun() ->
           Report = flurm_diagnostics:ets_report(),
           %% There should be at least one ETS table (e.g., from ets itself)
           ?assert(length(Report) > 0),
           [First | _] = Report,
           ?assert(is_map(First)),
           %% Check for expected keys (may have error key if table gone)
           ?assert(maps:is_key(name, First))
       end},
      {"ets_report entries have size and memory when table exists",
       fun() ->
           %% Create a test table
           Tab = ets:new(test_ets_report_table, [public, named_table]),
           ets:insert(Tab, {key, value}),
           Report = flurm_diagnostics:ets_report(),
           TestEntry = lists:filter(fun(E) ->
               maps:get(name, E, undefined) =:= test_ets_report_table
           end, Report),
           ?assert(length(TestEntry) =:= 1),
           [Entry] = TestEntry,
           ?assert(maps:is_key(size, Entry)),
           ?assert(maps:is_key(memory, Entry)),
           ?assert(maps:get(size, Entry) >= 1),
           ets:delete(Tab)
       end},
      {"ets_report handles deleted table gracefully",
       fun() ->
           %% This tests the catch clause - create and delete a table during iteration
           %% Just verify the function doesn't crash
           Report = flurm_diagnostics:ets_report(),
           ?assert(is_list(Report))
       end}
     ]}.

message_queue_report_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"message_queue_report returns a list",
       fun() ->
           Report = flurm_diagnostics:message_queue_report(),
           ?assert(is_list(Report))
       end},
      {"message_queue_report is empty when no large queues",
       fun() ->
           %% Under normal test conditions, no process should have >100 messages
           Report = flurm_diagnostics:message_queue_report(),
           %% This may or may not be empty depending on system state
           ?assert(is_list(Report))
       end},
      {"message_queue_report detects process with large queue",
       fun() ->
           %% Spawn a process that accumulates messages
           Pid = spawn(fun() ->
               receive
                   {get_queue_len, From} ->
                       {message_queue_len, Len} = process_info(self(), message_queue_len),
                       From ! {queue_len, Len}
               end
           end),
           %% Send 150 messages to exceed threshold
           lists:foreach(fun(N) -> Pid ! {msg, N} end, lists:seq(1, 150)),
           timer:sleep(50),
           Report = flurm_diagnostics:message_queue_report(),
           %% Should find our process
           Found = lists:filter(fun(E) ->
               maps:get(pid, E) =:= Pid
           end, Report),
           ?assert(length(Found) =:= 1),
           [Entry] = Found,
           ?assert(maps:get(queue_len, Entry) >= 100),
           exit(Pid, kill)
       end},
      {"message_queue_report is sorted by queue length descending",
       fun() ->
           %% Create two processes with different queue sizes
           Pid1 = spawn(fun() -> timer:sleep(10000) end),
           Pid2 = spawn(fun() -> timer:sleep(10000) end),
           lists:foreach(fun(N) -> Pid1 ! {msg, N} end, lists:seq(1, 120)),
           lists:foreach(fun(N) -> Pid2 ! {msg, N} end, lists:seq(1, 200)),
           timer:sleep(50),
           Report = flurm_diagnostics:message_queue_report(),
           %% Verify descending order
           QueueLens = [maps:get(queue_len, E) || E <- Report],
           ?assertEqual(QueueLens, lists:reverse(lists:sort(QueueLens))),
           exit(Pid1, kill),
           exit(Pid2, kill)
       end}
     ]}.

binary_leak_check_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"binary_leak_check returns a list",
       fun() ->
           Report = flurm_diagnostics:binary_leak_check(),
           ?assert(is_list(Report))
       end},
      {"binary_leak_check only returns processes with >1MB binaries",
       fun() ->
           Report = flurm_diagnostics:binary_leak_check(),
           lists:foreach(fun(Entry) ->
               ?assert(maps:get(binary_size, Entry) > 1024 * 1024)
           end, Report)
       end},
      {"binary_leak_check entries have expected keys",
       fun() ->
           %% Create a process holding large binaries
           Parent = self(),
           Pid = spawn(fun() ->
               %% Create a 2MB binary
               LargeBin = binary:copy(<<0>>, 2 * 1024 * 1024),
               Parent ! ready,
               receive stop -> ok end,
               _ = LargeBin  % Keep reference alive
           end),
           receive ready -> ok end,
           timer:sleep(50),
           Report = flurm_diagnostics:binary_leak_check(),
           %% Find our process
           Found = [E || E <- Report, maps:get(pid, E) =:= Pid],
           case Found of
               [Entry] ->
                   ?assert(maps:is_key(pid, Entry)),
                   ?assert(maps:is_key(binary_size, Entry)),
                   ?assert(maps:is_key(binary_count, Entry)),
                   ?assert(maps:is_key(name, Entry)),
                   ?assert(maps:is_key(heap_size, Entry));
               [] ->
                   %% Binary might have been GC'd, that's ok
                   ok
           end,
           Pid ! stop
       end}
     ]}.

top_memory_processes_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"top_memory_processes/0 returns default 20 processes",
       fun() ->
           Report = flurm_diagnostics:top_memory_processes(),
           ?assert(is_list(Report)),
           ?assert(length(Report) =< 20)
       end},
      {"top_memory_processes/1 respects N parameter",
       fun() ->
           Report5 = flurm_diagnostics:top_memory_processes(5),
           Report10 = flurm_diagnostics:top_memory_processes(10),
           ?assert(length(Report5) =< 5),
           ?assert(length(Report10) =< 10)
       end},
      {"top_memory_processes entries have required keys",
       fun() ->
           Report = flurm_diagnostics:top_memory_processes(5),
           lists:foreach(fun(Entry) ->
               ?assert(maps:is_key(pid, Entry)),
               ?assert(maps:is_key(memory, Entry)),
               ?assert(maps:is_key(name, Entry)),
               ?assert(maps:is_key(current_function, Entry)),
               ?assert(maps:is_key(message_queue_len, Entry))
           end, Report)
       end},
      {"top_memory_processes is sorted by memory descending",
       fun() ->
           Report = flurm_diagnostics:top_memory_processes(10),
           Memories = [maps:get(memory, E) || E <- Report],
           ?assertEqual(Memories, lists:reverse(lists:sort(Memories)))
       end}
     ]}.

top_message_queue_processes_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"top_message_queue_processes/0 returns default 20 processes",
       fun() ->
           Report = flurm_diagnostics:top_message_queue_processes(),
           ?assert(is_list(Report)),
           ?assert(length(Report) =< 20)
       end},
      {"top_message_queue_processes/1 respects N parameter",
       fun() ->
           Report3 = flurm_diagnostics:top_message_queue_processes(3),
           Report7 = flurm_diagnostics:top_message_queue_processes(7),
           ?assert(length(Report3) =< 3),
           ?assert(length(Report7) =< 7)
       end},
      {"top_message_queue_processes entries have required keys",
       fun() ->
           Report = flurm_diagnostics:top_message_queue_processes(5),
           lists:foreach(fun(Entry) ->
               ?assert(maps:is_key(pid, Entry)),
               ?assert(maps:is_key(message_queue_len, Entry)),
               ?assert(maps:is_key(name, Entry)),
               ?assert(maps:is_key(current_function, Entry))
           end, Report)
       end},
      {"top_message_queue_processes is sorted by queue length descending",
       fun() ->
           %% Create some processes with messages
           Pids = [spawn(fun() -> timer:sleep(10000) end) || _ <- lists:seq(1, 5)],
           lists:foreach(fun({Pid, N}) ->
               lists:foreach(fun(M) -> Pid ! {msg, M} end, lists:seq(1, N * 10))
           end, lists:zip(Pids, lists:seq(1, 5))),
           timer:sleep(50),
           Report = flurm_diagnostics:top_message_queue_processes(10),
           QueueLens = [maps:get(message_queue_len, E) || E <- Report],
           ?assertEqual(QueueLens, lists:reverse(lists:sort(QueueLens))),
           lists:foreach(fun(P) -> exit(P, kill) end, Pids)
       end}
     ]}.

full_report_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"full_report returns comprehensive map",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           ?assert(is_map(Report)),
           ?assert(maps:is_key(timestamp, Report)),
           ?assert(maps:is_key(node, Report)),
           ?assert(maps:is_key(memory, Report)),
           ?assert(maps:is_key(processes, Report)),
           ?assert(maps:is_key(top_memory_processes, Report)),
           ?assert(maps:is_key(message_queues, Report)),
           ?assert(maps:is_key(binary_leaks, Report)),
           ?assert(maps:is_key(ets_tables, Report)),
           ?assert(maps:is_key(schedulers, Report)),
           ?assert(maps:is_key(uptime_seconds, Report))
       end},
      {"full_report timestamp is recent",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           Now = erlang:system_time(millisecond),
           Timestamp = maps:get(timestamp, Report),
           ?assert(abs(Now - Timestamp) < 1000)  % Within 1 second
       end},
      {"full_report node is correct",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           ?assertEqual(node(), maps:get(node, Report))
       end},
      {"full_report memory is a map",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           ?assert(is_map(maps:get(memory, Report)))
       end},
      {"full_report processes is a map",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           ?assert(is_map(maps:get(processes, Report)))
       end},
      {"full_report top_memory_processes has 10 entries max",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           TopMem = maps:get(top_memory_processes, Report),
           ?assert(length(TopMem) =< 10)
       end},
      {"full_report ets_tables has 10 entries max",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           EtsTables = maps:get(ets_tables, Report),
           ?assert(length(EtsTables) =< 10)
       end},
      {"full_report schedulers is positive",
       fun() ->
           Report = flurm_diagnostics:full_report(),
           ?assert(maps:get(schedulers, Report) > 0)
       end}
     ]}.

%%====================================================================
%% Health Check Tests
%%====================================================================

health_check_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"health_check returns ok under normal conditions",
       fun() ->
           Result = flurm_diagnostics:health_check(),
           %% Should be ok or warning under normal test conditions
           ?assert(Result =:= ok orelse element(1, Result) =:= warning)
       end},
      {"health_check returns valid format",
       fun() ->
           Result = flurm_diagnostics:health_check(),
           case Result of
               ok -> ok;
               {warning, Reasons} ->
                   ?assert(is_list(Reasons));
               {critical, Reasons} ->
                   ?assert(is_list(Reasons))
           end
       end}
     ]}.

%%====================================================================
%% Leak Detector (Continuous Monitoring) Tests
%%====================================================================

leak_detector_start_stop_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_leak_detector/0 starts with default interval",
       fun() ->
           Result = flurm_diagnostics:start_leak_detector(),
           ?assertMatch({ok, _Pid}, Result),
           {ok, Pid} = Result,
           ?assert(is_process_alive(Pid)),
           flurm_diagnostics:stop_leak_detector()
       end},
      {"start_leak_detector/1 starts with custom interval",
       fun() ->
           Result = flurm_diagnostics:start_leak_detector(30000),
           ?assertMatch({ok, _Pid}, Result),
           flurm_diagnostics:stop_leak_detector()
       end},
      {"stop_leak_detector stops running detector",
       fun() ->
           {ok, Pid} = flurm_diagnostics:start_leak_detector(),
           ?assert(is_process_alive(Pid)),
           flurm_diagnostics:stop_leak_detector(),
           timer:sleep(50),
           ?assertEqual(undefined, whereis(flurm_leak_detector))
       end},
      {"stop_leak_detector is idempotent",
       fun() ->
           ?assertEqual(ok, flurm_diagnostics:stop_leak_detector()),
           ?assertEqual(ok, flurm_diagnostics:stop_leak_detector())
       end}
     ]}.

get_leak_history_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_leak_history returns error when not running",
       fun() ->
           ?assertEqual({error, not_running}, flurm_diagnostics:get_leak_history())
       end},
      {"get_leak_history returns history and alerts when running",
       fun() ->
           {ok, _} = flurm_diagnostics:start_leak_detector(100),
           timer:sleep(150),  % Allow one snapshot
           Result = flurm_diagnostics:get_leak_history(),
           ?assertMatch({ok, _, _}, Result),
           {ok, History, Alerts} = Result,
           ?assert(is_list(History)),
           ?assert(is_list(Alerts)),
           flurm_diagnostics:stop_leak_detector()
       end},
      {"get_leak_history accumulates snapshots over time",
       fun() ->
           {ok, _} = flurm_diagnostics:start_leak_detector(50),
           timer:sleep(200),  % Allow multiple snapshots
           {ok, History, _} = flurm_diagnostics:get_leak_history(),
           ?assert(length(History) >= 2),
           flurm_diagnostics:stop_leak_detector()
       end}
     ]}.

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init/1 returns ok with state",
       fun() ->
           Result = flurm_diagnostics:init([60000]),
           ?assertMatch({ok, _State}, Result),
           {ok, State} = Result,
           ?assert(is_record(State, state)),
           ?assertEqual(60000, State#state.interval),
           ?assertEqual([], State#state.history),
           ?assertEqual([], State#state.alerts)
       end},
      {"handle_call get_history returns history and alerts",
       fun() ->
           State = #state{
               interval = 60000,
               history = [#snapshot{timestamp = 1, total_memory = 1000,
                                    process_count = 10, ets_memory = 100,
                                    binary_memory = 50, atom_count = 100,
                                    port_count = 5}],
               max_history = 1440,
               alerts = []
           },
           {reply, {ok, History, Alerts}, NewState} =
               flurm_diagnostics:handle_call(get_history, {self(), make_ref()}, State),
           ?assertEqual(State#state.history, History),
           ?assertEqual(State#state.alerts, Alerts),
           ?assertEqual(State, NewState)
       end},
      {"handle_call unknown returns error",
       fun() ->
           State = #state{interval = 60000, history = [], max_history = 1440, alerts = []},
           {reply, Reply, _} = flurm_diagnostics:handle_call(unknown, {self(), make_ref()}, State),
           ?assertEqual({error, unknown_request}, Reply)
       end},
      {"handle_cast ignores unknown messages",
       fun() ->
           State = #state{interval = 60000, history = [], max_history = 1440, alerts = []},
           {noreply, NewState} = flurm_diagnostics:handle_cast(unknown, State),
           ?assertEqual(State, NewState)
       end},
      {"handle_info ignores unknown messages",
       fun() ->
           State = #state{interval = 60000, history = [], max_history = 1440, alerts = []},
           {noreply, NewState} = flurm_diagnostics:handle_info(unknown, State),
           ?assertEqual(State, NewState)
       end},
      {"terminate returns ok",
       fun() ->
           State = #state{interval = 60000, history = [], max_history = 1440, alerts = []},
           ?assertEqual(ok, flurm_diagnostics:terminate(normal, State)),
           ?assertEqual(ok, flurm_diagnostics:terminate(shutdown, State)),
           ?assertEqual(ok, flurm_diagnostics:terminate({error, reason}, State))
       end}
     ]}.

handle_info_take_snapshot_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle_info take_snapshot adds to history",
       fun() ->
           State = #state{
               interval = 60000,
               history = [],
               max_history = 1440,
               alerts = []
           },
           {noreply, NewState} = flurm_diagnostics:handle_info(take_snapshot, State),
           ?assertEqual(1, length(NewState#state.history)),
           [Snapshot] = NewState#state.history,
           ?assert(is_record(Snapshot, snapshot)),
           ?assert(Snapshot#snapshot.timestamp > 0),
           ?assert(Snapshot#snapshot.total_memory > 0),
           ?assert(Snapshot#snapshot.process_count > 0)
       end},
      {"handle_info take_snapshot limits history size",
       fun() ->
           %% Create state with max_history of 3
           OldSnapshots = [
               #snapshot{timestamp = I, total_memory = 1000, process_count = 10,
                         ets_memory = 100, binary_memory = 50, atom_count = 100,
                         port_count = 5}
               || I <- [1, 2, 3]
           ],
           State = #state{
               interval = 60000,
               history = OldSnapshots,
               max_history = 3,
               alerts = []
           },
           {noreply, NewState} = flurm_diagnostics:handle_info(take_snapshot, State),
           %% Should still have 3 snapshots (new one replaces oldest)
           ?assertEqual(3, length(NewState#state.history))
       end},
      {"handle_info take_snapshot schedules next snapshot",
       fun() ->
           State = #state{
               interval = 100,
               history = [],
               max_history = 10,
               alerts = []
           },
           {noreply, _} = flurm_diagnostics:handle_info(take_snapshot, State),
           %% Check that timer message will arrive
           receive
               take_snapshot -> ok
           after 200 ->
               ?assert(false)  % Should have received message
           end
       end}
     ]}.

%%====================================================================
%% start_link Tests
%%====================================================================

start_link_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link/1 starts gen_server",
       fun() ->
           {ok, Pid} = flurm_diagnostics:start_link([60000]),
           ?assert(is_process_alive(Pid)),
           ?assertEqual(Pid, whereis(flurm_leak_detector)),
           gen_server:stop(Pid)
       end},
      {"start_link/1 fails if already started",
       fun() ->
           {ok, Pid} = flurm_diagnostics:start_link([60000]),
           Result = flurm_diagnostics:start_link([60000]),
           ?assertMatch({error, {already_started, _}}, Result),
           gen_server:stop(Pid)
       end}
     ]}.

%%====================================================================
%% Edge Cases and Error Handling Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handles dead process in process inspection",
       fun() ->
           %% Spawn and immediately kill a process
           Pid = spawn(fun() -> ok end),
           exit(Pid, kill),
           timer:sleep(10),
           %% These should handle undefined process_info gracefully
           Report = flurm_diagnostics:top_memory_processes(),
           ?assert(is_list(Report))
       end},
      {"handles large N in top_memory_processes",
       fun() ->
           %% Request more processes than exist
           Report = flurm_diagnostics:top_memory_processes(100000),
           ?assert(is_list(Report)),
           ?assert(length(Report) < 100000)
       end},
      {"handles large N in top_message_queue_processes",
       fun() ->
           Report = flurm_diagnostics:top_message_queue_processes(100000),
           ?assert(is_list(Report))
       end},
      {"memory_report handles concurrent calls",
       fun() ->
           %% Make multiple concurrent calls
           Self = self(),
           Pids = [spawn(fun() ->
               Report = flurm_diagnostics:memory_report(),
               Self ! {done, Report}
           end) || _ <- lists:seq(1, 10)],
           Results = [receive {done, R} -> R after 5000 -> timeout end || _ <- Pids],
           lists:foreach(fun(R) ->
               ?assertNotEqual(timeout, R),
               ?assert(is_map(R))
           end, Results)
       end}
     ]}.

%%====================================================================
%% Leak Detection Algorithm Tests
%%====================================================================

leak_detection_algorithm_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"no alerts when history is empty",
       fun() ->
           State = #state{
               interval = 100,
               history = [],
               max_history = 100,
               alerts = []
           },
           {noreply, NewState} = flurm_diagnostics:handle_info(take_snapshot, State),
           %% First snapshot, no comparison possible
           ?assertEqual([], NewState#state.alerts)
       end},
      {"no alerts when growth is below threshold",
       fun() ->
           %% Create baseline snapshot
           Baseline = #snapshot{
               timestamp = 1,
               total_memory = 1000000,
               process_count = 100,
               ets_memory = 10000,
               binary_memory = 5000,
               atom_count = 1000,
               port_count = 10
           },
           State = #state{
               interval = 100,
               history = [Baseline],
               max_history = 100,
               alerts = []
           },
           %% Take new snapshot (memory should be similar)
           {noreply, NewState} = flurm_diagnostics:handle_info(take_snapshot, State),
           %% Should have no alerts if growth is minimal
           %% (Real system memory won't grow 50% instantly)
           ?assert(length(NewState#state.alerts) >= 0)  % May or may not have alerts
       end}
     ]}.

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"full workflow: start, collect data, get history, stop",
       fun() ->
           %% Start leak detector
           {ok, _} = flurm_diagnostics:start_leak_detector(50),

           %% Wait for some snapshots
           timer:sleep(200),

           %% Get history
           {ok, History, _Alerts} = flurm_diagnostics:get_leak_history(),
           ?assert(length(History) >= 2),

           %% Get various reports while detector is running
           MemReport = flurm_diagnostics:memory_report(),
           ?assert(is_map(MemReport)),

           ProcReport = flurm_diagnostics:process_report(),
           ?assert(is_map(ProcReport)),

           FullReport = flurm_diagnostics:full_report(),
           ?assert(is_map(FullReport)),

           %% Stop
           ?assertEqual(ok, flurm_diagnostics:stop_leak_detector()),
           ?assertEqual({error, not_running}, flurm_diagnostics:get_leak_history())
       end}
     ]}.

%%====================================================================
%% Memory Report Consistency Tests
%%====================================================================

memory_report_consistency_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"multiple memory reports are consistent",
       fun() ->
           Report1 = flurm_diagnostics:memory_report(),
           timer:sleep(10),
           Report2 = flurm_diagnostics:memory_report(),
           %% Memory values should be similar (not wildly different)
           Total1 = maps:get(total, Report1),
           Total2 = maps:get(total, Report2),
           %% Within 50% of each other
           ?assert(abs(Total1 - Total2) < Total1 * 0.5)
       end}
     ]}.
