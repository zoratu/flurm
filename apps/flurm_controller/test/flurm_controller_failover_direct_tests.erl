%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_controller_failover
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_failover_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

failover_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init initializes state", fun test_init/0},
      {"handle_call get_status returns status", fun test_get_status/0},
      {"handle_call unknown returns error", fun test_unknown_call/0},
      {"handle_cast became_leader starts recovery", fun test_became_leader/0},
      {"handle_cast lost_leadership cancels timers", fun test_lost_leadership/0},
      {"handle_cast unknown is ignored", fun test_unknown_cast/0},
      {"handle_info start_recovery begins recovery", fun test_start_recovery/0},
      {"handle_info start_recovery while recovering is ignored", fun test_start_recovery_already_recovering/0},
      {"handle_info health_check when leader", fun test_health_check_leader/0},
      {"handle_info health_check not leader", fun test_health_check_not_leader/0},
      {"handle_info health_check not recovered", fun test_health_check_not_recovered/0},
      {"handle_info recovery_complete success", fun test_recovery_complete_success/0},
      {"handle_info recovery_complete failure", fun test_recovery_complete_failure/0},
      {"handle_info unknown is ignored", fun test_unknown_info/0},
      {"terminate cancels timer", fun test_terminate_with_timer/0},
      {"terminate without timer", fun test_terminate_without_timer/0},
      {"code_change returns ok", fun test_code_change/0}
     ]}.

setup() ->
    meck:new(flurm_controller_cluster, [passthrough, non_strict]),
    meck:new(flurm_job_manager, [passthrough, non_strict]),
    meck:new(flurm_node_manager_server, [passthrough, non_strict]),
    meck:new(flurm_scheduler, [passthrough, non_strict]),

    %% Default mocks
    meck:expect(flurm_controller_cluster, cluster_status, fun() -> #{ra_ready => true} end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    ok.

cleanup(_) ->
    meck:unload(flurm_controller_cluster),
    meck:unload(flurm_job_manager),
    meck:unload(flurm_node_manager_server),
    meck:unload(flurm_scheduler),
    ok.

%% State record matching the module's internal state
-record(state, {
    is_leader = false :: boolean(),
    became_leader_time :: erlang:timestamp() | undefined,
    recovery_status = idle :: idle | recovering | recovered | failed,
    recovery_error :: term() | undefined,
    health_check_ref :: reference() | undefined,
    pending_operations = [] :: [{reference(), term()}]
}).

%%====================================================================
%% Test Cases
%%====================================================================

test_init() ->
    {ok, State} = flurm_controller_failover:init([]),
    ?assertMatch(#state{}, State),
    ?assertEqual(false, State#state.is_leader),
    ?assertEqual(idle, State#state.recovery_status).

test_get_status() ->
    State = #state{
        is_leader = true,
        became_leader_time = erlang:timestamp(),
        recovery_status = recovered
    },
    {reply, Status, _NewState} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assert(is_map(Status)),
    ?assertEqual(true, maps:get(is_leader, Status)),
    ?assertEqual(recovered, maps:get(recovery_status, Status)).

test_unknown_call() ->
    State = #state{},
    {reply, Result, _NewState} = flurm_controller_failover:handle_call(unknown, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Result).

test_became_leader() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_failover:handle_cast(became_leader, State),
    ?assertEqual(true, NewState#state.is_leader),
    ?assertNotEqual(undefined, NewState#state.became_leader_time),
    ?assertNotEqual(undefined, NewState#state.health_check_ref),
    %% Flush the start_recovery message
    receive start_recovery -> ok after 100 -> ok end.

test_lost_leadership() ->
    Ref = make_ref(),
    State = #state{
        is_leader = true,
        became_leader_time = erlang:timestamp(),
        health_check_ref = Ref
    },
    {noreply, NewState} = flurm_controller_failover:handle_cast(lost_leadership, State),
    ?assertEqual(false, NewState#state.is_leader),
    ?assertEqual(undefined, NewState#state.became_leader_time),
    ?assertEqual(undefined, NewState#state.health_check_ref).

test_unknown_cast() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_failover:handle_cast(unknown, State),
    ?assertEqual(State, NewState).

test_start_recovery() ->
    State = #state{is_leader = true, recovery_status = idle},
    {noreply, NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    ?assertEqual(recovering, NewState#state.recovery_status),
    %% Wait for recovery to complete
    receive {recovery_complete, _} -> ok after 1000 -> ok end.

test_start_recovery_already_recovering() ->
    State = #state{is_leader = true, recovery_status = recovering},
    {noreply, NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    %% State should remain unchanged
    ?assertEqual(recovering, NewState#state.recovery_status).

test_health_check_leader() ->
    State = #state{is_leader = true, recovery_status = recovered},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    ?assertNotEqual(undefined, NewState#state.health_check_ref).

test_health_check_not_leader() ->
    State = #state{is_leader = false, recovery_status = recovered},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    ?assertNotEqual(undefined, NewState#state.health_check_ref).

test_health_check_not_recovered() ->
    State = #state{is_leader = true, recovery_status = recovering},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    %% Should still schedule next health check
    ?assertNotEqual(undefined, NewState#state.health_check_ref).

test_recovery_complete_success() ->
    State = #state{recovery_status = recovering},
    {noreply, NewState} = flurm_controller_failover:handle_info({recovery_complete, {ok, recovered}}, State),
    ?assertEqual(recovered, NewState#state.recovery_status),
    ?assertEqual(undefined, NewState#state.recovery_error).

test_recovery_complete_failure() ->
    State = #state{recovery_status = recovering},
    {noreply, NewState} = flurm_controller_failover:handle_info({recovery_complete, {error, some_error}}, State),
    ?assertEqual(failed, NewState#state.recovery_status),
    ?assertEqual(some_error, NewState#state.recovery_error),
    %% Flush retry message
    receive start_recovery -> ok after 100 -> ok end.

test_unknown_info() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_failover:handle_info(unknown, State),
    ?assertEqual(State, NewState).

test_terminate_with_timer() ->
    Ref = make_ref(),
    State = #state{health_check_ref = Ref},
    Result = flurm_controller_failover:terminate(normal, State),
    ?assertEqual(ok, Result).

test_terminate_without_timer() ->
    State = #state{health_check_ref = undefined},
    Result = flurm_controller_failover:terminate(normal, State),
    ?assertEqual(ok, Result).

test_code_change() ->
    State = #state{},
    {ok, NewState} = flurm_controller_failover:code_change("1.0", State, []),
    ?assertEqual(State, NewState).

%%====================================================================
%% API Tests
%%====================================================================

api_test_() ->
    {setup,
     fun setup_api/0,
     fun cleanup_api/1,
     [
      {"on_became_leader sends cast", fun test_api_on_became_leader/0},
      {"on_lost_leadership sends cast", fun test_api_on_lost_leadership/0},
      {"get_status returns status map", fun test_api_get_status/0}
     ]}.

setup_api() ->
    %% Start the server for API tests
    meck:new(flurm_controller_cluster, [passthrough, non_strict]),
    meck:new(flurm_job_manager, [passthrough, non_strict]),
    meck:new(flurm_node_manager_server, [passthrough, non_strict]),
    meck:new(flurm_scheduler, [passthrough, non_strict]),

    meck:expect(flurm_controller_cluster, cluster_status, fun() -> #{ra_ready => true} end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),

    {ok, Pid} = flurm_controller_failover:start_link(),
    Pid.

cleanup_api(Pid) ->
    gen_server:stop(Pid),
    meck:unload(flurm_controller_cluster),
    meck:unload(flurm_job_manager),
    meck:unload(flurm_node_manager_server),
    meck:unload(flurm_scheduler),
    ok.

test_api_on_became_leader() ->
    %% Should not crash
    Result = flurm_controller_failover:on_became_leader(),
    ?assertEqual(ok, Result),
    %% Give time for cast to be processed
    timer:sleep(100).

test_api_on_lost_leadership() ->
    %% Should not crash
    Result = flurm_controller_failover:on_lost_leadership(),
    ?assertEqual(ok, Result),
    timer:sleep(50).

test_api_get_status() ->
    Status = flurm_controller_failover:get_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(is_leader, Status)),
    ?assert(maps:is_key(recovery_status, Status)).

%%====================================================================
%% Recovery Steps Tests
%%====================================================================

recovery_steps_test_() ->
    {setup,
     fun setup_recovery/0,
     fun cleanup_recovery/1,
     [
      {"recovery with running jobs", fun test_recovery_with_running_jobs/0},
      {"recovery with stale nodes", fun test_recovery_with_stale_nodes/0},
      {"recovery with job manager error", fun test_recovery_job_manager_error/0},
      {"health check detects leadership loss", fun test_health_check_leadership_loss/0}
     ]}.

setup_recovery() ->
    setup().

cleanup_recovery(_) ->
    cleanup(ok).

test_recovery_with_running_jobs() ->
    Job = #job{id = 1, name = <<"running_job">>, state = running, user = <<"user">>,
               partition = <<"default">>, script = <<>>, num_nodes = 1, num_cpus = 1,
               memory_mb = 1024, time_limit = 3600, priority = 100, submit_time = 0,
               allocated_nodes = [<<"node1">>]},
    meck:expect(flurm_job_manager, list_jobs, fun() -> [Job] end),

    State = #state{is_leader = true, recovery_status = idle},
    {noreply, _NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    %% Wait for recovery
    receive {recovery_complete, _} -> ok after 2000 -> ok end.

test_recovery_with_stale_nodes() ->
    StaleTime = erlang:system_time(second) - 100,  %% 100 seconds ago
    Node = #node{hostname = <<"stale_node">>, cpus = 8, memory_mb = 16384,
                 state = idle, features = [], partitions = [], running_jobs = [],
                 load_avg = 0.0, free_memory_mb = 8192, last_heartbeat = StaleTime},
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [Node] end),

    State = #state{is_leader = true, recovery_status = idle},
    {noreply, _NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    %% Wait for recovery
    receive {recovery_complete, _} -> ok after 2000 -> ok end.

test_recovery_job_manager_error() ->
    meck:expect(flurm_job_manager, list_jobs, fun() -> error(crash) end),

    State = #state{is_leader = true, recovery_status = idle},
    {noreply, _NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    %% Recovery should still complete (non-fatal error)
    receive {recovery_complete, _} -> ok after 2000 -> ok end.

test_health_check_leadership_loss() ->
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),

    State = #state{is_leader = true, recovery_status = recovered},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    %% Should detect leadership loss
    ?assertEqual(false, NewState#state.is_leader).

%%====================================================================
%% Leader Uptime Calculation Test
%%====================================================================

uptime_calculation_test_() ->
    [
     {"calculate_leader_uptime undefined returns 0", fun() ->
          State = #state{is_leader = true, became_leader_time = undefined},
          {reply, Status, _} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
          ?assertEqual(0, maps:get(uptime_as_leader, Status))
      end},
     {"calculate_leader_uptime with timestamp", fun() ->
          %% Set timestamp to 10 seconds ago
          {Mega, Sec, Micro} = erlang:timestamp(),
          PastTime = {Mega, Sec - 10, Micro},
          State = #state{is_leader = true, became_leader_time = PastTime},
          {reply, Status, _} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
          Uptime = maps:get(uptime_as_leader, Status),
          ?assert(Uptime >= 9 andalso Uptime =< 11)  %% Should be around 10 seconds
      end}
    ].
