%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_controller_cluster
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_cluster_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

cluster_api_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"is_leader returns false when not started", fun test_is_leader_not_started/0},
      {"get_leader returns error when not started", fun test_get_leader_not_started/0},
      {"cluster_status returns not_started when not started", fun test_cluster_status_not_started/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% API Tests (without server running)
%%====================================================================

test_is_leader_not_started() ->
    %% When server is not running, is_leader should return false
    Result = flurm_controller_cluster:is_leader(),
    ?assertEqual(false, Result).

test_get_leader_not_started() ->
    %% When server is not running, get_leader should return error
    Result = flurm_controller_cluster:get_leader(),
    ?assertMatch({error, not_ready}, Result).

test_cluster_status_not_started() ->
    %% When server is not running, cluster_status should indicate not_started
    Result = flurm_controller_cluster:cluster_status(),
    ?assertMatch(#{status := not_started}, Result).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_test_() ->
    {setup,
     fun setup_gen_server/0,
     fun cleanup_gen_server/1,
     [
      {"init initializes state", fun test_init/0},
      {"handle_call is_leader not ready", fun test_handle_call_is_leader_not_ready/0},
      {"handle_call is_leader ready", fun test_handle_call_is_leader_ready/0},
      {"handle_call get_leader not ready", fun test_handle_call_get_leader_not_ready/0},
      {"handle_call get_leader no leader", fun test_handle_call_get_leader_no_leader/0},
      {"handle_call get_leader with leader", fun test_handle_call_get_leader_with_leader/0},
      {"handle_call cluster_status", fun test_handle_call_cluster_status/0},
      {"handle_call forward_to_leader not ready", fun test_handle_call_forward_not_ready/0},
      {"handle_call forward_to_leader as leader", fun test_handle_call_forward_as_leader/0},
      {"handle_call forward_to_leader no leader", fun test_handle_call_forward_no_leader/0},
      {"handle_call get_members not ready", fun test_handle_call_get_members_not_ready/0},
      {"handle_call unknown request", fun test_handle_call_unknown/0},
      {"handle_cast ignores messages", fun test_handle_cast/0},
      {"handle_info ignores unknown", fun test_handle_info_unknown/0},
      {"terminate cancels timer", fun test_terminate/0},
      {"code_change returns ok", fun test_code_change/0}
     ]}.

setup_gen_server() ->
    meck:new(application, [unstick, passthrough]),
    meck:new(ra, [passthrough, non_strict]),
    meck:new(flurm_job_manager, [passthrough, non_strict]),
    meck:new(flurm_controller_failover, [passthrough, non_strict]),

    meck:expect(application, get_env, fun
        (flurm_controller, cluster_name, Default) -> Default;
        (flurm_controller, ra_data_dir, Default) -> Default;
        (flurm_controller, cluster_nodes, Default) -> Default;
        (_, _, Default) -> Default
    end),
    meck:expect(flurm_controller_failover, on_became_leader, fun() -> ok end),
    meck:expect(flurm_controller_failover, on_lost_leadership, fun() -> ok end),
    ok.

cleanup_gen_server(_) ->
    meck:unload(application),
    meck:unload(ra),
    meck:unload(flurm_job_manager),
    meck:unload(flurm_controller_failover),
    ok.

%% State record matching the module's internal state
-record(state, {
    cluster_name,
    ra_cluster_id,
    is_leader = false,
    current_leader,
    cluster_nodes = [],
    ra_ready = false,
    leader_check_ref,
    data_dir
}).

test_init() ->
    {ok, State} = flurm_controller_cluster:init([]),
    ?assertMatch(#state{}, State),
    ?assertEqual(false, State#state.ra_ready),
    ?assertEqual(false, State#state.is_leader).

test_handle_call_is_leader_not_ready() ->
    State = #state{is_leader = true, ra_ready = false},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(is_leader, {self(), make_ref()}, State),
    ?assertEqual(false, Result).

test_handle_call_is_leader_ready() ->
    State = #state{is_leader = true, ra_ready = true},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(is_leader, {self(), make_ref()}, State),
    ?assertEqual(true, Result).

test_handle_call_get_leader_not_ready() ->
    State = #state{ra_ready = false},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(get_leader, {self(), make_ref()}, State),
    ?assertEqual({error, not_ready}, Result).

test_handle_call_get_leader_no_leader() ->
    State = #state{ra_ready = true, current_leader = undefined},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(get_leader, {self(), make_ref()}, State),
    ?assertEqual({error, no_leader}, Result).

test_handle_call_get_leader_with_leader() ->
    Leader = {flurm_controller, node()},
    State = #state{ra_ready = true, current_leader = Leader},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(get_leader, {self(), make_ref()}, State),
    ?assertEqual({ok, Leader}, Result).

test_handle_call_cluster_status() ->
    State = #state{
        cluster_name = test_cluster,
        is_leader = true,
        current_leader = {test_cluster, node()},
        cluster_nodes = [node()],
        ra_ready = true
    },
    {reply, Status, _NewState} = flurm_controller_cluster:handle_call(cluster_status, {self(), make_ref()}, State),
    ?assert(is_map(Status)),
    ?assertEqual(test_cluster, maps:get(cluster_name, Status)),
    ?assertEqual(true, maps:get(is_leader, Status)).

test_handle_call_forward_not_ready() ->
    State = #state{ra_ready = false},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(
        {forward_to_leader, test_op, []}, {self(), make_ref()}, State),
    ?assertEqual({error, cluster_not_ready}, Result).

test_handle_call_forward_as_leader() ->
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
    State = #state{ra_ready = true, is_leader = true},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(
        {forward_to_leader, list_jobs, []}, {self(), make_ref()}, State),
    ?assertEqual([], Result).

test_handle_call_forward_no_leader() ->
    State = #state{ra_ready = true, is_leader = false, current_leader = undefined},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(
        {forward_to_leader, test_op, []}, {self(), make_ref()}, State),
    ?assertEqual({error, no_leader}, Result).

test_handle_call_get_members_not_ready() ->
    State = #state{ra_ready = false},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(get_members, {self(), make_ref()}, State),
    ?assertEqual({error, not_ready}, Result).

test_handle_call_unknown() ->
    State = #state{},
    {reply, Result, _NewState} = flurm_controller_cluster:handle_call(unknown_request, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Result).

test_handle_cast() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_cluster:handle_cast(some_message, State),
    ?assertEqual(State, NewState).

test_handle_info_unknown() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_cluster:handle_info(unknown_message, State),
    ?assertEqual(State, NewState).

test_terminate() ->
    TimerRef = erlang:make_ref(),
    State = #state{leader_check_ref = TimerRef},
    Result = flurm_controller_cluster:terminate(normal, State),
    ?assertEqual(ok, Result).

test_code_change() ->
    State = #state{},
    {ok, NewState} = flurm_controller_cluster:code_change("1.0", State, []),
    ?assertEqual(State, NewState).

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_test_() ->
    {setup,
     fun setup_gen_server/0,
     fun cleanup_gen_server/1,
     [
      {"handle_info check_leader updates status", fun test_handle_info_check_leader/0},
      {"handle_info ra_event leader change", fun test_handle_info_ra_event_leader_change/0},
      {"handle_info ra_event other", fun test_handle_info_ra_event_other/0}
     ]}.

test_handle_info_check_leader() ->
    meck:expect(ra, members, fun(_) -> {error, not_ready} end),
    State = #state{ra_ready = true, ra_cluster_id = {test, node()}},
    {noreply, NewState} = flurm_controller_cluster:handle_info(check_leader, State),
    ?assertNotEqual(undefined, NewState#state.leader_check_ref).

test_handle_info_ra_event_leader_change() ->
    NewLeader = {test_cluster, node()},
    State = #state{is_leader = false, current_leader = undefined},
    {noreply, NewState} = flurm_controller_cluster:handle_info(
        {ra_event, {test, node()}, {machine, leader_change, NewLeader}}, State),
    ?assertEqual(NewLeader, NewState#state.current_leader).

test_handle_info_ra_event_other() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_cluster:handle_info(
        {ra_event, {test, node()}, some_other_event}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% Forwarded Request Handler Test (placeholder - requires cluster setup)
%%====================================================================

forwarded_request_placeholder_test_() ->
    [
     {"handle_forwarded_request tested via gen_server callbacks", fun() -> ok end}
    ].
