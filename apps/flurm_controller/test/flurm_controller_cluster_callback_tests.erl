%%%-------------------------------------------------------------------
%%% @doc Callback tests for flurm_controller_cluster gen_server
%%%
%%% Tests the gen_server callbacks directly without starting the process.
%%% This allows testing the callback logic in isolation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_cluster_callback_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

make_state() ->
    make_state(#{}).

make_state(Opts) ->
    ClusterName = maps:get(cluster_name, Opts, flurm_controller),
    RaClusterId = maps:get(ra_cluster_id, Opts, {flurm_controller, node()}),
    IsLeader = maps:get(is_leader, Opts, false),
    CurrentLeader = maps:get(current_leader, Opts, undefined),
    ClusterNodes = maps:get(cluster_nodes, Opts, [node()]),
    RaReady = maps:get(ra_ready, Opts, false),
    LeaderCheckRef = maps:get(leader_check_ref, Opts, undefined),
    DataDir = maps:get(data_dir, Opts, "/var/lib/flurm/ra"),
    {state,
     ClusterName,
     RaClusterId,
     IsLeader,
     CurrentLeader,
     ClusterNodes,
     RaReady,
     LeaderCheckRef,
     DataDir}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_returns_state_test() ->
    %% init/1 sends init_ra_cluster message to self
    {ok, State} = flurm_controller_cluster:init([]),
    %% State should be initialized with defaults
    ?assertMatch({state, _, _, false, undefined, _, false, undefined, _}, State).

%%====================================================================
%% handle_call Tests - is_leader
%%====================================================================

handle_call_is_leader_not_ready_test() ->
    State = make_state(#{ra_ready => false, is_leader => true}),
    {reply, false, _NewState} = flurm_controller_cluster:handle_call(
        is_leader, {self(), make_ref()}, State
    ).

handle_call_is_leader_ready_and_leader_test() ->
    State = make_state(#{ra_ready => true, is_leader => true}),
    {reply, true, _NewState} = flurm_controller_cluster:handle_call(
        is_leader, {self(), make_ref()}, State
    ).

handle_call_is_leader_ready_not_leader_test() ->
    State = make_state(#{ra_ready => true, is_leader => false}),
    {reply, false, _NewState} = flurm_controller_cluster:handle_call(
        is_leader, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - get_leader
%%====================================================================

handle_call_get_leader_not_ready_test() ->
    State = make_state(#{ra_ready => false}),
    {reply, {error, not_ready}, _NewState} = flurm_controller_cluster:handle_call(
        get_leader, {self(), make_ref()}, State
    ).

handle_call_get_leader_no_leader_test() ->
    State = make_state(#{ra_ready => true, current_leader => undefined}),
    {reply, {error, no_leader}, _NewState} = flurm_controller_cluster:handle_call(
        get_leader, {self(), make_ref()}, State
    ).

handle_call_get_leader_has_leader_test() ->
    Leader = {flurm_controller, 'node1@localhost'},
    State = make_state(#{ra_ready => true, current_leader => Leader}),
    {reply, {ok, Leader}, _NewState} = flurm_controller_cluster:handle_call(
        get_leader, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - forward_to_leader
%%====================================================================

handle_call_forward_to_leader_not_ready_test() ->
    State = make_state(#{ra_ready => false}),
    {reply, {error, cluster_not_ready}, _NewState} = flurm_controller_cluster:handle_call(
        {forward_to_leader, submit_job, #{name => <<"test">>}},
        {self(), make_ref()}, State
    ).

handle_call_forward_to_leader_no_leader_test() ->
    State = make_state(#{ra_ready => true, is_leader => false, current_leader => undefined}),
    {reply, {error, no_leader}, _NewState} = flurm_controller_cluster:handle_call(
        {forward_to_leader, submit_job, #{name => <<"test">>}},
        {self(), make_ref()}, State
    ).

handle_call_forward_to_leader_is_leader_test() ->
    %% When we are the leader, handle_local_operation is called
    %% This will fail without flurm_job_manager running, so we just test the path
    State = make_state(#{ra_ready => true, is_leader => true}),
    %% Using an unknown operation to avoid needing flurm_job_manager
    {reply, {error, unknown_operation}, _NewState} = flurm_controller_cluster:handle_call(
        {forward_to_leader, unknown_op, #{}},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - cluster_status
%%====================================================================

handle_call_cluster_status_test() ->
    Leader = {flurm_controller, node()},
    State = make_state(#{
        ra_ready => true,
        is_leader => true,
        current_leader => Leader,
        cluster_nodes => [node()]
    }),
    {reply, Status, _NewState} = flurm_controller_cluster:handle_call(
        cluster_status, {self(), make_ref()}, State
    ),
    ?assertEqual(flurm_controller, maps:get(cluster_name, Status)),
    ?assertEqual(node(), maps:get(this_node, Status)),
    ?assertEqual(true, maps:get(is_leader, Status)),
    ?assertEqual(Leader, maps:get(current_leader, Status)),
    ?assertEqual(true, maps:get(ra_ready, Status)).

%%====================================================================
%% handle_call Tests - get_members
%%====================================================================

handle_call_get_members_not_ready_test() ->
    State = make_state(#{ra_ready => false}),
    {reply, {error, not_ready}, _NewState} = flurm_controller_cluster:handle_call(
        get_members, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - unknown request
%%====================================================================

handle_call_unknown_request_test() ->
    State = make_state(),
    {reply, {error, unknown_request}, _NewState} = flurm_controller_cluster:handle_call(
        {unknown_operation, some_args},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_unknown_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_controller_cluster:handle_cast(
        {unknown_cast, args}, State
    ),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_unknown_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_controller_cluster:handle_info(
        {unknown_message, args}, State
    ),
    ?assertEqual(State, NewState).

handle_info_ra_event_leader_change_test() ->
    %% Test leader change event processing
    NewLeader = {flurm_controller, node()},
    State = make_state(#{ra_ready => true, is_leader => false, current_leader => undefined}),
    {noreply, NewState} = flurm_controller_cluster:handle_info(
        {ra_event, {flurm_controller, node()}, {machine, leader_change, NewLeader}},
        State
    ),
    ?assertMatch({state, _, _, true, NewLeader, _, _, _, _}, NewState).

handle_info_ra_event_other_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_controller_cluster:handle_info(
        {ra_event, {flurm_controller, node()}, {some_other_event, data}},
        State
    ),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_no_timer_test() ->
    State = make_state(#{leader_check_ref => undefined}),
    ?assertEqual(ok, flurm_controller_cluster:terminate(normal, State)).

terminate_with_timer_test() ->
    %% Create a real timer reference
    Ref = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{leader_check_ref => Ref}),
    ?assertEqual(ok, flurm_controller_cluster:terminate(normal, State)).

%%====================================================================
%% code_change Tests
%%====================================================================

code_change_test() ->
    State = make_state(),
    {ok, NewState} = flurm_controller_cluster:code_change("1.0.0", State, []),
    ?assertEqual(State, NewState).

%%====================================================================
%% Helper Function Tests
%%====================================================================

is_this_node_leader_undefined_test() ->
    ?assertEqual(false, flurm_controller_cluster:is_this_node_leader(undefined)).

is_this_node_leader_same_node_test() ->
    Leader = {flurm_controller, node()},
    ?assertEqual(true, flurm_controller_cluster:is_this_node_leader(Leader)).

is_this_node_leader_different_node_test() ->
    Leader = {flurm_controller, 'other@localhost'},
    ?assertEqual(false, flurm_controller_cluster:is_this_node_leader(Leader)).

get_ra_members_not_ready_test() ->
    State = make_state(#{ra_ready => false}),
    ?assertEqual([], flurm_controller_cluster:get_ra_members(State)).

handle_local_operation_unknown_test() ->
    Result = flurm_controller_cluster:handle_local_operation(unknown_operation, #{}),
    ?assertEqual({error, unknown_operation}, Result).

%%====================================================================
%% update_leader_status Tests
%%====================================================================

update_leader_status_not_ready_test() ->
    State = make_state(#{ra_ready => false}),
    NewState = flurm_controller_cluster:update_leader_status(State),
    ?assertEqual(State, NewState).
