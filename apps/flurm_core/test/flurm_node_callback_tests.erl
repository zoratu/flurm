%%%-------------------------------------------------------------------
%%% @doc Callback tests for flurm_node gen_server
%%%
%%% Tests the gen_server callbacks directly without starting the process.
%%% This allows testing the callback logic in isolation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_callback_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

make_node_spec() ->
    make_node_spec(<<"node001">>).

make_node_spec(Name) ->
    #node_spec{
        name = Name,
        hostname = <<"node001.example.com">>,
        port = 6818,
        cpus = 16,
        memory = 32768,
        gpus = 4,
        features = [gpu, nvme],
        partitions = [<<"default">>, <<"compute">>]
    }.

make_node_state() ->
    make_node_state(#{}).

make_node_state(Opts) ->
    Name = maps:get(name, Opts, <<"node001">>),
    Hostname = maps:get(hostname, Opts, <<"node001.example.com">>),
    Port = maps:get(port, Opts, 6818),
    Cpus = maps:get(cpus, Opts, 16),
    CpusUsed = maps:get(cpus_used, Opts, 0),
    Memory = maps:get(memory, Opts, 32768),
    MemoryUsed = maps:get(memory_used, Opts, 0),
    Gpus = maps:get(gpus, Opts, 4),
    GpusUsed = maps:get(gpus_used, Opts, 0),
    State = maps:get(state, Opts, up),
    DrainReason = maps:get(drain_reason, Opts, undefined),
    Features = maps:get(features, Opts, [gpu, nvme]),
    Partitions = maps:get(partitions, Opts, [<<"default">>, <<"compute">>]),
    Jobs = maps:get(jobs, Opts, []),
    LastHeartbeat = maps:get(last_heartbeat, Opts, erlang:timestamp()),
    Version = maps:get(version, Opts, ?NODE_STATE_VERSION),
    #node_state{
        name = Name,
        hostname = Hostname,
        port = Port,
        cpus = Cpus,
        cpus_used = CpusUsed,
        memory = Memory,
        memory_used = MemoryUsed,
        gpus = Gpus,
        gpus_used = GpusUsed,
        state = State,
        drain_reason = DrainReason,
        features = Features,
        partitions = Partitions,
        jobs = Jobs,
        last_heartbeat = LastHeartbeat,
        version = Version
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_creates_state_from_spec_test() ->
    Spec = make_node_spec(),
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(<<"node001">>, State#node_state.name),
    ?assertEqual(<<"node001.example.com">>, State#node_state.hostname),
    ?assertEqual(6818, State#node_state.port),
    ?assertEqual(16, State#node_state.cpus),
    ?assertEqual(0, State#node_state.cpus_used),
    ?assertEqual(32768, State#node_state.memory),
    ?assertEqual(0, State#node_state.memory_used),
    ?assertEqual(4, State#node_state.gpus),
    ?assertEqual(0, State#node_state.gpus_used),
    ?assertEqual(up, State#node_state.state),
    ?assertEqual(undefined, State#node_state.drain_reason),
    ?assertEqual([gpu, nvme], State#node_state.features),
    ?assertEqual([<<"default">>, <<"compute">>], State#node_state.partitions),
    ?assertEqual([], State#node_state.jobs),
    ?assertEqual(?NODE_STATE_VERSION, State#node_state.version).

%%====================================================================
%% handle_call Tests - allocate
%%====================================================================

handle_call_allocate_success_test() ->
    State = make_node_state(),
    {reply, ok, NewState} = flurm_node:handle_call(
        {allocate, 1001, 4, 8192, 2}, {self(), make_ref()}, State
    ),
    ?assertEqual(4, NewState#node_state.cpus_used),
    ?assertEqual(8192, NewState#node_state.memory_used),
    ?assertEqual(2, NewState#node_state.gpus_used),
    ?assertEqual([1001], NewState#node_state.jobs).

handle_call_allocate_insufficient_cpus_test() ->
    State = make_node_state(#{cpus => 4}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node:handle_call(
        {allocate, 1001, 8, 1024, 0}, {self(), make_ref()}, State
    ).

handle_call_allocate_insufficient_memory_test() ->
    State = make_node_state(#{memory => 8192}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node:handle_call(
        {allocate, 1001, 1, 16384, 0}, {self(), make_ref()}, State
    ).

handle_call_allocate_insufficient_gpus_test() ->
    State = make_node_state(#{gpus => 2}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node:handle_call(
        {allocate, 1001, 1, 1024, 4}, {self(), make_ref()}, State
    ).

handle_call_allocate_node_down_test() ->
    State = make_node_state(#{state => down}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node:handle_call(
        {allocate, 1001, 1, 1024, 0}, {self(), make_ref()}, State
    ).

handle_call_allocate_node_drain_test() ->
    State = make_node_state(#{state => drain}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node:handle_call(
        {allocate, 1001, 1, 1024, 0}, {self(), make_ref()}, State
    ).

handle_call_allocate_node_maint_test() ->
    State = make_node_state(#{state => maint}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node:handle_call(
        {allocate, 1001, 1, 1024, 0}, {self(), make_ref()}, State
    ).

handle_call_allocate_multiple_jobs_test() ->
    State = make_node_state(),
    {reply, ok, State2} = flurm_node:handle_call(
        {allocate, 1001, 4, 8192, 1}, {self(), make_ref()}, State
    ),
    {reply, ok, State3} = flurm_node:handle_call(
        {allocate, 1002, 4, 8192, 1}, {self(), make_ref()}, State2
    ),
    ?assertEqual(8, State3#node_state.cpus_used),
    ?assertEqual(16384, State3#node_state.memory_used),
    ?assertEqual(2, State3#node_state.gpus_used),
    ?assertEqual([1002, 1001], State3#node_state.jobs).

%%====================================================================
%% handle_call Tests - release
%%====================================================================

handle_call_release_success_test() ->
    State = make_node_state(#{
        cpus_used => 4,
        memory_used => 8192,
        gpus_used => 2,
        jobs => [1001]
    }),
    {reply, ok, NewState} = flurm_node:handle_call(
        {release, 1001}, {self(), make_ref()}, State
    ),
    ?assertEqual(0, NewState#node_state.cpus_used),
    ?assertEqual(0, NewState#node_state.memory_used),
    ?assertEqual(0, NewState#node_state.gpus_used),
    ?assertEqual([], NewState#node_state.jobs).

handle_call_release_job_not_found_test() ->
    State = make_node_state(),
    {reply, {error, job_not_found}, _NewState} = flurm_node:handle_call(
        {release, 9999}, {self(), make_ref()}, State
    ).

handle_call_release_partial_test() ->
    %% When multiple jobs exist, resources are divided evenly
    State = make_node_state(#{
        cpus_used => 8,
        memory_used => 16384,
        gpus_used => 4,
        jobs => [1001, 1002]
    }),
    {reply, ok, NewState} = flurm_node:handle_call(
        {release, 1001}, {self(), make_ref()}, State
    ),
    %% 8/2 = 4 cpus released, 16384/2 = 8192 memory released, 4/2 = 2 gpus released
    ?assertEqual(4, NewState#node_state.cpus_used),
    ?assertEqual(8192, NewState#node_state.memory_used),
    ?assertEqual(2, NewState#node_state.gpus_used),
    ?assertEqual([1002], NewState#node_state.jobs).

%%====================================================================
%% handle_call Tests - set_state
%%====================================================================

handle_call_set_state_up_test() ->
    State = make_node_state(#{state => down}),
    {reply, ok, NewState} = flurm_node:handle_call(
        {set_state, up}, {self(), make_ref()}, State
    ),
    ?assertEqual(up, NewState#node_state.state).

handle_call_set_state_down_test() ->
    State = make_node_state(#{state => up}),
    {reply, ok, NewState} = flurm_node:handle_call(
        {set_state, down}, {self(), make_ref()}, State
    ),
    ?assertEqual(down, NewState#node_state.state).

handle_call_set_state_drain_test() ->
    State = make_node_state(#{state => up}),
    {reply, ok, NewState} = flurm_node:handle_call(
        {set_state, drain}, {self(), make_ref()}, State
    ),
    ?assertEqual(drain, NewState#node_state.state).

handle_call_set_state_maint_test() ->
    State = make_node_state(#{state => up}),
    {reply, ok, NewState} = flurm_node:handle_call(
        {set_state, maint}, {self(), make_ref()}, State
    ),
    ?assertEqual(maint, NewState#node_state.state).

%%====================================================================
%% handle_call Tests - get_info
%%====================================================================

handle_call_get_info_test() ->
    State = make_node_state(#{
        cpus_used => 4,
        memory_used => 8192,
        gpus_used => 2,
        jobs => [1001, 1002]
    }),
    {reply, {ok, Info}, _NewState} = flurm_node:handle_call(
        get_info, {self(), make_ref()}, State
    ),
    ?assertEqual(<<"node001">>, maps:get(name, Info)),
    ?assertEqual(<<"node001.example.com">>, maps:get(hostname, Info)),
    ?assertEqual(6818, maps:get(port, Info)),
    ?assertEqual(16, maps:get(cpus, Info)),
    ?assertEqual(4, maps:get(cpus_used, Info)),
    ?assertEqual(12, maps:get(cpus_available, Info)),
    ?assertEqual(32768, maps:get(memory, Info)),
    ?assertEqual(8192, maps:get(memory_used, Info)),
    ?assertEqual(24576, maps:get(memory_available, Info)),
    ?assertEqual(4, maps:get(gpus, Info)),
    ?assertEqual(2, maps:get(gpus_used, Info)),
    ?assertEqual(2, maps:get(gpus_available, Info)),
    ?assertEqual(up, maps:get(state, Info)),
    ?assertEqual([1001, 1002], maps:get(jobs, Info)),
    ?assertEqual(2, maps:get(job_count, Info)).

%%====================================================================
%% handle_call Tests - list_jobs
%%====================================================================

handle_call_list_jobs_empty_test() ->
    State = make_node_state(),
    {reply, {ok, Jobs}, _NewState} = flurm_node:handle_call(
        list_jobs, {self(), make_ref()}, State
    ),
    ?assertEqual([], Jobs).

handle_call_list_jobs_with_jobs_test() ->
    State = make_node_state(#{jobs => [1001, 1002, 1003]}),
    {reply, {ok, Jobs}, _NewState} = flurm_node:handle_call(
        list_jobs, {self(), make_ref()}, State
    ),
    ?assertEqual([1001, 1002, 1003], Jobs).

%%====================================================================
%% handle_call Tests - drain
%%====================================================================

handle_call_drain_test() ->
    State = make_node_state(#{state => up}),
    {reply, ok, NewState} = flurm_node:handle_call(
        {drain, <<"scheduled maintenance">>}, {self(), make_ref()}, State
    ),
    ?assertEqual(drain, NewState#node_state.state),
    ?assertEqual(<<"scheduled maintenance">>, NewState#node_state.drain_reason).

handle_call_drain_already_drained_test() ->
    State = make_node_state(#{state => drain, drain_reason => <<"old reason">>}),
    {reply, ok, NewState} = flurm_node:handle_call(
        {drain, <<"new reason">>}, {self(), make_ref()}, State
    ),
    ?assertEqual(drain, NewState#node_state.state),
    ?assertEqual(<<"new reason">>, NewState#node_state.drain_reason).

%%====================================================================
%% handle_call Tests - undrain
%%====================================================================

handle_call_undrain_success_test() ->
    State = make_node_state(#{state => drain, drain_reason => <<"test">>}),
    {reply, ok, NewState} = flurm_node:handle_call(
        undrain, {self(), make_ref()}, State
    ),
    ?assertEqual(up, NewState#node_state.state),
    ?assertEqual(undefined, NewState#node_state.drain_reason).

handle_call_undrain_not_draining_test() ->
    State = make_node_state(#{state => up}),
    {reply, {error, not_draining}, _NewState} = flurm_node:handle_call(
        undrain, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - get_drain_reason
%%====================================================================

handle_call_get_drain_reason_success_test() ->
    State = make_node_state(#{state => drain, drain_reason => <<"scheduled maintenance">>}),
    {reply, {ok, Reason}, _NewState} = flurm_node:handle_call(
        get_drain_reason, {self(), make_ref()}, State
    ),
    ?assertEqual(<<"scheduled maintenance">>, Reason).

handle_call_get_drain_reason_not_draining_test() ->
    State = make_node_state(#{state => up}),
    {reply, {error, not_draining}, _NewState} = flurm_node:handle_call(
        get_drain_reason, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - is_draining
%%====================================================================

handle_call_is_draining_true_test() ->
    State = make_node_state(#{state => drain}),
    {reply, true, _NewState} = flurm_node:handle_call(
        is_draining, {self(), make_ref()}, State
    ).

handle_call_is_draining_false_test() ->
    State = make_node_state(#{state => up}),
    {reply, false, _NewState} = flurm_node:handle_call(
        is_draining, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - unknown request
%%====================================================================

handle_call_unknown_request_test() ->
    State = make_node_state(),
    {reply, {error, unknown_request}, _NewState} = flurm_node:handle_call(
        {unknown_operation, some_args}, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_heartbeat_test() ->
    OldTimestamp = {0, 0, 0},
    State = make_node_state(#{last_heartbeat => OldTimestamp}),
    {noreply, NewState} = flurm_node:handle_cast(heartbeat, State),
    %% Heartbeat should be updated
    ?assertNotEqual(OldTimestamp, NewState#node_state.last_heartbeat).

handle_cast_unknown_test() ->
    State = make_node_state(),
    {noreply, NewState} = flurm_node:handle_cast({unknown_cast, args}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_unknown_test() ->
    State = make_node_state(),
    {noreply, NewState} = flurm_node:handle_info({unknown_message, args}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% code_change Tests
%%====================================================================

code_change_same_version_test() ->
    State = make_node_state(#{version => ?NODE_STATE_VERSION}),
    {ok, NewState} = flurm_node:code_change("1.0.0", State, []),
    ?assertEqual(?NODE_STATE_VERSION, NewState#node_state.version).

%%====================================================================
%% Helper Function Tests
%%====================================================================

can_allocate_up_sufficient_resources_test() ->
    State = make_node_state(#{cpus => 16, memory => 32768, gpus => 4}),
    ?assertEqual(true, flurm_node:can_allocate(State, 4, 8192, 2)).

can_allocate_insufficient_cpus_test() ->
    State = make_node_state(#{cpus => 4, cpus_used => 0}),
    ?assertEqual(false, flurm_node:can_allocate(State, 8, 1024, 0)).

can_allocate_insufficient_memory_test() ->
    State = make_node_state(#{memory => 8192, memory_used => 0}),
    ?assertEqual(false, flurm_node:can_allocate(State, 1, 16384, 0)).

can_allocate_insufficient_gpus_test() ->
    State = make_node_state(#{gpus => 2, gpus_used => 0}),
    ?assertEqual(false, flurm_node:can_allocate(State, 1, 1024, 4)).

can_allocate_down_state_test() ->
    State = make_node_state(#{state => down}),
    ?assertEqual(false, flurm_node:can_allocate(State, 1, 1024, 0)).

can_allocate_drain_state_test() ->
    State = make_node_state(#{state => drain}),
    ?assertEqual(false, flurm_node:can_allocate(State, 1, 1024, 0)).

can_allocate_maint_state_test() ->
    State = make_node_state(#{state => maint}),
    ?assertEqual(false, flurm_node:can_allocate(State, 1, 1024, 0)).

can_allocate_with_existing_allocations_test() ->
    State = make_node_state(#{
        cpus => 16, cpus_used => 8,
        memory => 32768, memory_used => 16384,
        gpus => 4, gpus_used => 2
    }),
    %% Should be able to allocate remaining resources
    ?assertEqual(true, flurm_node:can_allocate(State, 4, 8192, 1)),
    %% Should not be able to exceed remaining
    ?assertEqual(false, flurm_node:can_allocate(State, 10, 8192, 1)).

can_allocate_exactly_available_test() ->
    State = make_node_state(#{
        cpus => 8, cpus_used => 4,
        memory => 16384, memory_used => 8192,
        gpus => 2, gpus_used => 1
    }),
    ?assertEqual(true, flurm_node:can_allocate(State, 4, 8192, 1)).

build_node_info_test() ->
    State = make_node_state(#{
        cpus_used => 8,
        memory_used => 16384,
        gpus_used => 2,
        jobs => [1001, 1002, 1003]
    }),
    Info = flurm_node:build_node_info(State),
    ?assertEqual(<<"node001">>, maps:get(name, Info)),
    ?assertEqual(<<"node001.example.com">>, maps:get(hostname, Info)),
    ?assertEqual(6818, maps:get(port, Info)),
    ?assertEqual(16, maps:get(cpus, Info)),
    ?assertEqual(8, maps:get(cpus_used, Info)),
    ?assertEqual(8, maps:get(cpus_available, Info)),
    ?assertEqual(32768, maps:get(memory, Info)),
    ?assertEqual(16384, maps:get(memory_used, Info)),
    ?assertEqual(16384, maps:get(memory_available, Info)),
    ?assertEqual(4, maps:get(gpus, Info)),
    ?assertEqual(2, maps:get(gpus_used, Info)),
    ?assertEqual(2, maps:get(gpus_available, Info)),
    ?assertEqual(up, maps:get(state, Info)),
    ?assertEqual(undefined, maps:get(drain_reason, Info)),
    ?assertEqual([gpu, nvme], maps:get(features, Info)),
    ?assertEqual([<<"default">>, <<"compute">>], maps:get(partitions, Info)),
    ?assertEqual([1001, 1002, 1003], maps:get(jobs, Info)),
    ?assertEqual(3, maps:get(job_count, Info)).

build_node_info_with_drain_test() ->
    State = make_node_state(#{state => drain, drain_reason => <<"maintenance">>}),
    Info = flurm_node:build_node_info(State),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(<<"maintenance">>, maps:get(drain_reason, Info)).

maybe_upgrade_state_current_version_test() ->
    State = make_node_state(#{version => ?NODE_STATE_VERSION}),
    Upgraded = flurm_node:maybe_upgrade_state(State),
    ?assertEqual(?NODE_STATE_VERSION, Upgraded#node_state.version).

maybe_upgrade_state_old_version_test() ->
    %% Simulate an older version
    OldVersion = ?NODE_STATE_VERSION - 1,
    State = make_node_state(#{version => OldVersion}),
    Upgraded = flurm_node:maybe_upgrade_state(State),
    ?assertEqual(?NODE_STATE_VERSION, Upgraded#node_state.version).
