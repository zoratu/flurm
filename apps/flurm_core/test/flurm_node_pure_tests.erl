%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_node Module
%%%
%%% These tests directly exercise gen_server callbacks without mocking.
%%% We test init/1, handle_call/3, handle_cast/2, handle_info/2,
%%% terminate/2, and code_change/3 with real data structures.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a sample node_spec for testing
sample_node_spec() ->
    #node_spec{
        name = <<"node001">>,
        hostname = <<"node001.cluster.local">>,
        port = 6818,
        cpus = 32,
        memory = 65536,
        gpus = 4,
        features = [gpu, fast_network],
        partitions = [<<"default">>, <<"gpu">>]
    }.

%% Create a minimal node_spec
minimal_node_spec() ->
    #node_spec{
        name = <<"minimal">>,
        hostname = <<"minimal.local">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    }.

%% Create a state for testing handle_call/handle_cast
sample_state() ->
    #node_state{
        name = <<"testnode">>,
        hostname = <<"testnode.cluster.local">>,
        port = 6818,
        cpus = 16,
        cpus_used = 4,
        memory = 32768,
        memory_used = 8192,
        gpus = 2,
        gpus_used = 0,
        state = up,
        drain_reason = undefined,
        features = [gpu],
        partitions = [<<"default">>],
        jobs = [1001],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    }.

%% Create an empty state (no jobs, no resources used)
empty_state() ->
    #node_state{
        name = <<"emptynode">>,
        hostname = <<"emptynode.local">>,
        port = 6818,
        cpus = 8,
        cpus_used = 0,
        memory = 16384,
        memory_used = 0,
        gpus = 0,
        gpus_used = 0,
        state = up,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    }.

%% State with drain
drained_state() ->
    #node_state{
        name = <<"drainednode">>,
        hostname = <<"drainednode.local">>,
        port = 6818,
        cpus = 8,
        cpus_used = 4,
        memory = 16384,
        memory_used = 4096,
        gpus = 0,
        gpus_used = 0,
        state = drain,
        drain_reason = <<"scheduled maintenance">>,
        features = [],
        partitions = [<<"default">>],
        jobs = [2001],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    }.

%% State that's down
down_state() ->
    #node_state{
        name = <<"downnode">>,
        hostname = <<"downnode.local">>,
        port = 6818,
        cpus = 8,
        cpus_used = 0,
        memory = 16384,
        memory_used = 0,
        gpus = 0,
        gpus_used = 0,
        state = down,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 callback tests", [
        {"init with sample node spec", fun init_sample_spec/0},
        {"init with minimal node spec", fun init_minimal_spec/0},
        {"init sets all fields correctly", fun init_fields_correct/0},
        {"init with GPU node", fun init_gpu_node/0},
        {"init with multiple partitions", fun init_multi_partition/0}
    ]}.

init_sample_spec() ->
    Spec = sample_node_spec(),
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(<<"node001">>, State#node_state.name),
    ?assertEqual(<<"node001.cluster.local">>, State#node_state.hostname),
    ?assertEqual(6818, State#node_state.port),
    ?assertEqual(32, State#node_state.cpus),
    ?assertEqual(65536, State#node_state.memory),
    ?assertEqual(4, State#node_state.gpus),
    ?assertEqual(up, State#node_state.state).

init_minimal_spec() ->
    Spec = minimal_node_spec(),
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(<<"minimal">>, State#node_state.name),
    ?assertEqual(4, State#node_state.cpus),
    ?assertEqual(0, State#node_state.gpus),
    ?assertEqual([], State#node_state.features).

init_fields_correct() ->
    Spec = sample_node_spec(),
    {ok, State} = flurm_node:init([Spec]),
    %% Check all resources start at 0 used
    ?assertEqual(0, State#node_state.cpus_used),
    ?assertEqual(0, State#node_state.memory_used),
    ?assertEqual(0, State#node_state.gpus_used),
    %% Check initial state
    ?assertEqual(up, State#node_state.state),
    ?assertEqual(undefined, State#node_state.drain_reason),
    %% Check jobs list is empty
    ?assertEqual([], State#node_state.jobs),
    %% Check version is set
    ?assertEqual(?NODE_STATE_VERSION, State#node_state.version),
    %% Check last_heartbeat is set (tuple with 3 elements)
    ?assertEqual(3, tuple_size(State#node_state.last_heartbeat)).

init_gpu_node() ->
    Spec = #node_spec{
        name = <<"gpu-node">>,
        hostname = <<"gpu-node.local">>,
        port = 6818,
        cpus = 64,
        memory = 131072,
        gpus = 8,
        features = [gpu, a100, nvlink],
        partitions = [<<"gpu">>, <<"gpu-large">>]
    },
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(8, State#node_state.gpus),
    ?assertEqual([gpu, a100, nvlink], State#node_state.features),
    ?assertEqual([<<"gpu">>, <<"gpu-large">>], State#node_state.partitions).

init_multi_partition() ->
    Spec = #node_spec{
        name = <<"multi-part-node">>,
        hostname = <<"multi.local">>,
        port = 6818,
        cpus = 16,
        memory = 32768,
        gpus = 0,
        features = [],
        partitions = [<<"short">>, <<"long">>, <<"debug">>, <<"batch">>]
    },
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(4, length(State#node_state.partitions)),
    ?assert(lists:member(<<"short">>, State#node_state.partitions)),
    ?assert(lists:member(<<"batch">>, State#node_state.partitions)).

%%====================================================================
%% handle_call - allocate Tests
%%====================================================================

allocate_test_() ->
    {"handle_call allocate tests", [
        {"successful allocation", fun allocate_success/0},
        {"allocation updates resources", fun allocate_updates_resources/0},
        {"allocation adds job to list", fun allocate_adds_job/0},
        {"insufficient CPUs", fun allocate_insufficient_cpus/0},
        {"insufficient memory", fun allocate_insufficient_memory/0},
        {"insufficient GPUs", fun allocate_insufficient_gpus/0},
        {"allocation on drained node fails", fun allocate_drained_fails/0},
        {"allocation on down node fails", fun allocate_down_fails/0},
        {"multiple allocations", fun allocate_multiple/0},
        {"exact resource allocation", fun allocate_exact_resources/0}
    ]}.

allocate_success() ->
    State = empty_state(),
    {reply, ok, NewState} = flurm_node:handle_call({allocate, 1001, 2, 4096, 0}, {self(), make_ref()}, State),
    ?assertEqual(2, NewState#node_state.cpus_used),
    ?assertEqual(4096, NewState#node_state.memory_used),
    ?assertEqual([1001], NewState#node_state.jobs).

allocate_updates_resources() ->
    State = sample_state(),
    %% sample_state has cpus_used=4, memory_used=8192, gpus_used=0
    {reply, ok, NewState} = flurm_node:handle_call({allocate, 2002, 4, 4096, 1}, {self(), make_ref()}, State),
    ?assertEqual(8, NewState#node_state.cpus_used),
    ?assertEqual(12288, NewState#node_state.memory_used),
    ?assertEqual(1, NewState#node_state.gpus_used).

allocate_adds_job() ->
    State = sample_state(),
    %% sample_state already has job 1001
    {reply, ok, NewState} = flurm_node:handle_call({allocate, 2002, 2, 1024, 0}, {self(), make_ref()}, State),
    ?assert(lists:member(2002, NewState#node_state.jobs)),
    ?assert(lists:member(1001, NewState#node_state.jobs)),
    ?assertEqual(2, length(NewState#node_state.jobs)).

allocate_insufficient_cpus() ->
    State = empty_state(),  %% 8 CPUs total
    {reply, {error, insufficient_resources}, State} =
        flurm_node:handle_call({allocate, 3001, 16, 1024, 0}, {self(), make_ref()}, State).

allocate_insufficient_memory() ->
    State = empty_state(),  %% 16384 MB memory total
    {reply, {error, insufficient_resources}, State} =
        flurm_node:handle_call({allocate, 3002, 1, 32768, 0}, {self(), make_ref()}, State).

allocate_insufficient_gpus() ->
    State = sample_state(),  %% 2 GPUs total
    {reply, {error, insufficient_resources}, State} =
        flurm_node:handle_call({allocate, 3003, 1, 1024, 4}, {self(), make_ref()}, State).

allocate_drained_fails() ->
    State = drained_state(),
    {reply, {error, insufficient_resources}, State} =
        flurm_node:handle_call({allocate, 3004, 1, 1024, 0}, {self(), make_ref()}, State).

allocate_down_fails() ->
    State = down_state(),
    {reply, {error, insufficient_resources}, State} =
        flurm_node:handle_call({allocate, 3005, 1, 1024, 0}, {self(), make_ref()}, State).

allocate_multiple() ->
    State0 = empty_state(),
    {reply, ok, State1} = flurm_node:handle_call({allocate, 4001, 2, 2048, 0}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node:handle_call({allocate, 4002, 2, 2048, 0}, {self(), make_ref()}, State1),
    {reply, ok, State3} = flurm_node:handle_call({allocate, 4003, 2, 2048, 0}, {self(), make_ref()}, State2),
    ?assertEqual(6, State3#node_state.cpus_used),
    ?assertEqual(6144, State3#node_state.memory_used),
    ?assertEqual(3, length(State3#node_state.jobs)),
    ?assert(lists:member(4001, State3#node_state.jobs)),
    ?assert(lists:member(4002, State3#node_state.jobs)),
    ?assert(lists:member(4003, State3#node_state.jobs)).

allocate_exact_resources() ->
    State = empty_state(),  %% 8 CPUs, 16384 MB memory
    {reply, ok, NewState} = flurm_node:handle_call({allocate, 5001, 8, 16384, 0}, {self(), make_ref()}, State),
    ?assertEqual(8, NewState#node_state.cpus_used),
    ?assertEqual(16384, NewState#node_state.memory_used),
    %% No more resources available
    {reply, {error, insufficient_resources}, NewState} =
        flurm_node:handle_call({allocate, 5002, 1, 1, 0}, {self(), make_ref()}, NewState).

%%====================================================================
%% handle_call - release Tests
%%====================================================================

release_test_() ->
    {"handle_call release tests", [
        {"successful release", fun release_success/0},
        {"release non-existent job", fun release_nonexistent/0},
        {"release removes job from list", fun release_removes_job/0},
        {"release updates resources", fun release_updates_resources/0},
        {"release with multiple jobs", fun release_multiple_jobs/0}
    ]}.

release_success() ->
    State = sample_state(),  %% Has job 1001
    {reply, ok, NewState} = flurm_node:handle_call({release, 1001}, {self(), make_ref()}, State),
    ?assertNot(lists:member(1001, NewState#node_state.jobs)).

release_nonexistent() ->
    State = sample_state(),
    {reply, {error, job_not_found}, State} =
        flurm_node:handle_call({release, 9999}, {self(), make_ref()}, State).

release_removes_job() ->
    State = #node_state{
        name = <<"releasetest">>,
        hostname = <<"releasetest.local">>,
        port = 6818,
        cpus = 16,
        cpus_used = 8,
        memory = 32768,
        memory_used = 8192,
        gpus = 0,
        gpus_used = 0,
        state = up,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [1001, 1002, 1003],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    },
    {reply, ok, NewState} = flurm_node:handle_call({release, 1002}, {self(), make_ref()}, State),
    ?assertNot(lists:member(1002, NewState#node_state.jobs)),
    ?assert(lists:member(1001, NewState#node_state.jobs)),
    ?assert(lists:member(1003, NewState#node_state.jobs)),
    ?assertEqual(2, length(NewState#node_state.jobs)).

release_updates_resources() ->
    State = sample_state(),  %% cpus_used=4, memory_used=8192, 1 job
    {reply, ok, NewState} = flurm_node:handle_call({release, 1001}, {self(), make_ref()}, State),
    %% Resources should be reduced (per-job allocation released)
    ?assert(NewState#node_state.cpus_used =< State#node_state.cpus_used),
    ?assert(NewState#node_state.memory_used =< State#node_state.memory_used).

release_multiple_jobs() ->
    State = #node_state{
        name = <<"multirelease">>,
        hostname = <<"multirelease.local">>,
        port = 6818,
        cpus = 32,
        cpus_used = 12,
        memory = 65536,
        memory_used = 12288,
        gpus = 4,
        gpus_used = 3,
        state = up,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [101, 102, 103],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    },
    {reply, ok, State1} = flurm_node:handle_call({release, 101}, {self(), make_ref()}, State),
    {reply, ok, State2} = flurm_node:handle_call({release, 102}, {self(), make_ref()}, State1),
    {reply, ok, State3} = flurm_node:handle_call({release, 103}, {self(), make_ref()}, State2),
    ?assertEqual([], State3#node_state.jobs).

%%====================================================================
%% handle_call - set_state Tests
%%====================================================================

set_state_test_() ->
    {"handle_call set_state tests", [
        {"set state to drain", fun set_state_drain/0},
        {"set state to down", fun set_state_down/0},
        {"set state to up", fun set_state_up/0},
        {"set state to maint", fun set_state_maint/0}
    ]}.

set_state_drain() ->
    State = empty_state(),
    {reply, ok, NewState} = flurm_node:handle_call({set_state, drain}, {self(), make_ref()}, State),
    ?assertEqual(drain, NewState#node_state.state).

set_state_down() ->
    State = empty_state(),
    {reply, ok, NewState} = flurm_node:handle_call({set_state, down}, {self(), make_ref()}, State),
    ?assertEqual(down, NewState#node_state.state).

set_state_up() ->
    State = drained_state(),
    {reply, ok, NewState} = flurm_node:handle_call({set_state, up}, {self(), make_ref()}, State),
    ?assertEqual(up, NewState#node_state.state).

set_state_maint() ->
    State = empty_state(),
    {reply, ok, NewState} = flurm_node:handle_call({set_state, maint}, {self(), make_ref()}, State),
    ?assertEqual(maint, NewState#node_state.state).

%%====================================================================
%% handle_call - get_info Tests
%%====================================================================

get_info_test_() ->
    {"handle_call get_info tests", [
        {"get info returns map", fun get_info_returns_map/0},
        {"get info contains all fields", fun get_info_all_fields/0},
        {"get info calculates available resources", fun get_info_available/0},
        {"get info with drain reason", fun get_info_drain_reason/0}
    ]}.

get_info_returns_map() ->
    State = sample_state(),
    {reply, {ok, Info}, State} = flurm_node:handle_call(get_info, {self(), make_ref()}, State),
    ?assert(is_map(Info)).

get_info_all_fields() ->
    State = sample_state(),
    {reply, {ok, Info}, State} = flurm_node:handle_call(get_info, {self(), make_ref()}, State),
    %% Check all expected keys are present
    ExpectedKeys = [name, hostname, port, cpus, cpus_used, cpus_available,
                    memory, memory_used, memory_available, gpus, gpus_used,
                    gpus_available, state, drain_reason, features, partitions,
                    jobs, job_count, last_heartbeat],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Info), {missing_key, Key})
    end, ExpectedKeys).

get_info_available() ->
    State = sample_state(),  %% 16 CPUs, 4 used; 32768 mem, 8192 used; 2 GPUs, 0 used
    {reply, {ok, Info}, State} = flurm_node:handle_call(get_info, {self(), make_ref()}, State),
    ?assertEqual(12, maps:get(cpus_available, Info)),
    ?assertEqual(24576, maps:get(memory_available, Info)),
    ?assertEqual(2, maps:get(gpus_available, Info)),
    ?assertEqual(1, maps:get(job_count, Info)).

get_info_drain_reason() ->
    State = drained_state(),
    {reply, {ok, Info}, State} = flurm_node:handle_call(get_info, {self(), make_ref()}, State),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(<<"scheduled maintenance">>, maps:get(drain_reason, Info)).

%%====================================================================
%% handle_call - list_jobs Tests
%%====================================================================

list_jobs_test_() ->
    {"handle_call list_jobs tests", [
        {"list jobs returns jobs", fun list_jobs_returns_jobs/0},
        {"list jobs empty", fun list_jobs_empty/0},
        {"list jobs multiple", fun list_jobs_multiple/0}
    ]}.

list_jobs_returns_jobs() ->
    State = sample_state(),
    {reply, {ok, Jobs}, State} = flurm_node:handle_call(list_jobs, {self(), make_ref()}, State),
    ?assertEqual([1001], Jobs).

list_jobs_empty() ->
    State = empty_state(),
    {reply, {ok, Jobs}, State} = flurm_node:handle_call(list_jobs, {self(), make_ref()}, State),
    ?assertEqual([], Jobs).

list_jobs_multiple() ->
    State = #node_state{
        name = <<"multijob">>,
        hostname = <<"multijob.local">>,
        port = 6818,
        cpus = 16,
        cpus_used = 8,
        memory = 32768,
        memory_used = 16384,
        gpus = 0,
        gpus_used = 0,
        state = up,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [1001, 1002, 1003, 1004, 1005],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    },
    {reply, {ok, Jobs}, State} = flurm_node:handle_call(list_jobs, {self(), make_ref()}, State),
    ?assertEqual(5, length(Jobs)),
    ?assert(lists:member(1003, Jobs)).

%%====================================================================
%% handle_call - drain Tests
%%====================================================================

drain_test_() ->
    {"handle_call drain tests", [
        {"drain node with reason", fun drain_with_reason/0},
        {"drain already drained node", fun drain_already_drained/0},
        {"drain sets state and reason", fun drain_sets_state/0}
    ]}.

drain_with_reason() ->
    State = empty_state(),
    {reply, ok, NewState} = flurm_node:handle_call({drain, <<"hardware issue">>}, {self(), make_ref()}, State),
    ?assertEqual(drain, NewState#node_state.state),
    ?assertEqual(<<"hardware issue">>, NewState#node_state.drain_reason).

drain_already_drained() ->
    State = drained_state(),
    {reply, ok, NewState} = flurm_node:handle_call({drain, <<"new reason">>}, {self(), make_ref()}, State),
    ?assertEqual(drain, NewState#node_state.state),
    ?assertEqual(<<"new reason">>, NewState#node_state.drain_reason).

drain_sets_state() ->
    State = sample_state(),
    {reply, ok, NewState} = flurm_node:handle_call({drain, <<"test drain">>}, {self(), make_ref()}, State),
    ?assertEqual(drain, NewState#node_state.state),
    ?assertEqual(<<"test drain">>, NewState#node_state.drain_reason),
    %% Jobs should still be there
    ?assertEqual([1001], NewState#node_state.jobs).

%%====================================================================
%% handle_call - undrain Tests
%%====================================================================

undrain_test_() ->
    {"handle_call undrain tests", [
        {"undrain drained node", fun undrain_drained/0},
        {"undrain non-drained node fails", fun undrain_not_drained/0},
        {"undrain clears drain reason", fun undrain_clears_reason/0}
    ]}.

undrain_drained() ->
    State = drained_state(),
    {reply, ok, NewState} = flurm_node:handle_call(undrain, {self(), make_ref()}, State),
    ?assertEqual(up, NewState#node_state.state),
    ?assertEqual(undefined, NewState#node_state.drain_reason).

undrain_not_drained() ->
    State = empty_state(),  %% state = up
    {reply, {error, not_draining}, State} =
        flurm_node:handle_call(undrain, {self(), make_ref()}, State).

undrain_clears_reason() ->
    State = drained_state(),
    {reply, ok, NewState} = flurm_node:handle_call(undrain, {self(), make_ref()}, State),
    ?assertEqual(undefined, NewState#node_state.drain_reason).

%%====================================================================
%% handle_call - get_drain_reason Tests
%%====================================================================

get_drain_reason_test_() ->
    {"handle_call get_drain_reason tests", [
        {"get drain reason when draining", fun get_drain_reason_draining/0},
        {"get drain reason when not draining", fun get_drain_reason_not_draining/0}
    ]}.

get_drain_reason_draining() ->
    State = drained_state(),
    {reply, {ok, Reason}, State} = flurm_node:handle_call(get_drain_reason, {self(), make_ref()}, State),
    ?assertEqual(<<"scheduled maintenance">>, Reason).

get_drain_reason_not_draining() ->
    State = empty_state(),
    {reply, {error, not_draining}, State} =
        flurm_node:handle_call(get_drain_reason, {self(), make_ref()}, State).

%%====================================================================
%% handle_call - is_draining Tests
%%====================================================================

is_draining_test_() ->
    {"handle_call is_draining tests", [
        {"is_draining when draining", fun is_draining_true/0},
        {"is_draining when not draining", fun is_draining_false/0},
        {"is_draining when down", fun is_draining_down/0}
    ]}.

is_draining_true() ->
    State = drained_state(),
    {reply, true, State} = flurm_node:handle_call(is_draining, {self(), make_ref()}, State).

is_draining_false() ->
    State = empty_state(),
    {reply, false, State} = flurm_node:handle_call(is_draining, {self(), make_ref()}, State).

is_draining_down() ->
    State = down_state(),
    {reply, false, State} = flurm_node:handle_call(is_draining, {self(), make_ref()}, State).

%%====================================================================
%% handle_call - unknown request Tests
%%====================================================================

unknown_request_test_() ->
    {"handle_call unknown request tests", [
        {"unknown atom request", fun unknown_atom_request/0},
        {"unknown tuple request", fun unknown_tuple_request/0}
    ]}.

unknown_atom_request() ->
    State = empty_state(),
    {reply, {error, unknown_request}, State} =
        flurm_node:handle_call(unknown_request, {self(), make_ref()}, State).

unknown_tuple_request() ->
    State = empty_state(),
    {reply, {error, unknown_request}, State} =
        flurm_node:handle_call({unknown, with, args}, {self(), make_ref()}, State).

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_test_() ->
    {"handle_cast tests", [
        {"heartbeat updates timestamp", fun cast_heartbeat/0},
        {"unknown cast is ignored", fun cast_unknown/0}
    ]}.

cast_heartbeat() ->
    OldTimestamp = {0, 0, 0},
    State = empty_state(),
    State2 = State#node_state{last_heartbeat = OldTimestamp},
    {noreply, NewState} = flurm_node:handle_cast(heartbeat, State2),
    ?assertNotEqual(OldTimestamp, NewState#node_state.last_heartbeat),
    %% Verify it's a valid timestamp
    {MegaSecs, Secs, MicroSecs} = NewState#node_state.last_heartbeat,
    ?assert(is_integer(MegaSecs)),
    ?assert(is_integer(Secs)),
    ?assert(is_integer(MicroSecs)).

cast_unknown() ->
    State = empty_state(),
    {noreply, State} = flurm_node:handle_cast(unknown_message, State),
    {noreply, State} = flurm_node:handle_cast({some, tuple, message}, State).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests", [
        {"unknown info is ignored", fun info_unknown/0},
        {"timeout info is ignored", fun info_timeout/0},
        {"arbitrary message is ignored", fun info_arbitrary/0}
    ]}.

info_unknown() ->
    State = empty_state(),
    {noreply, State} = flurm_node:handle_info(unknown_info, State).

info_timeout() ->
    State = empty_state(),
    {noreply, State} = flurm_node:handle_info(timeout, State).

info_arbitrary() ->
    State = empty_state(),
    {noreply, State} = flurm_node:handle_info({nodedown, some_node}, State),
    {noreply, State} = flurm_node:handle_info({'EXIT', self(), normal}, State).

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests", [
        {"terminate attempts unregister (noproc expected)", fun terminate_noproc/0},
        {"terminate with different reasons (noproc expected)", fun terminate_reasons_noproc/0}
    ]}.

%% Note: terminate/2 calls flurm_node_registry:unregister_node/1
%% Without a running registry, this will exit with noproc.
%% We verify the function is called by catching the expected error.
terminate_noproc() ->
    State = sample_state(),
    %% Expect noproc because flurm_node_registry is not running
    ?assertException(exit, {noproc, _}, flurm_node:terminate(normal, State)).

terminate_reasons_noproc() ->
    State = sample_state(),
    %% All termination reasons attempt to call the registry
    ?assertException(exit, {noproc, _}, flurm_node:terminate(shutdown, State)),
    State2 = State#node_state{name = <<"test2">>},
    ?assertException(exit, {noproc, _}, flurm_node:terminate({shutdown, term}, State2)).

%%====================================================================
%% code_change Tests
%%====================================================================

code_change_test_() ->
    {"code_change tests", [
        {"code_change same version", fun code_change_same_version/0},
        {"code_change old version", fun code_change_old_version/0},
        {"code_change with extra", fun code_change_with_extra/0}
    ]}.

code_change_same_version() ->
    State = sample_state(),
    {ok, NewState} = flurm_node:code_change("1.0.0", State, []),
    ?assertEqual(?NODE_STATE_VERSION, NewState#node_state.version).

code_change_old_version() ->
    State = sample_state(),
    OldState = State#node_state{version = 0},
    {ok, NewState} = flurm_node:code_change("0.9.0", OldState, []),
    ?assertEqual(?NODE_STATE_VERSION, NewState#node_state.version).

code_change_with_extra() ->
    State = sample_state(),
    {ok, NewState} = flurm_node:code_change("1.0.0", State, {some, extra, data}),
    ?assertEqual(?NODE_STATE_VERSION, NewState#node_state.version),
    ?assertEqual(State#node_state.name, NewState#node_state.name).

%%====================================================================
%% Integration-style Tests (using callbacks directly)
%%====================================================================

integration_test_() ->
    {"integration-style callback tests", [
        {"full lifecycle: init -> allocate -> release", fun lifecycle_alloc_release/0},
        {"drain workflow", fun workflow_drain/0},
        {"state transitions", fun state_transitions/0}
    ]}.

lifecycle_alloc_release() ->
    %% Init a node
    Spec = #node_spec{
        name = <<"lifecycle-node">>,
        hostname = <<"lifecycle.local">>,
        port = 6818,
        cpus = 16,
        memory = 32768,
        gpus = 2,
        features = [gpu],
        partitions = [<<"default">>]
    },
    {ok, State0} = flurm_node:init([Spec]),
    ?assertEqual([], State0#node_state.jobs),

    %% Allocate job 1
    {reply, ok, State1} = flurm_node:handle_call({allocate, 1001, 4, 8192, 1}, {self(), make_ref()}, State0),
    ?assertEqual([1001], State1#node_state.jobs),
    ?assertEqual(4, State1#node_state.cpus_used),

    %% Allocate job 2
    {reply, ok, State2} = flurm_node:handle_call({allocate, 1002, 4, 8192, 1}, {self(), make_ref()}, State1),
    ?assertEqual(2, length(State2#node_state.jobs)),
    ?assertEqual(8, State2#node_state.cpus_used),
    ?assertEqual(2, State2#node_state.gpus_used),

    %% Try to allocate more GPUs than available
    {reply, {error, insufficient_resources}, State2} =
        flurm_node:handle_call({allocate, 1003, 1, 1024, 1}, {self(), make_ref()}, State2),

    %% Release job 1
    {reply, ok, State3} = flurm_node:handle_call({release, 1001}, {self(), make_ref()}, State2),
    ?assertEqual(1, length(State3#node_state.jobs)),
    ?assert(State3#node_state.gpus_used < State2#node_state.gpus_used),

    %% Release job 2
    {reply, ok, State4} = flurm_node:handle_call({release, 1002}, {self(), make_ref()}, State3),
    ?assertEqual([], State4#node_state.jobs).

workflow_drain() ->
    %% Start with a node that has jobs
    State0 = sample_state(),
    ?assertEqual(up, State0#node_state.state),

    %% Drain the node
    {reply, ok, State1} = flurm_node:handle_call({drain, <<"maintenance window">>}, {self(), make_ref()}, State0),
    ?assertEqual(drain, State1#node_state.state),
    ?assertEqual(<<"maintenance window">>, State1#node_state.drain_reason),

    %% Check drain status
    {reply, true, State1} = flurm_node:handle_call(is_draining, {self(), make_ref()}, State1),
    {reply, {ok, <<"maintenance window">>}, State1} = flurm_node:handle_call(get_drain_reason, {self(), make_ref()}, State1),

    %% Try to allocate (should fail)
    {reply, {error, insufficient_resources}, State1} =
        flurm_node:handle_call({allocate, 9001, 1, 1024, 0}, {self(), make_ref()}, State1),

    %% Release existing job
    {reply, ok, State2} = flurm_node:handle_call({release, 1001}, {self(), make_ref()}, State1),
    ?assertEqual([], State2#node_state.jobs),
    ?assertEqual(drain, State2#node_state.state),

    %% Undrain the node
    {reply, ok, State3} = flurm_node:handle_call(undrain, {self(), make_ref()}, State2),
    ?assertEqual(up, State3#node_state.state),
    ?assertEqual(undefined, State3#node_state.drain_reason),

    %% Now allocation should work
    {reply, ok, State4} = flurm_node:handle_call({allocate, 9001, 1, 1024, 0}, {self(), make_ref()}, State3),
    ?assertEqual([9001], State4#node_state.jobs).

state_transitions() ->
    State0 = empty_state(),
    ?assertEqual(up, State0#node_state.state),

    %% up -> drain
    {reply, ok, State1} = flurm_node:handle_call({set_state, drain}, {self(), make_ref()}, State0),
    ?assertEqual(drain, State1#node_state.state),

    %% drain -> down
    {reply, ok, State2} = flurm_node:handle_call({set_state, down}, {self(), make_ref()}, State1),
    ?assertEqual(down, State2#node_state.state),

    %% down -> maint
    {reply, ok, State3} = flurm_node:handle_call({set_state, maint}, {self(), make_ref()}, State2),
    ?assertEqual(maint, State3#node_state.state),

    %% maint -> up
    {reply, ok, State4} = flurm_node:handle_call({set_state, up}, {self(), make_ref()}, State3),
    ?assertEqual(up, State4#node_state.state).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {"edge case tests", [
        {"zero GPU node", fun zero_gpu_node/0},
        {"large resource values", fun large_resource_values/0},
        {"many jobs", fun many_jobs/0},
        {"empty partitions", fun empty_partitions/0},
        {"release from single job state", fun release_single_job/0}
    ]}.

zero_gpu_node() ->
    Spec = #node_spec{
        name = <<"no-gpu">>,
        hostname = <<"no-gpu.local">>,
        port = 6818,
        cpus = 32,
        memory = 65536,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(0, State#node_state.gpus),

    %% Allocation requiring GPUs should fail
    {reply, {error, insufficient_resources}, State} =
        flurm_node:handle_call({allocate, 1001, 1, 1024, 1}, {self(), make_ref()}, State),

    %% Allocation without GPUs should succeed
    {reply, ok, _} = flurm_node:handle_call({allocate, 1001, 1, 1024, 0}, {self(), make_ref()}, State).

large_resource_values() ->
    Spec = #node_spec{
        name = <<"bignode">>,
        hostname = <<"bignode.local">>,
        port = 6818,
        cpus = 1024,
        memory = 4194304,  %% 4 TB
        gpus = 64,
        features = [huge, dgx],
        partitions = [<<"huge">>]
    },
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual(1024, State#node_state.cpus),
    ?assertEqual(4194304, State#node_state.memory),

    %% Large allocation should work
    {reply, ok, State1} = flurm_node:handle_call({allocate, 1001, 512, 2097152, 32}, {self(), make_ref()}, State),
    ?assertEqual(512, State1#node_state.cpus_used),
    ?assertEqual(2097152, State1#node_state.memory_used).

many_jobs() ->
    State0 = #node_state{
        name = <<"manyjobs">>,
        hostname = <<"manyjobs.local">>,
        port = 6818,
        cpus = 128,
        cpus_used = 0,
        memory = 262144,
        memory_used = 0,
        gpus = 0,
        gpus_used = 0,
        state = up,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    },

    %% Add 100 jobs
    StateN = lists:foldl(fun(JobId, AccState) ->
        {reply, ok, NewState} = flurm_node:handle_call({allocate, JobId, 1, 1024, 0}, {self(), make_ref()}, AccState),
        NewState
    end, State0, lists:seq(1, 100)),

    ?assertEqual(100, length(StateN#node_state.jobs)),
    ?assertEqual(100, StateN#node_state.cpus_used),

    %% List all jobs
    {reply, {ok, Jobs}, StateN} = flurm_node:handle_call(list_jobs, {self(), make_ref()}, StateN),
    ?assertEqual(100, length(Jobs)).

empty_partitions() ->
    Spec = #node_spec{
        name = <<"no-part">>,
        hostname = <<"no-part.local">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = []
    },
    {ok, State} = flurm_node:init([Spec]),
    ?assertEqual([], State#node_state.partitions).

release_single_job() ->
    %% Test release when there's only one job (edge case for resource calculation)
    State = #node_state{
        name = <<"singlejob">>,
        hostname = <<"singlejob.local">>,
        port = 6818,
        cpus = 8,
        cpus_used = 4,
        memory = 16384,
        memory_used = 8192,
        gpus = 2,
        gpus_used = 1,
        state = up,
        drain_reason = undefined,
        features = [],
        partitions = [<<"default">>],
        jobs = [1001],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    },
    {reply, ok, NewState} = flurm_node:handle_call({release, 1001}, {self(), make_ref()}, State),
    ?assertEqual([], NewState#node_state.jobs),
    %% Resources should be released (at least partially, based on per-job calculation)
    ?assert(NewState#node_state.cpus_used < State#node_state.cpus_used).

%%====================================================================
%% Maint state Tests
%%====================================================================

maint_state_test_() ->
    {"maintenance state tests", [
        {"allocate on maint node fails", fun maint_allocate_fails/0},
        {"maint preserves drain reason when set via set_state", fun maint_preserves_reason/0}
    ]}.

maint_allocate_fails() ->
    State = empty_state(),
    {reply, ok, MaintState} = flurm_node:handle_call({set_state, maint}, {self(), make_ref()}, State),
    {reply, {error, insufficient_resources}, MaintState} =
        flurm_node:handle_call({allocate, 1001, 1, 1024, 0}, {self(), make_ref()}, MaintState).

maint_preserves_reason() ->
    State = drained_state(),
    OrigReason = State#node_state.drain_reason,
    {reply, ok, MaintState} = flurm_node:handle_call({set_state, maint}, {self(), make_ref()}, State),
    ?assertEqual(maint, MaintState#node_state.state),
    %% Note: set_state doesn't touch drain_reason, only drain/undrain do
    ?assertEqual(OrigReason, MaintState#node_state.drain_reason).
