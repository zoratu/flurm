%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_system_monitor module
%%%
%%% Tests the gen_server callbacks and internal logic directly
%%% without mocking. Creates state records and calls handlers directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_system_monitor_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Replicate the state record for testing
-record(state, {
    hostname :: binary(),
    cpus :: pos_integer(),
    total_memory_mb :: pos_integer(),
    load_avg :: float(),
    load_avg_5 :: float(),
    load_avg_15 :: float(),
    free_memory_mb :: non_neg_integer(),
    cached_memory_mb :: non_neg_integer(),
    available_memory_mb :: non_neg_integer(),
    gpus :: list(),
    disk_usage :: map(),
    platform :: linux | darwin | other,
    gpu_allocation :: #{non_neg_integer() => pos_integer()}
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a test state with minimal valid data
test_state() ->
    #state{
        hostname = <<"testhost">>,
        cpus = 4,
        total_memory_mb = 8192,
        load_avg = 1.5,
        load_avg_5 = 1.2,
        load_avg_15 = 0.8,
        free_memory_mb = 4096,
        cached_memory_mb = 1024,
        available_memory_mb = 5120,
        gpus = [
            #{index => 0, name => <<"GPU 0">>, type => nvidia, memory_mb => 8192},
            #{index => 1, name => <<"GPU 1">>, type => nvidia, memory_mb => 8192}
        ],
        disk_usage = #{<<"/">> => #{total_mb => 100000, used_mb => 50000, available_mb => 50000}},
        platform = linux,
        gpu_allocation = #{}
    }.

%% State with GPUs already allocated
test_state_with_allocation() ->
    State = test_state(),
    State#state{gpu_allocation = #{0 => 100}}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

flurm_system_monitor_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% handle_call tests
      {"handle_call get_metrics returns correct map",
       fun handle_call_get_metrics_test/0},
      {"handle_call get_hostname returns hostname",
       fun handle_call_get_hostname_test/0},
      {"handle_call get_gpus returns gpu list",
       fun handle_call_get_gpus_test/0},
      {"handle_call get_disk_usage returns disk info",
       fun handle_call_get_disk_usage_test/0},
      {"handle_call get_gpu_allocation returns allocation map",
       fun handle_call_get_gpu_allocation_test/0},
      {"handle_call unknown request returns error",
       fun handle_call_unknown_test/0},

      %% GPU allocation tests
      {"allocate_gpus with 0 GPUs requested returns empty list",
       fun allocate_gpus_zero_test/0},
      {"allocate_gpus with available GPUs succeeds",
       fun allocate_gpus_success_test/0},
      {"allocate_gpus when not enough GPUs available fails",
       fun allocate_gpus_not_enough_test/0},
      {"allocate_gpus with partial allocation",
       fun allocate_gpus_partial_test/0},

      %% handle_cast tests
      {"handle_cast release_gpus removes allocation",
       fun handle_cast_release_gpus_test/0},
      {"handle_cast release_gpus for non-existent job is no-op",
       fun handle_cast_release_gpus_nonexistent_test/0},
      {"handle_cast unknown message is ignored",
       fun handle_cast_unknown_test/0},

      %% handle_info tests
      {"handle_info collect updates metrics",
       fun handle_info_collect_test/0},
      {"handle_info unknown message is ignored",
       fun handle_info_unknown_test/0},

      %% terminate test
      {"terminate returns ok",
       fun terminate_test/0},

      %% Module exports test
      {"module exports all expected functions",
       fun exports_test/0}
     ]}.

%%====================================================================
%% handle_call Tests
%%====================================================================

handle_call_get_metrics_test() ->
    State = test_state(),
    {reply, Metrics, NewState} = flurm_system_monitor:handle_call(get_metrics, {self(), make_ref()}, State),

    %% Verify all expected keys are present
    ?assertEqual(<<"testhost">>, maps:get(hostname, Metrics)),
    ?assertEqual(4, maps:get(cpus, Metrics)),
    ?assertEqual(8192, maps:get(total_memory_mb, Metrics)),
    ?assertEqual(4096, maps:get(free_memory_mb, Metrics)),
    ?assertEqual(5120, maps:get(available_memory_mb, Metrics)),
    ?assertEqual(1024, maps:get(cached_memory_mb, Metrics)),
    ?assertEqual(1.5, maps:get(load_avg, Metrics)),
    ?assertEqual(1.2, maps:get(load_avg_5, Metrics)),
    ?assertEqual(0.8, maps:get(load_avg_15, Metrics)),
    ?assertEqual(2, maps:get(gpu_count, Metrics)),

    %% State should be unchanged
    ?assertEqual(State, NewState).

handle_call_get_hostname_test() ->
    State = test_state(),
    {reply, Hostname, NewState} = flurm_system_monitor:handle_call(get_hostname, {self(), make_ref()}, State),

    ?assertEqual(<<"testhost">>, Hostname),
    ?assertEqual(State, NewState).

handle_call_get_gpus_test() ->
    State = test_state(),
    {reply, GPUs, NewState} = flurm_system_monitor:handle_call(get_gpus, {self(), make_ref()}, State),

    ?assertEqual(2, length(GPUs)),
    ?assertEqual([
        #{index => 0, name => <<"GPU 0">>, type => nvidia, memory_mb => 8192},
        #{index => 1, name => <<"GPU 1">>, type => nvidia, memory_mb => 8192}
    ], GPUs),
    ?assertEqual(State, NewState).

handle_call_get_disk_usage_test() ->
    State = test_state(),
    {reply, DiskUsage, NewState} = flurm_system_monitor:handle_call(get_disk_usage, {self(), make_ref()}, State),

    ?assert(maps:is_key(<<"/">>, DiskUsage)),
    ?assertEqual(State, NewState).

handle_call_get_gpu_allocation_test() ->
    State = test_state_with_allocation(),
    {reply, Allocation, NewState} = flurm_system_monitor:handle_call(get_gpu_allocation, {self(), make_ref()}, State),

    ?assertEqual(#{0 => 100}, Allocation),
    ?assertEqual(State, NewState).

handle_call_unknown_test() ->
    State = test_state(),
    {reply, Result, NewState} = flurm_system_monitor:handle_call(unknown_request, {self(), make_ref()}, State),

    ?assertEqual({error, unknown_request}, Result),
    ?assertEqual(State, NewState).

%%====================================================================
%% GPU Allocation Tests
%%====================================================================

allocate_gpus_zero_test() ->
    State = test_state(),
    {reply, Result, NewState} = flurm_system_monitor:handle_call({allocate_gpus, 1, 0}, {self(), make_ref()}, State),

    ?assertEqual({ok, []}, Result),
    ?assertEqual(State, NewState).

allocate_gpus_success_test() ->
    State = test_state(),
    {reply, Result, NewState} = flurm_system_monitor:handle_call({allocate_gpus, 101, 2}, {self(), make_ref()}, State),

    ?assertMatch({ok, [_, _]}, Result),
    {ok, AllocatedIndices} = Result,
    ?assertEqual(2, length(AllocatedIndices)),

    %% Verify allocation is recorded
    GpuAllocation = NewState#state.gpu_allocation,
    ?assertEqual(2, maps:size(GpuAllocation)),
    lists:foreach(fun(Idx) ->
        ?assertEqual(101, maps:get(Idx, GpuAllocation))
    end, AllocatedIndices).

allocate_gpus_not_enough_test() ->
    State = test_state(),
    %% Request more GPUs than available
    {reply, Result, NewState} = flurm_system_monitor:handle_call({allocate_gpus, 102, 5}, {self(), make_ref()}, State),

    ?assertEqual({error, not_enough_gpus}, Result),
    ?assertEqual(State, NewState).

allocate_gpus_partial_test() ->
    %% Start with one GPU already allocated
    State = test_state_with_allocation(),

    %% Request 1 GPU - should succeed since 1 is available
    {reply, Result, NewState} = flurm_system_monitor:handle_call({allocate_gpus, 102, 1}, {self(), make_ref()}, State),

    ?assertMatch({ok, [_]}, Result),
    {ok, [AllocatedIdx]} = Result,

    %% Should allocate GPU 1 (since GPU 0 is taken)
    ?assertEqual(1, AllocatedIdx),

    %% Verify both allocations exist
    GpuAllocation = NewState#state.gpu_allocation,
    ?assertEqual(2, maps:size(GpuAllocation)),
    ?assertEqual(100, maps:get(0, GpuAllocation)),
    ?assertEqual(102, maps:get(1, GpuAllocation)).

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_release_gpus_test() ->
    %% State with GPUs allocated to job 100
    State = test_state(),
    StateWithAlloc = State#state{gpu_allocation = #{0 => 100, 1 => 100}},

    {noreply, NewState} = flurm_system_monitor:handle_cast({release_gpus, 100}, StateWithAlloc),

    %% All GPUs for job 100 should be released
    ?assertEqual(#{}, NewState#state.gpu_allocation).

handle_cast_release_gpus_nonexistent_test() ->
    State = test_state_with_allocation(),

    %% Release GPUs for a job that doesn't have any
    {noreply, NewState} = flurm_system_monitor:handle_cast({release_gpus, 999}, State),

    %% Allocation should be unchanged
    ?assertEqual(State#state.gpu_allocation, NewState#state.gpu_allocation).

handle_cast_unknown_test() ->
    State = test_state(),
    {noreply, NewState} = flurm_system_monitor:handle_cast(unknown_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_collect_test() ->
    State = test_state(),

    %% Call handle_info with collect message
    {noreply, NewState} = flurm_system_monitor:handle_info(collect, State),

    %% State should be updated (values depend on system)
    %% At minimum, state should be returned and be a valid record
    ?assert(is_record(NewState, state)),
    ?assert(is_float(NewState#state.load_avg)),
    ?assert(is_float(NewState#state.load_avg_5)),
    ?assert(is_float(NewState#state.load_avg_15)),
    ?assert(is_integer(NewState#state.free_memory_mb)),
    ?assert(is_map(NewState#state.disk_usage)).

handle_info_unknown_test() ->
    State = test_state(),
    {noreply, NewState} = flurm_system_monitor:handle_info(unknown_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate Test
%%====================================================================

terminate_test() ->
    State = test_state(),
    Result = flurm_system_monitor:terminate(normal, State),
    ?assertEqual(ok, Result).

%%====================================================================
%% Module Exports Test
%%====================================================================

exports_test() ->
    Exports = flurm_system_monitor:module_info(exports),

    %% API functions
    ?assert(lists:member({start_link, 0}, Exports)),
    ?assert(lists:member({get_metrics, 0}, Exports)),
    ?assert(lists:member({get_hostname, 0}, Exports)),
    ?assert(lists:member({get_gpus, 0}, Exports)),
    ?assert(lists:member({get_disk_usage, 0}, Exports)),
    ?assert(lists:member({get_gpu_allocation, 0}, Exports)),
    ?assert(lists:member({allocate_gpus, 2}, Exports)),
    ?assert(lists:member({release_gpus, 1}, Exports)),

    %% gen_server callbacks
    ?assert(lists:member({init, 1}, Exports)),
    ?assert(lists:member({handle_call, 3}, Exports)),
    ?assert(lists:member({handle_cast, 2}, Exports)),
    ?assert(lists:member({handle_info, 2}, Exports)),
    ?assert(lists:member({terminate, 2}, Exports)).
