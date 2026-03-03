%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_chaos hard-to-hit branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_chaos_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

-record(delay_config, {
    min_delay_ms = 10 :: non_neg_integer(),
    max_delay_ms = 500 :: non_neg_integer(),
    target_modules = [] :: [module()],
    exclude_modules = [flurm_chaos] :: [module()]
}).

-record(kill_config, {
    max_kills_per_tick = 1 :: pos_integer(),
    kill_signal = chaos_kill :: term(),
    respect_links = true :: boolean()
}).

-record(partition_config, {
    duration_ms = 5000 :: pos_integer(),
    auto_heal = true :: boolean(),
    block_mode = disconnect :: disconnect | filter
}).

-record(gc_config, {
    max_processes_per_tick = 10 :: pos_integer(),
    target_heap_size = undefined :: undefined | pos_integer(),
    aggressive = false :: boolean()
}).

-record(scheduler_config, {
    min_suspend_ms = 1 :: pos_integer(),
    max_suspend_ms = 100 :: pos_integer(),
    affect_dirty_schedulers = false :: boolean()
}).

-record(state, {
    enabled = false :: boolean(),
    scenarios = #{} :: #{atom() => float()},
    scenario_enabled = #{} :: #{atom() => boolean()},
    tick_ref :: reference() | undefined,
    tick_ms :: pos_integer(),
    stats :: #{atom() => non_neg_integer()},
    protected_apps :: [atom()],
    protected_pids :: sets:set(pid()),
    seed :: rand:state(),
    partitioned_nodes :: #{node() => reference()},
    message_filters :: #{node() => fun()},
    delay_config :: #delay_config{},
    kill_config :: #kill_config{},
    partition_config :: #partition_config{},
    gc_config :: #gc_config{},
    scheduler_config :: #scheduler_config{}
}).

chaos_tail_cov_test_() ->
    [
     {"maybe_delay_call probability false branch",
      fun maybe_delay_probability_false_branch_test/0},
     {"maybe_delay_call module-not-targeted branch",
      fun maybe_delay_module_not_targeted_branch_test/0},
     {"maybe_delay_call catch branch when server missing",
      fun maybe_delay_catch_branch_test/0},
     {"kill_random_process no killable process branch",
      fun kill_random_process_no_killable_branch_test/0},
     {"find_killable_process empty branch",
      fun find_killable_empty_branch_test/0},
     {"is_killable protected/undefined/catch branches",
      fun is_killable_edge_branches_test/0},
     {"partition_node auto_heal=false branch",
      fun partition_node_auto_heal_false_branch_test/0},
     {"heal_partition undefined timer branch",
      fun heal_partition_undefined_timer_branch_test/0},
     {"heal_all_partitions undefined timer branch",
      fun heal_all_partitions_undefined_timer_branch_test/0},
     {"gc_all_processes non-true gc branch",
      fun gc_all_processes_non_true_branch_test/0},
     {"trigger_gc empty-processes branch",
      fun trigger_gc_empty_processes_branch_test/0},
     {"network_partition available-nodes branch",
      fun network_partition_available_nodes_branch_test/0},
     {"do_suspend_schedulers timeout-kill branch",
      fun do_suspend_schedulers_timeout_branch_test/0}
    ].

maybe_delay_probability_false_branch_test() ->
    Pid = start_chaos(#{
        scenarios => #{delay_message => 0.0},
        scenario_enabled => #{delay_message => true},
        delay_config => #{target_modules => [target_mod], exclude_modules => []}
    }),
    try
        ok = flurm_chaos:enable(),
        ?assertEqual(ok, flurm_chaos:maybe_delay_call(target_mod))
    after
        stop_chaos(Pid)
    end.

maybe_delay_module_not_targeted_branch_test() ->
    Pid = start_chaos(#{
        scenarios => #{delay_message => 1.0},
        scenario_enabled => #{delay_message => true},
        delay_config => #{target_modules => [other_mod], exclude_modules => []}
    }),
    try
        ok = flurm_chaos:enable(),
        ?assertEqual(ok, flurm_chaos:maybe_delay_call(target_mod))
    after
        stop_chaos(Pid)
    end.

maybe_delay_catch_branch_test() ->
    stop_chaos(whereis(flurm_chaos)),
    ?assertEqual(ok, flurm_chaos:maybe_delay_call(target_mod)).

kill_random_process_no_killable_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    AllPids = sets:from_list(erlang:processes()),
    State1 = State0#state{protected_pids = AllPids},
    ?assertEqual({error, no_killable_process},
                 flurm_chaos:execute_scenario(kill_random_process, State1)).

find_killable_empty_branch_test() ->
    AllPids = sets:from_list(erlang:processes()),
    ?assertEqual(none, flurm_chaos:find_killable_process([], AllPids)).

is_killable_edge_branches_test() ->
    Protected = sets:add_element(self(), sets:new()),
    ?assertEqual(false, flurm_chaos:is_killable(self(), [], Protected)),

    DeadPid = spawn(fun() -> ok end),
    timer:sleep(20),
    ?assertEqual(false, flurm_chaos:is_killable(DeadPid, [], sets:new())),

    ?assertEqual(false, flurm_chaos:is_killable(not_a_pid, [], sets:new())).

partition_node_auto_heal_false_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    From = {self(), make_ref()},
    Node = 'existing@node',
    %% Seed partition map so membership check passes without real cluster nodes.
    State1 = State0#state{partitioned_nodes = #{Node => make_ref()}},
    {reply, ok, State2} = flurm_chaos:handle_call(
        {partition_node, Node, #{auto_heal => false, duration_ms => 1}}, From, State1),
    ?assertEqual(undefined, maps:get(Node, State2#state.partitioned_nodes)).

heal_partition_undefined_timer_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    From = {self(), make_ref()},
    Node = 'manual@node',
    State1 = State0#state{partitioned_nodes = #{Node => undefined}},
    {reply, ok, State2} = flurm_chaos:handle_call({heal_partition, Node}, From, State1),
    ?assertEqual(false, maps:is_key(Node, State2#state.partitioned_nodes)).

heal_all_partitions_undefined_timer_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    From = {self(), make_ref()},
    State1 = State0#state{partitioned_nodes = #{'a@n' => undefined, 'b@n' => make_ref()}},
    {reply, ok, State2} = flurm_chaos:handle_call(heal_all_partitions, From, State1),
    ?assertEqual(#{}, State2#state.partitioned_nodes).

gc_all_processes_non_true_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    OldProcs = application:get_env(flurm_core, chaos_processes_fun),
    OldGc = application:get_env(flurm_core, chaos_gc_fun),
    application:set_env(flurm_core, chaos_processes_fun, fun() -> [fake_pid] end),
    application:set_env(flurm_core, chaos_gc_fun, fun(_) -> false end),
    try
        {reply, {ok, 0}, _} = flurm_chaos:handle_call(gc_all_processes, {self(), make_ref()}, State0)
    after
        restore_env(chaos_processes_fun, OldProcs),
        restore_env(chaos_gc_fun, OldGc)
    end.

trigger_gc_empty_processes_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    OldProcs = application:get_env(flurm_core, chaos_processes_fun),
    application:set_env(flurm_core, chaos_processes_fun, fun() -> [] end),
    try
        ?assertEqual(ok, flurm_chaos:execute_scenario(trigger_gc, State0))
    after
        restore_env(chaos_processes_fun, OldProcs)
    end.

network_partition_available_nodes_branch_test() ->
    {ok, State0} = flurm_chaos:init(#{}),
    State1 = State0#state{
        partition_config = #partition_config{duration_ms = 1, auto_heal = true, block_mode = disconnect}
    },
    OldNodes = application:get_env(flurm_core, chaos_nodes_fun),
    OldDisc = application:get_env(flurm_core, chaos_disconnect_fun),
    OldPing = application:get_env(flurm_core, chaos_ping_fun),
    application:set_env(flurm_core, chaos_nodes_fun, fun() -> ['n1@test'] end),
    application:set_env(flurm_core, chaos_disconnect_fun, fun(_) -> true end),
    application:set_env(flurm_core, chaos_ping_fun, fun(_) -> pong end),
    try
        ?assertEqual(ok, flurm_chaos:execute_scenario(network_partition, State1)),
        timer:sleep(20)
    after
        restore_env(chaos_nodes_fun, OldNodes),
        restore_env(chaos_disconnect_fun, OldDisc),
        restore_env(chaos_ping_fun, OldPing)
    end.

do_suspend_schedulers_timeout_branch_test() ->
    OldSpawn = application:get_env(flurm_core, chaos_spawn_scheduler_fun),
    application:set_env(flurm_core, chaos_spawn_scheduler_fun,
        fun(_SchedId, _DurationMs) ->
            spawn(fun() -> receive after 10000 -> ok end end)
        end),
    try
        ok = flurm_chaos:do_suspend_schedulers(1, #scheduler_config{})
    after
        restore_env(chaos_spawn_scheduler_fun, OldSpawn)
    end.

start_chaos(Opts) ->
    stop_chaos(whereis(flurm_chaos)),
    {ok, Pid} = flurm_chaos:start_link(Opts),
    Pid.

stop_chaos(undefined) ->
    ok;
stop_chaos(Pid) when is_pid(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(20),
    ok.

restore_env(Key, undefined) ->
    application:unset_env(flurm_core, Key);
restore_env(Key, {ok, Value}) ->
    application:set_env(flurm_core, Key, Value).
