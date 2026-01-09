%%%-------------------------------------------------------------------
%%% @doc FLURM Scheduler Tests
%%%
%%% EUnit tests for the flurm_scheduler gen_server, flurm_node,
%%% flurm_node_sup, flurm_node_registry, and flurm_partition modules.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_tests).

-compile(nowarn_unused_function).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for all tests
scheduler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Submit job, verify it gets scheduled", fun test_submit_and_schedule/0},
        {"Submit multiple jobs, verify FIFO order", fun test_fifo_order/0},
        {"Resource exhaustion, jobs wait", fun test_resource_exhaustion/0},
        {"Job completion frees resources", fun test_job_completion_frees_resources/0},
        {"Node failure handling", fun test_node_failure/0},
        {"Scheduler stats", fun test_scheduler_stats/0},
        {"Partition operations", fun test_partition_operations/0},
        {"Node operations", fun test_node_operations/0},
        {"Node registry operations", fun test_node_registry_operations/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),

    %% Start registries and supervisors
    {ok, JobRegistryPid} = flurm_job_registry:start_link(),
    {ok, JobSupPid} = flurm_job_sup:start_link(),
    {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
    {ok, NodeSupPid} = flurm_node_sup:start_link(),
    {ok, SchedulerPid} = flurm_scheduler:start_link(),

    #{
        job_registry => JobRegistryPid,
        job_sup => JobSupPid,
        node_registry => NodeRegistryPid,
        node_sup => NodeSupPid,
        scheduler => SchedulerPid
    }.

cleanup(#{job_registry := JobRegistryPid,
          job_sup := JobSupPid,
          node_registry := NodeRegistryPid,
          node_sup := NodeSupPid,
          scheduler := SchedulerPid}) ->
    %% Stop all jobs first
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    %% Stop all nodes
    [flurm_node_sup:stop_node(Pid) || Pid <- flurm_node_sup:which_nodes()],
    %% Stop processes
    catch exit(SchedulerPid, shutdown),
    catch exit(NodeSupPid, shutdown),
    catch exit(NodeRegistryPid, shutdown),
    catch exit(JobSupPid, shutdown),
    catch exit(JobRegistryPid, shutdown),
    timer:sleep(100),
    ok.

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

make_node_spec(Name) ->
    make_node_spec(Name, #{}).

make_node_spec(Name, Overrides) ->
    Defaults = #{
        hostname => <<"localhost">>,
        port => 5555,
        cpus => 8,
        memory => 16384,
        gpus => 0,
        features => [],
        partitions => [<<"default">>]
    },
    Props = maps:merge(Defaults, Overrides),
    #node_spec{
        name = Name,
        hostname = maps:get(hostname, Props),
        port = maps:get(port, Props),
        cpus = maps:get(cpus, Props),
        memory = maps:get(memory, Props),
        gpus = maps:get(gpus, Props),
        features = maps:get(features, Props),
        partitions = maps:get(partitions, Props)
    }.

register_test_node(Name) ->
    register_test_node(Name, #{}).

register_test_node(Name, Overrides) ->
    NodeSpec = make_node_spec(Name, Overrides),
    {ok, Pid, Name} = flurm_node:register_node(NodeSpec),
    Pid.

%%====================================================================
%% Scheduler Tests
%%====================================================================

test_submit_and_schedule() ->
    %% Register a node that can run jobs
    _NodePid = register_test_node(<<"node1">>, #{cpus => 8, memory => 16384}),

    %% Submit a job
    JobSpec = make_job_spec(#{num_cpus => 4}),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Add job to scheduler
    ok = flurm_scheduler:submit_job(JobId),

    %% Wait for scheduling cycle
    timer:sleep(200),

    %% Verify job was scheduled (moved to configuring)
    {ok, JobState} = flurm_job:get_state(JobId),
    ?assertEqual(configuring, JobState),

    %% Verify job has allocated nodes
    {ok, Info} = flurm_job:get_info(JobId),
    ?assertEqual([<<"node1">>], maps:get(allocated_nodes, Info)),
    ok.

test_fifo_order() ->
    %% Register a node with limited resources (can only run one job at a time)
    _NodePid = register_test_node(<<"node1">>, #{cpus => 4, memory => 8192}),

    %% Submit multiple jobs that each require all resources
    Jobs = lists:map(
        fun(I) ->
            JobSpec = make_job_spec(#{num_cpus => 4}),
            {ok, _Pid, JobId} = flurm_job:submit(JobSpec),
            ok = flurm_scheduler:submit_job(JobId),
            {I, JobId}
        end,
        lists:seq(1, 3)
    ),

    %% Wait for scheduling cycle
    timer:sleep(200),

    %% First job should be scheduled
    {1, FirstJobId} = lists:nth(1, Jobs),
    {ok, FirstJobState} = flurm_job:get_state(FirstJobId),
    ?assertEqual(configuring, FirstJobState),

    %% Other jobs should still be pending (waiting for resources)
    {2, SecondJobId} = lists:nth(2, Jobs),
    {ok, SecondJobState} = flurm_job:get_state(SecondJobId),
    ?assertEqual(pending, SecondJobState),

    {3, ThirdJobId} = lists:nth(3, Jobs),
    {ok, ThirdJobState} = flurm_job:get_state(ThirdJobId),
    ?assertEqual(pending, ThirdJobState),
    ok.

test_resource_exhaustion() ->
    %% Register a small node
    _NodePid = register_test_node(<<"node1">>, #{cpus => 4, memory => 4096}),

    %% Submit a job requiring more resources than available
    JobSpec = make_job_spec(#{num_cpus => 8}),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),
    ok = flurm_scheduler:submit_job(JobId),

    %% Wait for scheduling cycle
    timer:sleep(200),

    %% Job should still be pending (insufficient resources)
    {ok, JobState} = flurm_job:get_state(JobId),
    ?assertEqual(pending, JobState),

    %% Check scheduler stats - should show pending job
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 1),
    ok.

test_job_completion_frees_resources() ->
    %% Register a node
    _NodePid = register_test_node(<<"node1">>, #{cpus => 4, memory => 8192}),

    %% Submit first job
    JobSpec1 = make_job_spec(#{num_cpus => 4}),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),
    ok = flurm_scheduler:submit_job(JobId1),

    %% Submit second job (will wait)
    JobSpec2 = make_job_spec(#{num_cpus => 4}),
    {ok, _Pid2, JobId2} = flurm_job:submit(JobSpec2),
    ok = flurm_scheduler:submit_job(JobId2),

    %% Wait for scheduling
    timer:sleep(200),

    %% First job should be scheduled
    {ok, configuring} = flurm_job:get_state(JobId1),

    %% Second job should be pending
    {ok, pending} = flurm_job:get_state(JobId2),

    %% Complete the first job
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),

    %% Notify scheduler
    ok = flurm_scheduler:job_completed(JobId1),

    %% Wait for scheduling cycle
    timer:sleep(200),

    %% Second job should now be scheduled
    {ok, Job2State} = flurm_job:get_state(JobId2),
    ?assertEqual(configuring, Job2State),
    ok.

test_node_failure() ->
    %% Register nodes
    _NodePid1 = register_test_node(<<"node1">>, #{cpus => 4, memory => 8192}),
    NodePid2 = register_test_node(<<"node2">>, #{cpus => 4, memory => 8192}),

    %% Submit a job
    JobSpec = make_job_spec(#{num_cpus => 4}),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),
    ok = flurm_scheduler:submit_job(JobId),

    %% Wait for scheduling
    timer:sleep(200),

    %% Job should be scheduled
    {ok, configuring} = flurm_job:get_state(JobId),

    %% Kill node2 (simulate failure)
    exit(NodePid2, kill),
    timer:sleep(100),

    %% Verify node2 is unregistered
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"node2">>)),

    %% node1 should still be available
    ?assertMatch({ok, _}, flurm_node_registry:lookup_node(<<"node1">>)),
    ok.

test_scheduler_stats() ->
    %% Register a node
    _NodePid = register_test_node(<<"node1">>, #{cpus => 8, memory => 16384}),

    %% Get initial stats
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    %% Submit some jobs
    lists:foreach(
        fun(_) ->
            JobSpec = make_job_spec(#{num_cpus => 2}),
            {ok, _Pid, JobId} = flurm_job:submit(JobSpec),
            ok = flurm_scheduler:submit_job(JobId)
        end,
        lists:seq(1, 3)
    ),

    %% Wait for scheduling
    timer:sleep(300),

    %% Get stats
    {ok, Stats} = flurm_scheduler:get_stats(),

    %% Verify stats are reasonable
    ?assert(maps:get(schedule_cycles, Stats) > InitCycles),
    ?assert(maps:get(running_count, Stats) >= 0),
    ok.

%%====================================================================
%% Partition Tests
%%====================================================================

test_partition_operations() ->
    %% Create a partition
    PartitionSpec = #partition_spec{
        name = <<"compute">>,
        nodes = [],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100
    },
    {ok, _Pid} = flurm_partition:start_link(PartitionSpec),

    %% Get info
    {ok, Info} = flurm_partition:get_info(<<"compute">>),
    ?assertEqual(<<"compute">>, maps:get(name, Info)),
    ?assertEqual([], maps:get(nodes, Info)),
    ?assertEqual(86400, maps:get(max_time, Info)),

    %% Add nodes
    ok = flurm_partition:add_node(<<"compute">>, <<"node1">>),
    ok = flurm_partition:add_node(<<"compute">>, <<"node2">>),

    {ok, Nodes} = flurm_partition:get_nodes(<<"compute">>),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"node1">>, Nodes)),
    ?assert(lists:member(<<"node2">>, Nodes)),

    %% Adding same node again fails
    {error, already_member} = flurm_partition:add_node(<<"compute">>, <<"node1">>),

    %% Remove node
    ok = flurm_partition:remove_node(<<"compute">>, <<"node1">>),
    {ok, RemainingNodes} = flurm_partition:get_nodes(<<"compute">>),
    ?assertEqual([<<"node2">>], RemainingNodes),

    %% Remove non-existent node
    {error, not_member} = flurm_partition:remove_node(<<"compute">>, <<"node3">>),

    %% State operations
    {ok, up} = flurm_partition:get_state(<<"compute">>),
    ok = flurm_partition:set_state(<<"compute">>, drain),
    {ok, drain} = flurm_partition:get_state(<<"compute">>),
    ok.

%%====================================================================
%% Node Tests
%%====================================================================

test_node_operations() ->
    %% Create a node
    NodeSpec = make_node_spec(<<"testnode">>, #{cpus => 8, memory => 16384, gpus => 2}),
    {ok, Pid, <<"testnode">>} = flurm_node:register_node(NodeSpec),

    %% Get info
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(<<"testnode">>, maps:get(name, Info)),
    ?assertEqual(8, maps:get(cpus, Info)),
    ?assertEqual(0, maps:get(cpus_used, Info)),
    ?assertEqual(8, maps:get(cpus_available, Info)),
    ?assertEqual(16384, maps:get(memory, Info)),
    ?assertEqual(2, maps:get(gpus, Info)),

    %% Allocate resources
    ok = flurm_node:allocate(Pid, 1, {4, 8192, 1}),

    {ok, Info2} = flurm_node:get_info(Pid),
    ?assertEqual(4, maps:get(cpus_used, Info2)),
    ?assertEqual(4, maps:get(cpus_available, Info2)),
    ?assertEqual(8192, maps:get(memory_used, Info2)),
    ?assertEqual(1, maps:get(gpus_used, Info2)),

    %% List jobs on node
    {ok, Jobs} = flurm_node:list_jobs(Pid),
    ?assertEqual([1], Jobs),

    %% Release resources
    ok = flurm_node:release(Pid, 1),

    {ok, Info3} = flurm_node:get_info(Pid),
    ?assertEqual(0, maps:get(cpus_used, Info3)),
    ?assertEqual([], maps:get(jobs, Info3)),

    %% State operations
    ok = flurm_node:set_state(Pid, drain),
    {ok, Info4} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info4)),

    %% Cannot allocate on drain node
    {error, insufficient_resources} = flurm_node:allocate(Pid, 2, {1, 1024, 0}),

    %% Heartbeat
    ok = flurm_node:heartbeat(Pid),
    ok.

test_node_registry_operations() ->
    %% Register some nodes
    _Pid1 = register_test_node(<<"node1">>, #{partitions => [<<"default">>, <<"gpu">>]}),
    _Pid2 = register_test_node(<<"node2">>, #{partitions => [<<"default">>]}),
    _Pid3 = register_test_node(<<"node3">>, #{partitions => [<<"gpu">>]}),

    %% List all nodes
    AllNodes = flurm_node_registry:list_nodes(),
    ?assertEqual(3, length(AllNodes)),

    %% List by state (all should be up)
    UpNodes = flurm_node_registry:list_nodes_by_state(up),
    ?assertEqual(3, length(UpNodes)),

    %% List by partition
    DefaultNodes = flurm_node_registry:list_nodes_by_partition(<<"default">>),
    ?assertEqual(2, length(DefaultNodes)),

    GpuNodes = flurm_node_registry:list_nodes_by_partition(<<"gpu">>),
    ?assertEqual(2, length(GpuNodes)),

    %% Get available nodes
    AvailNodes = flurm_node_registry:get_available_nodes({4, 8192, 0}),
    ?assertEqual(3, length(AvailNodes)),

    %% Lookup single node
    {ok, _} = flurm_node_registry:lookup_node(<<"node1">>),
    {error, not_found} = flurm_node_registry:lookup_node(<<"nonexistent">>),

    %% Get node entry
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"node1">>),
    ?assertEqual(<<"node1">>, Entry#node_entry.name),

    %% Count by state
    Counts = flurm_node_registry:count_by_state(),
    ?assertEqual(3, maps:get(up, Counts)),
    ?assertEqual(0, maps:get(down, Counts)),
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

%% Test allocating more resources than available
allocation_overflow_test_() ->
    {setup,
     fun() ->
         {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
         {ok, NodeSupPid} = flurm_node_sup:start_link(),
         #{node_registry => NodeRegistryPid, node_sup => NodeSupPid}
     end,
     fun(#{node_registry := NodeRegistryPid, node_sup := NodeSupPid}) ->
         catch exit(NodeSupPid, shutdown),
         catch exit(NodeRegistryPid, shutdown),
         timer:sleep(50)
     end,
     fun(_) ->
         {"Cannot allocate more resources than available", fun() ->
             NodeSpec = #node_spec{
                 name = <<"smallnode">>,
                 hostname = <<"localhost">>,
                 port = 5555,
                 cpus = 2,
                 memory = 1024,
                 gpus = 0,
                 features = [],
                 partitions = [<<"default">>]
             },
             {ok, Pid, _} = flurm_node:register_node(NodeSpec),

             %% Try to allocate more CPUs than available
             Result = flurm_node:allocate(Pid, 1, {4, 512, 0}),
             ?assertEqual({error, insufficient_resources}, Result),

             %% Try to allocate more memory than available
             Result2 = flurm_node:allocate(Pid, 2, {1, 2048, 0}),
             ?assertEqual({error, insufficient_resources}, Result2)
         end}
     end}.

%% Test releasing non-existent job
release_nonexistent_test_() ->
    {setup,
     fun() ->
         {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
         {ok, NodeSupPid} = flurm_node_sup:start_link(),
         #{node_registry => NodeRegistryPid, node_sup => NodeSupPid}
     end,
     fun(#{node_registry := NodeRegistryPid, node_sup := NodeSupPid}) ->
         catch exit(NodeSupPid, shutdown),
         catch exit(NodeRegistryPid, shutdown),
         timer:sleep(50)
     end,
     fun(_) ->
         {"Releasing non-existent job returns error", fun() ->
             NodeSpec = #node_spec{
                 name = <<"testnode">>,
                 hostname = <<"localhost">>,
                 port = 5555,
                 cpus = 8,
                 memory = 8192,
                 gpus = 0,
                 features = [],
                 partitions = [<<"default">>]
             },
             {ok, Pid, _} = flurm_node:register_node(NodeSpec),

             %% Try to release a job that doesn't exist
             Result = flurm_node:release(Pid, 999),
             ?assertEqual({error, job_not_found}, Result)
         end}
     end}.

%% Test partition max nodes limit
partition_max_nodes_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     fun(_) ->
         {"Cannot exceed max nodes in partition", fun() ->
             PartitionSpec = #partition_spec{
                 name = <<"limited">>,
                 nodes = [],
                 max_time = 3600,
                 default_time = 3600,
                 max_nodes = 2,
                 priority = 100
             },
             {ok, _Pid} = flurm_partition:start_link(PartitionSpec),

             ok = flurm_partition:add_node(<<"limited">>, <<"node1">>),
             ok = flurm_partition:add_node(<<"limited">>, <<"node2">>),

             %% Third node should fail
             Result = flurm_partition:add_node(<<"limited">>, <<"node3">>),
             ?assertEqual({error, max_nodes_reached}, Result)
         end}
     end}.

%% Test node monitor cleanup
node_monitor_cleanup_test_() ->
    {setup,
     fun() ->
         {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
         {ok, NodeSupPid} = flurm_node_sup:start_link(),
         #{node_registry => NodeRegistryPid, node_sup => NodeSupPid}
     end,
     fun(#{node_registry := NodeRegistryPid, node_sup := NodeSupPid}) ->
         catch exit(NodeSupPid, shutdown),
         catch exit(NodeRegistryPid, shutdown),
         timer:sleep(50)
     end,
     fun(_) ->
         {"Node automatically unregistered when process dies", fun() ->
             NodeSpec = #node_spec{
                 name = <<"dyingnode">>,
                 hostname = <<"localhost">>,
                 port = 5555,
                 cpus = 4,
                 memory = 8192,
                 gpus = 0,
                 features = [],
                 partitions = [<<"default">>]
             },
             {ok, Pid, <<"dyingnode">>} = flurm_node:register_node(NodeSpec),

             %% Verify node is registered
             {ok, Pid} = flurm_node_registry:lookup_node(<<"dyingnode">>),

             %% Kill the process
             exit(Pid, kill),
             timer:sleep(100),

             %% Node should be automatically unregistered
             ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"dyingnode">>))
         end}
     end}.
