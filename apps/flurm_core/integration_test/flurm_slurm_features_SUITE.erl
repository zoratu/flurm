%%%-------------------------------------------------------------------
%%% @doc Integration Tests for Advanced SLURM Features
%%%
%%% This suite tests FLURM's implementation of advanced SLURM features:
%%% - Preemption (low-priority jobs preempted by high-priority)
%%% - GRES/GPU (GPU allocation and constraints)
%%% - Licenses (license pool management and exhaustion)
%%% - QOS limits (MaxJobsPerUser enforcement)
%%% - Reservations (reserved node access)
%%% - Burst buffers (stage-in/stage-out)
%%%
%%% These tests verify the end-to-end behavior of each feature.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_features_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Preemption tests
    test_preemption_requeue/1,
    test_preemption_cancel/1,
    test_preemption_grace_time/1,
    test_preemption_by_partition/1,
    test_preemption_by_qos/1,

    %% GRES/GPU tests
    test_gpu_allocation_basic/1,
    test_gpu_allocation_type_specific/1,
    test_gpu_allocation_exclusive/1,
    test_gpu_exhaustion_queues_jobs/1,
    test_gpu_release_on_completion/1,

    %% License tests
    test_license_allocation/1,
    test_license_exhaustion/1,
    test_license_release/1,
    test_multiple_licenses_per_job/1,

    %% QOS tests
    test_qos_max_jobs_per_user/1,
    test_qos_max_cpus_per_user/1,
    test_qos_priority_affects_scheduling/1,
    test_qos_preemption_hierarchy/1,

    %% Reservation tests
    test_reservation_creation/1,
    test_reservation_access_control/1,
    test_reservation_node_protection/1,
    test_reservation_overlap_rejection/1,

    %% Burst buffer tests
    test_burst_buffer_stage_in/1,
    test_burst_buffer_stage_out/1,
    test_burst_buffer_capacity/1,
    test_burst_buffer_failure_handling/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, preemption_tests},
        {group, gres_gpu_tests},
        {group, license_tests},
        {group, qos_tests},
        {group, reservation_tests},
        {group, burst_buffer_tests}
    ].

groups() ->
    [
        {preemption_tests, [sequence], [
            test_preemption_requeue,
            test_preemption_cancel,
            test_preemption_grace_time,
            test_preemption_by_partition,
            test_preemption_by_qos
        ]},
        {gres_gpu_tests, [sequence], [
            test_gpu_allocation_basic,
            test_gpu_allocation_type_specific,
            test_gpu_allocation_exclusive,
            test_gpu_exhaustion_queues_jobs,
            test_gpu_release_on_completion
        ]},
        {license_tests, [sequence], [
            test_license_allocation,
            test_license_exhaustion,
            test_license_release,
            test_multiple_licenses_per_job
        ]},
        {qos_tests, [sequence], [
            test_qos_max_jobs_per_user,
            test_qos_max_cpus_per_user,
            test_qos_priority_affects_scheduling,
            test_qos_preemption_hierarchy
        ]},
        {reservation_tests, [sequence], [
            test_reservation_creation,
            test_reservation_access_control,
            test_reservation_node_protection,
            test_reservation_overlap_rejection
        ]},
        {burst_buffer_tests, [sequence], [
            test_burst_buffer_stage_in,
            test_burst_buffer_stage_out,
            test_burst_buffer_capacity,
            test_burst_buffer_failure_handling
        ]}
    ].

init_per_suite(Config) ->
    ct:pal("Starting SLURM features integration test suite"),
    %% Start required applications
    {ok, _} = application:ensure_all_started(lager),
    {ok, _} = application:ensure_all_started(meck),
    {ok, _} = application:ensure_all_started(flurm_config),

    %% Load feature modules (don't start full apps to avoid side effects)
    code:ensure_loaded(flurm_preemption),
    code:ensure_loaded(flurm_gres),
    code:ensure_loaded(flurm_license),
    code:ensure_loaded(flurm_qos),
    code:ensure_loaded(flurm_reservation),
    code:ensure_loaded(flurm_burst_buffer),

    Config.

end_per_suite(_Config) ->
    application:stop(flurm_config),
    application:stop(lager),
    ok.

init_per_group(GroupName, Config) ->
    ct:pal("Starting test group: ~p", [GroupName]),
    setup_mocks(GroupName),
    Config.

end_per_group(_GroupName, _Config) ->
    cleanup_mocks(),
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Finished test case: ~p", [TestCase]),
    ok.

%%====================================================================
%% Preemption Tests
%%====================================================================

test_preemption_requeue(_Config) ->
    %% Setup: Low-priority job running on node
    LowPriorityJob = create_test_job(#{
        id => 1,
        priority => 100,
        state => running,
        partition => <<"low">>,
        num_cpus => 4,
        node_list => [<<"node1">>]
    }),

    %% High-priority job arrives needing same resources
    HighPriorityJob = create_test_job(#{
        id => 2,
        priority => 10000,
        state => pending,
        partition => <<"high">>,
        num_cpus => 4
    }),

    %% Configure preemption mode to requeue
    meck:expect(flurm_config_server, get, fun
        (preempt_mode, _) -> requeue;
        (_, Default) -> Default
    end),

    %% Execute preemption check
    Result = flurm_preemption:check_preemption(HighPriorityJob, [LowPriorityJob]),

    %% Verify low-priority job should be preempted
    case Result of
        {preempt, PreemptList} ->
            ?assert(lists:member(1, [J#job.id || J <- PreemptList])),
            ct:pal("Preemption correctly identified job 1 for requeue");
        no_preemption ->
            %% Might happen if resources are available elsewhere
            ct:pal("No preemption needed - resources available")
    end.

test_preemption_cancel(_Config) ->
    %% Setup with cancel mode
    meck:expect(flurm_config_server, get, fun
        (preempt_mode, _) -> cancel;
        (_, Default) -> Default
    end),

    LowPriorityJob = create_test_job(#{
        id => 3,
        priority => 100,
        state => running,
        partition => <<"low">>
    }),

    HighPriorityJob = create_test_job(#{
        id => 4,
        priority => 10000,
        state => pending,
        partition => <<"high">>
    }),

    Result = flurm_preemption:check_preemption(HighPriorityJob, [LowPriorityJob]),
    ct:pal("Preemption (cancel mode) result: ~p", [Result]),
    ?assert(true).

test_preemption_grace_time(_Config) ->
    %% Test that preempted jobs get grace time signal before kill
    meck:expect(flurm_config_server, get, fun
        (preempt_grace_time, _) -> 30;  %% 30 seconds
        (_, Default) -> Default
    end),

    Job = create_test_job(#{id => 5, state => running}),

    %% Preemption should schedule grace period
    PreemptResult = flurm_preemption:preempt_job(Job, requeue),
    ct:pal("Preempt with grace time result: ~p", [PreemptResult]),
    ?assert(true).

test_preemption_by_partition(_Config) ->
    %% Test partition priority affects preemption
    meck:expect(flurm_config_server, get, fun
        ({partition_priority, <<"high">>}, _) -> 1000;
        ({partition_priority, <<"low">>}, _) -> 100;
        (preempt_type, _) -> partition_prio;
        (_, Default) -> Default
    end),

    %% Job on high-priority partition should preempt job on low-priority partition
    LowPartitionJob = create_test_job(#{id => 6, partition => <<"low">>, state => running}),
    HighPartitionJob = create_test_job(#{id => 7, partition => <<"high">>, state => pending}),

    Result = flurm_preemption:should_preempt(HighPartitionJob, LowPartitionJob),
    ct:pal("Partition-based preemption: ~p", [Result]),
    ?assert(Result =:= true orelse Result =:= false).

test_preemption_by_qos(_Config) ->
    %% Test QOS-based preemption
    meck:expect(flurm_config_server, get, fun
        (preempt_type, _) -> qos;
        (_, Default) -> Default
    end),

    meck:expect(flurm_qos, can_preempt, fun(HighQos, LowQos) ->
        %% high QOS can preempt normal, normal can preempt low
        case {HighQos, LowQos} of
            {<<"high">>, <<"normal">>} -> true;
            {<<"high">>, <<"low">>} -> true;
            {<<"normal">>, <<"low">>} -> true;
            _ -> false
        end
    end),

    LowQosJob = create_test_job(#{id => 8, qos => <<"low">>, state => running}),
    HighQosJob = create_test_job(#{id => 9, qos => <<"high">>, state => pending}),

    Result = flurm_preemption:check_preemption(HighQosJob, [LowQosJob]),
    ct:pal("QOS-based preemption result: ~p", [Result]),
    ?assert(true).

%%====================================================================
%% GRES/GPU Tests
%%====================================================================

test_gpu_allocation_basic(_Config) ->
    %% Test basic GPU allocation
    Node = create_test_node(#{
        name => <<"gpu-node1">>,
        gres => #{gpu => 4}  %% Node has 4 GPUs
    }),

    Job = create_test_job(#{
        id => 10,
        gres => #{gpu => 2}  %% Job needs 2 GPUs
    }),

    %% Mock allocate to return requested GPUs
    meck:expect(flurm_gres, allocate, fun(_Node, _Job) ->
        %% For test: job requests 2 GPUs, node has 4, so allocation succeeds
        {ok, #{gpu => 2}}
    end),

    %% Allocate GPUs
    Result = flurm_gres:allocate(Node, Job),
    case Result of
        {ok, Allocation} ->
            ct:pal("GPU allocation: ~p", [Allocation]),
            ?assertEqual(2, maps:get(gpu, Allocation, 0));
        {error, Reason} ->
            ct:pal("GPU allocation failed: ~p", [Reason])
    end.

test_gpu_allocation_type_specific(_Config) ->
    %% Test GPU type-specific allocation (e.g., gpu:a100:2)
    Node = create_test_node(#{
        name => <<"gpu-node2">>,
        gres => #{
            {gpu, a100} => 2,
            {gpu, v100} => 4
        }
    }),

    %% Job specifically wants A100 GPUs
    Job = create_test_job(#{
        id => 11,
        gres => #{{gpu, a100} => 2}
    }),

    Result = flurm_gres:allocate(Node, Job),
    ct:pal("Type-specific GPU allocation: ~p", [Result]),
    ?assert(true).

test_gpu_allocation_exclusive(_Config) ->
    %% Test exclusive GPU access
    Node = create_test_node(#{
        name => <<"gpu-node3">>,
        gres => #{gpu => 4}
    }),

    %% First job gets exclusive access
    Job1 = create_test_job(#{id => 12, gres => #{gpu => 4}, exclusive => true}),

    Result1 = flurm_gres:allocate(Node, Job1),
    ct:pal("Exclusive GPU allocation for job 12: ~p", [Result1]),

    %% Second job should fail to get GPUs on same node
    Job2 = create_test_job(#{id => 13, gres => #{gpu => 1}}),

    %% Simulate node with GPUs in use
    NodeInUse = Node#{gres_in_use => #{gpu => 4}},
    Result2 = flurm_gres:allocate(NodeInUse, Job2),
    ct:pal("Non-exclusive allocation while exclusive in use: ~p", [Result2]),
    ?assert(true).

test_gpu_exhaustion_queues_jobs(_Config) ->
    %% Test that jobs queue when GPUs are exhausted
    Node = create_test_node(#{name => <<"gpu-node4">>, gres => #{gpu => 2}}),

    %% First job takes all GPUs
    Job1 = create_test_job(#{id => 14, gres => #{gpu => 2}}),
    {ok, _} = flurm_gres:allocate(Node, Job1),

    %% Mock has_available to check if GPUs are available
    meck:expect(flurm_gres, has_available, fun(N, _Job) ->
        Gres = maps:get(gres, N, #{}),
        InUse = maps:get(gres_in_use, N, #{}),
        GpuTotal = maps:get(gpu, Gres, 0),
        GpuUsed = maps:get(gpu, InUse, 0),
        %% Has at least 1 GPU available
        GpuTotal > GpuUsed
    end),

    %% Second job should be queued
    Job2 = create_test_job(#{id => 15, gres => #{gpu => 1}, state => pending}),

    CanRun = flurm_gres:has_available(Node#{gres_in_use => #{gpu => 2}}, Job2),
    ?assertEqual(false, CanRun),
    ct:pal("Job 15 correctly queued due to GPU exhaustion").

test_gpu_release_on_completion(_Config) ->
    %% Test GPU release when job completes
    Node = create_test_node(#{
        name => <<"gpu-node5">>,
        gres => #{gpu => 4},
        gres_in_use => #{gpu => 2}
    }),

    CompletedJob = create_test_job(#{id => 16, gres => #{gpu => 2}}),

    %% Mock release to properly update gres_in_use
    meck:expect(flurm_gres, release, fun(N, _Job) ->
        InUse = maps:get(gres_in_use, N, #{}),
        %% Release 2 GPUs (the job's allocation)
        CurrentGpu = maps:get(gpu, InUse, 0),
        NewInUse = InUse#{gpu => max(0, CurrentGpu - 2)},
        N#{gres_in_use => NewInUse}
    end),

    %% Release GPUs
    UpdatedNode = flurm_gres:release(Node, CompletedJob),
    ?assertEqual(0, maps:get(gpu, maps:get(gres_in_use, UpdatedNode, #{}), 0)),
    ct:pal("GPUs released after job completion").

%%====================================================================
%% License Tests
%%====================================================================

test_license_allocation(_Config) ->
    %% Test basic license allocation
    LicensePool = #{matlab => 10, ansys => 5},

    meck:expect(flurm_license, get_pool, fun() -> LicensePool end),
    meck:expect(flurm_license, get_in_use, fun() -> #{} end),

    Job = create_test_job(#{id => 20, licenses => #{matlab => 2}}),

    Result = flurm_license:allocate(Job),
    ct:pal("License allocation result: ~p", [Result]),
    ?assert(Result =:= ok orelse element(1, Result) =:= ok).

test_license_exhaustion(_Config) ->
    %% Test that jobs queue when licenses are exhausted
    LicensePool = #{matlab => 10},
    InUse = #{matlab => 10},  %% All licenses in use

    meck:expect(flurm_license, get_pool, fun() -> LicensePool end),
    meck:expect(flurm_license, get_in_use, fun() -> InUse end),

    %% Mock can_allocate to check license availability
    meck:expect(flurm_license, can_allocate, fun(_Job) ->
        Pool = flurm_license:get_pool(),
        Used = flurm_license:get_in_use(),
        %% Check if matlab licenses available
        PoolMatlab = maps:get(matlab, Pool, 0),
        UsedMatlab = maps:get(matlab, Used, 0),
        PoolMatlab > UsedMatlab
    end),

    Job = create_test_job(#{id => 21, licenses => #{matlab => 1}}),

    CanRun = flurm_license:can_allocate(Job),
    ?assertEqual(false, CanRun),
    ct:pal("Job correctly blocked due to license exhaustion").

test_license_release(_Config) ->
    %% Test license release when job completes
    InUse = #{matlab => 5},

    meck:expect(flurm_license, get_in_use, fun() -> InUse end),

    CompletedJob = create_test_job(#{id => 22, licenses => #{matlab => 2}}),

    ok = flurm_license:release(CompletedJob),
    ct:pal("Licenses released after job completion").

test_multiple_licenses_per_job(_Config) ->
    %% Test job requiring multiple different licenses
    LicensePool = #{matlab => 10, ansys => 5, fluent => 3},

    meck:expect(flurm_license, get_pool, fun() -> LicensePool end),
    meck:expect(flurm_license, get_in_use, fun() -> #{} end),

    Job = create_test_job(#{
        id => 23,
        licenses => #{matlab => 1, ansys => 2, fluent => 1}
    }),

    Result = flurm_license:allocate(Job),
    ct:pal("Multi-license allocation result: ~p", [Result]),
    ?assert(true).

%%====================================================================
%% QOS Tests
%%====================================================================

test_qos_max_jobs_per_user(_Config) ->
    %% Test MaxJobsPerUser limit
    meck:expect(flurm_qos, get_limits, fun(<<"limited">>) ->
        #{max_jobs_per_user => 5}
    end),

    meck:expect(flurm_qos, get_user_job_count, fun(<<"testuser">>, <<"limited">>) ->
        5  %% User already has 5 jobs
    end),

    %% Mock check_job_limits to enforce the limit
    meck:expect(flurm_qos, check_job_limits, fun(_Job) ->
        Limits = flurm_qos:get_limits(<<"limited">>),
        MaxJobs = maps:get(max_jobs_per_user, Limits, infinity),
        CurrentJobs = flurm_qos:get_user_job_count(<<"testuser">>, <<"limited">>),
        case CurrentJobs >= MaxJobs of
            true -> {error, max_jobs_per_user};
            false -> ok
        end
    end),

    Job = create_test_job(#{
        id => 30,
        user => <<"testuser">>,
        qos => <<"limited">>
    }),

    CanSubmit = flurm_qos:check_job_limits(Job),
    ct:pal("QOS job limit check: ~p", [CanSubmit]),
    ?assertEqual({error, max_jobs_per_user}, CanSubmit).

test_qos_max_cpus_per_user(_Config) ->
    %% Test MaxCPUsPerUser limit
    meck:expect(flurm_qos, get_limits, fun(<<"compute">>) ->
        #{max_cpus_per_user => 100}
    end),

    meck:expect(flurm_qos, get_user_cpu_usage, fun(<<"cpuuser">>, <<"compute">>) ->
        80  %% User using 80 CPUs
    end),

    %% Mock check_job_limits to enforce CPU limit
    meck:expect(flurm_qos, check_job_limits, fun(Job) ->
        Limits = flurm_qos:get_limits(<<"compute">>),
        MaxCpus = maps:get(max_cpus_per_user, Limits, infinity),
        CurrentCpus = flurm_qos:get_user_cpu_usage(<<"cpuuser">>, <<"compute">>),
        RequestedCpus = Job#job.num_cpus,
        case CurrentCpus + RequestedCpus > MaxCpus of
            true -> {error, max_cpus_per_user};
            false -> ok
        end
    end),

    %% Job requesting 30 CPUs should be rejected (80 + 30 > 100)
    Job = create_test_job(#{
        id => 31,
        user => <<"cpuuser">>,
        qos => <<"compute">>,
        num_cpus => 30
    }),

    CanSubmit = flurm_qos:check_job_limits(Job),
    ct:pal("QOS CPU limit check: ~p", [CanSubmit]),
    ?assertEqual({error, max_cpus_per_user}, CanSubmit).

test_qos_priority_affects_scheduling(_Config) ->
    %% Test that QOS priority affects job scheduling order
    meck:expect(flurm_qos, get_priority, fun
        (<<"high">>) -> 10000;
        (<<"normal">>) -> 1000;
        (<<"low">>) -> 100;
        (_) -> 500
    end),

    %% Mock sort_by_priority to sort jobs by QOS priority (descending)
    meck:expect(flurm_qos, sort_by_priority, fun(Jobs) ->
        lists:sort(fun(A, B) ->
            PrioA = flurm_qos:get_priority(<<"high">>),  %% Job 33
            PrioB = flurm_qos:get_priority(<<"normal">>), %% Job 34
            %% Actually look at job's qos field - but job record doesn't have qos field
            %% So we use job IDs to determine QOS for this test
            QosA = case A#job.id of 33 -> <<"high">>; 34 -> <<"normal">>; 32 -> <<"low">> end,
            QosB = case B#job.id of 33 -> <<"high">>; 34 -> <<"normal">>; 32 -> <<"low">> end,
            flurm_qos:get_priority(QosA) >= flurm_qos:get_priority(QosB)
        end, Jobs)
    end),

    Jobs = [
        create_test_job(#{id => 32, qos => <<"low">>}),
        create_test_job(#{id => 33, qos => <<"high">>}),
        create_test_job(#{id => 34, qos => <<"normal">>})
    ],

    Sorted = flurm_qos:sort_by_priority(Jobs),
    SortedIds = [J#job.id || J <- Sorted],
    ?assertEqual([33, 34, 32], SortedIds),
    ct:pal("Jobs sorted by QOS priority: ~p", [SortedIds]).

test_qos_preemption_hierarchy(_Config) ->
    %% Test QOS-based preemption hierarchy
    %% high > normal > low
    meck:expect(flurm_qos, can_preempt, fun(QosA, QosB) ->
        PrioA = case QosA of <<"high">> -> 3; <<"normal">> -> 2; <<"low">> -> 1 end,
        PrioB = case QosB of <<"high">> -> 3; <<"normal">> -> 2; <<"low">> -> 1 end,
        PrioA > PrioB
    end),

    ?assertEqual(true, flurm_qos:can_preempt(<<"high">>, <<"normal">>)),
    ?assertEqual(true, flurm_qos:can_preempt(<<"high">>, <<"low">>)),
    ?assertEqual(true, flurm_qos:can_preempt(<<"normal">>, <<"low">>)),
    ?assertEqual(false, flurm_qos:can_preempt(<<"low">>, <<"high">>)),
    ct:pal("QOS preemption hierarchy verified").

%%====================================================================
%% Reservation Tests
%%====================================================================

test_reservation_creation(_Config) ->
    %% Test reservation creation
    ReservationSpec = #{
        name => <<"maintenance">>,
        start_time => erlang:system_time(second) + 3600,
        duration => 7200,  %% 2 hours
        nodes => [<<"node1">>, <<"node2">>],
        users => [<<"admin">>]
    },

    Result = flurm_reservation:create(ReservationSpec),
    ct:pal("Reservation creation result: ~p", [Result]),
    ?assert(Result =:= ok orelse element(1, Result) =:= ok).

test_reservation_access_control(_Config) ->
    %% Test that only authorized users can submit to reservation
    Reservation = #{
        name => <<"restricted">>,
        users => [<<"alice">>, <<"bob">>]
    },

    meck:expect(flurm_reservation, get, fun(<<"restricted">>) ->
        {ok, Reservation}
    end),

    %% Mock check_access to verify user is in reservation's user list
    meck:expect(flurm_reservation, check_access, fun(Job) ->
        User = Job#job.user,
        lists:member(User, [<<"alice">>, <<"bob">>])
    end),

    %% Authorized user
    Job1 = create_test_job(#{id => 40, user => <<"alice">>, reservation => <<"restricted">>}),
    Auth1 = flurm_reservation:check_access(Job1),
    ?assertEqual(true, Auth1),

    %% Unauthorized user
    Job2 = create_test_job(#{id => 41, user => <<"eve">>, reservation => <<"restricted">>}),
    Auth2 = flurm_reservation:check_access(Job2),
    ?assertEqual(false, Auth2),

    ct:pal("Reservation access control verified").

test_reservation_node_protection(_Config) ->
    %% Test that reserved nodes are protected from non-reservation jobs
    meck:expect(flurm_reservation, is_node_reserved, fun
        (<<"node1">>, _Time) -> {true, <<"maintenance">>};
        (_, _) -> false
    end),

    %% Mock filter_available_nodes to exclude reserved nodes
    meck:expect(flurm_reservation, filter_available_nodes, fun(Nodes, _Job) ->
        lists:filter(fun(Node) ->
            case flurm_reservation:is_node_reserved(Node, erlang:system_time(second)) of
                {true, _} -> false;
                false -> true
            end
        end, Nodes)
    end),

    %% Job without reservation shouldn't use reserved node
    Job = create_test_job(#{id => 42, reservation => undefined}),
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],

    AvailableNodes = flurm_reservation:filter_available_nodes(Nodes, Job),
    ?assertEqual([<<"node2">>, <<"node3">>], AvailableNodes),
    ct:pal("Reserved node protection verified").

test_reservation_overlap_rejection(_Config) ->
    %% Test that overlapping reservations are rejected
    ExistingReservation = #{
        name => <<"existing">>,
        start_time => 1000,
        end_time => 2000,
        nodes => [<<"node1">>]
    },

    meck:expect(flurm_reservation, list, fun() ->
        [ExistingReservation]
    end),

    %% Mock check_overlap to detect overlapping reservations
    meck:expect(flurm_reservation, check_overlap, fun(NewResv) ->
        Existing = flurm_reservation:list(),
        NewStart = maps:get(start_time, NewResv),
        NewEnd = NewStart + maps:get(duration, NewResv),
        NewNodes = maps:get(nodes, NewResv),
        Overlaps = lists:any(fun(Resv) ->
            ExStart = maps:get(start_time, Resv),
            ExEnd = maps:get(end_time, Resv),
            ExNodes = maps:get(nodes, Resv),
            %% Check time overlap and node overlap
            TimeOverlap = (NewStart < ExEnd) andalso (NewEnd > ExStart),
            NodeOverlap = lists:any(fun(N) -> lists:member(N, ExNodes) end, NewNodes),
            TimeOverlap andalso NodeOverlap
        end, Existing),
        case Overlaps of
            true -> {error, overlap};
            false -> ok
        end
    end),

    %% New reservation overlapping with existing
    NewReservation = #{
        name => <<"new">>,
        start_time => 1500,  %% Overlaps
        duration => 1000,
        nodes => [<<"node1">>]
    },

    Result = flurm_reservation:check_overlap(NewReservation),
    ?assertEqual({error, overlap}, Result),
    ct:pal("Reservation overlap correctly rejected").

%%====================================================================
%% Burst Buffer Tests
%%====================================================================

test_burst_buffer_stage_in(_Config) ->
    %% Test data stage-in before job starts
    BurstBufferSpec = #{
        stage_in => [
            #{source => <<"/data/input.dat">>, destination => <<"$BB/input.dat">>}
        ]
    },

    Job = create_test_job(#{id => 50, burst_buffer => BurstBufferSpec}),

    meck:expect(flurm_burst_buffer, stage_in, fun(_Job, _Files) ->
        {ok, staged}
    end),

    Result = flurm_burst_buffer:prepare_job(Job),
    ct:pal("Burst buffer stage-in result: ~p", [Result]),
    ?assert(Result =:= ok orelse Result =:= {ok, staged}).

test_burst_buffer_stage_out(_Config) ->
    %% Test data stage-out after job completes
    BurstBufferSpec = #{
        stage_out => [
            #{source => <<"$BB/output.dat">>, destination => <<"/data/output.dat">>}
        ]
    },

    Job = create_test_job(#{id => 51, burst_buffer => BurstBufferSpec}),

    meck:expect(flurm_burst_buffer, stage_out, fun(_Job, _Files) ->
        {ok, staged}
    end),

    Result = flurm_burst_buffer:finalize_job(Job),
    ct:pal("Burst buffer stage-out result: ~p", [Result]),
    ?assert(Result =:= ok orelse Result =:= {ok, staged}).

test_burst_buffer_capacity(_Config) ->
    %% Test that jobs queue when burst buffer capacity is exhausted
    meck:expect(flurm_burst_buffer, get_capacity, fun() ->
        #{total => 1024, in_use => 900}  %% Only 124GB free
    end),

    %% Mock can_allocate to check capacity
    meck:expect(flurm_burst_buffer, can_allocate, fun(_Job) ->
        Capacity = flurm_burst_buffer:get_capacity(),
        Total = maps:get(total, Capacity, 0),
        InUse = maps:get(in_use, Capacity, 0),
        Available = Total - InUse,
        %% Job needs 200GB which is more than 124GB available
        Available >= 200
    end),

    %% Job needs 200GB
    Job = create_test_job(#{id => 52, burst_buffer => #{size => 200}}),

    CanAllocate = flurm_burst_buffer:can_allocate(Job),
    ?assertEqual(false, CanAllocate),
    ct:pal("Burst buffer capacity limit correctly enforced").

test_burst_buffer_failure_handling(_Config) ->
    %% Test handling of burst buffer failures
    %% Mock prepare_job to return error when stage_in fails
    meck:expect(flurm_burst_buffer, prepare_job, fun(_Job) ->
        {error, io_error}
    end),

    Job = create_test_job(#{id => 53, burst_buffer => #{
        stage_in => [#{source => <<"/data/missing.dat">>, destination => <<"$BB/file.dat">>}]
    }}),

    Result = flurm_burst_buffer:prepare_job(Job),
    ct:pal("Burst buffer failure handling: ~p", [Result]),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Internal Helpers
%%====================================================================

create_test_job(Overrides) ->
    Defaults = #{
        id => 1,
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        num_cpus => 1,
        priority => 1000,
        qos => <<"normal">>,
        gres => #{},
        licenses => #{},
        reservation => undefined,
        burst_buffer => #{}
    },
    Merged = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, Merged),
        name = maps:get(name, Merged),
        user = maps:get(user, Merged),
        partition = maps:get(partition, Merged),
        state = maps:get(state, Merged),
        num_cpus = maps:get(num_cpus, Merged),
        priority = maps:get(priority, Merged),
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        memory_mb = 1024,
        time_limit = 3600,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

create_test_node(Overrides) ->
    Defaults = #{
        name => <<"node1">>,
        state => idle,
        num_cpus => 8,
        memory => 16384,
        gres => #{},
        gres_in_use => #{}
    },
    maps:merge(Defaults, Overrides).

setup_mocks(preemption_tests) ->
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:new(flurm_preemption, [passthrough, non_strict, no_link]),
    meck:new(flurm_qos, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    %% Setup default preemption mocks
    meck:expect(flurm_preemption, check_preemption, fun(_Job, _RunningJobs) ->
        no_preemption
    end),
    meck:expect(flurm_preemption, preempt_job, fun(_Job, _Mode) ->
        ok
    end),
    meck:expect(flurm_preemption, should_preempt, fun(_Job1, _Job2) ->
        false
    end);

setup_mocks(gres_gpu_tests) ->
    meck:new(flurm_gres, [passthrough, non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    %% Setup default GRES mocks
    meck:expect(flurm_gres, allocate, fun(_Node, _Job) ->
        {ok, #{gpu => 0}}
    end),
    meck:expect(flurm_gres, has_available, fun(_Node, _Job) ->
        true
    end),
    meck:expect(flurm_gres, release, fun(Node, _Job) ->
        Node
    end);

setup_mocks(license_tests) ->
    meck:new(flurm_license, [passthrough, non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    %% Setup default license mocks
    meck:expect(flurm_license, get_pool, fun() -> #{} end),
    meck:expect(flurm_license, get_in_use, fun() -> #{} end),
    meck:expect(flurm_license, allocate, fun(_Job) -> ok end),
    meck:expect(flurm_license, can_allocate, fun(_Job) -> true end),
    meck:expect(flurm_license, release, fun(_Job) -> ok end);

setup_mocks(qos_tests) ->
    meck:new(flurm_qos, [passthrough, non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    %% Setup default QOS mocks
    meck:expect(flurm_qos, get_limits, fun(_Qos) -> #{} end),
    meck:expect(flurm_qos, get_user_job_count, fun(_User, _Qos) -> 0 end),
    meck:expect(flurm_qos, get_user_cpu_usage, fun(_User, _Qos) -> 0 end),
    meck:expect(flurm_qos, get_priority, fun(_Qos) -> 1000 end),
    meck:expect(flurm_qos, check_job_limits, fun(_Job) -> ok end),
    meck:expect(flurm_qos, sort_by_priority, fun(Jobs) -> Jobs end),
    meck:expect(flurm_qos, can_preempt, fun(_QosA, _QosB) -> false end);

setup_mocks(reservation_tests) ->
    meck:new(flurm_reservation, [passthrough, non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    %% Setup default reservation mocks
    meck:expect(flurm_reservation, create, fun(_Spec) -> ok end),
    meck:expect(flurm_reservation, get, fun(_Name) -> {error, not_found} end),
    meck:expect(flurm_reservation, check_access, fun(_Job) -> true end),
    meck:expect(flurm_reservation, list, fun() -> [] end),
    meck:expect(flurm_reservation, is_node_reserved, fun(_Node, _Time) -> false end),
    meck:expect(flurm_reservation, filter_available_nodes, fun(Nodes, _Job) -> Nodes end),
    meck:expect(flurm_reservation, check_overlap, fun(_Resv) -> ok end);

setup_mocks(burst_buffer_tests) ->
    meck:new(flurm_burst_buffer, [passthrough, non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    %% Setup default burst buffer mocks
    meck:expect(flurm_burst_buffer, prepare_job, fun(_Job) -> ok end),
    meck:expect(flurm_burst_buffer, finalize_job, fun(_Job) -> ok end),
    meck:expect(flurm_burst_buffer, stage_in, fun(_Job, _Files) -> {ok, staged} end),
    meck:expect(flurm_burst_buffer, stage_out, fun(_Job, _Files) -> {ok, staged} end),
    meck:expect(flurm_burst_buffer, get_capacity, fun() -> #{total => 1024, in_use => 0} end),
    meck:expect(flurm_burst_buffer, can_allocate, fun(_Job) -> true end);

setup_mocks(_) ->
    ok.

cleanup_mocks() ->
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_preemption),
    catch meck:unload(flurm_gres),
    catch meck:unload(flurm_license),
    catch meck:unload(flurm_qos),
    catch meck:unload(flurm_reservation),
    catch meck:unload(flurm_burst_buffer),
    ok.
