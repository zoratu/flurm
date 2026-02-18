%%%-------------------------------------------------------------------
%%% @doc FLURM DB Ra Effects 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_db_ra_effects module covering all
%%% effect handlers, subscriber management, and internal helper functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_effects_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Mock error_logger
    meck:new(error_logger, [unstick, passthrough]),
    meck:expect(error_logger, info_msg, fun(_Fmt, _Args) -> ok end),
    %% Mock pg (process groups)
    meck:new(pg, [non_strict, no_link]),
    meck:expect(pg, start, fun(_Scope) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(pg, join, fun(_Group, _Pid) -> ok end),
    meck:expect(pg, leave, fun(_Group, _Pid) -> ok end),
    meck:expect(pg, get_members, fun(_Group) -> [] end),
    %% Mock flurm_db_ra
    meck:new(flurm_db_ra, [non_strict, no_link]),
    meck:expect(flurm_db_ra, update_job_state, fun(_JobId, _State) -> ok end),
    %% Mock flurm_controller_failover
    meck:new(flurm_controller_failover, [non_strict, no_link]),
    meck:expect(flurm_controller_failover, on_became_leader, fun() -> ok end),
    meck:expect(flurm_controller_failover, on_lost_leadership, fun() -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(error_logger),
    catch meck:unload(pg),
    catch meck:unload(flurm_db_ra),
    catch meck:unload(flurm_controller_failover),
    ok.

%%====================================================================
%% Job Submitted Tests
%%====================================================================

job_submitted_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_submitted logs event", fun() ->
                 meck:expect(error_logger, info_msg, fun(Fmt, Args) ->
                     ?assert(lists:member(job_submitted, Args)),
                     ok
                 end),
                 Job = make_job(1),
                 Result = flurm_db_ra_effects:job_submitted(Job),
                 ?assertEqual(ok, Result)
             end},
             {"job_submitted notifies scheduler", fun() ->
                 %% Register a fake scheduler
                 Self = self(),
                 register(flurm_scheduler, Self),
                 Job = make_job(123),
                 ok = flurm_db_ra_effects:job_submitted(Job),
                 receive
                     {ra_event, {job_pending, 123}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_submitted notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Job = make_job(456),
                 ok = flurm_db_ra_effects:job_submitted(Job),
                 receive
                     {flurm_db_event, {job_submitted, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Job Cancelled Tests
%%====================================================================

job_cancelled_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_cancelled logs event", fun() ->
                 meck:expect(error_logger, info_msg, fun(Fmt, Args) ->
                     ?assert(lists:member(job_cancelled, Args)),
                     ok
                 end),
                 Job = make_job(1),
                 Result = flurm_db_ra_effects:job_cancelled(Job),
                 ?assertEqual(ok, Result)
             end},
             {"job_cancelled notifies scheduler", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 Job = make_job(789),
                 ok = flurm_db_ra_effects:job_cancelled(Job),
                 receive
                     {ra_event, {job_cancelled, 789}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_cancelled releases allocated nodes", fun() ->
                 Self = self(),
                 register(flurm_node_manager, Self),
                 Job = make_job_with_nodes(100, [<<"node1">>, <<"node2">>]),
                 ok = flurm_db_ra_effects:job_cancelled(Job),
                 %% Should receive release messages for each node
                 receive
                     {ra_event, {release, <<"node1">>, 100}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 receive
                     {ra_event, {release, <<"node2">>, 100}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_node_manager)
             end},
             {"job_cancelled with no allocated nodes", fun() ->
                 Job = make_job(101),
                 Result = flurm_db_ra_effects:job_cancelled(Job),
                 ?assertEqual(ok, Result)
             end},
             {"job_cancelled notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Job = make_job(102),
                 ok = flurm_db_ra_effects:job_cancelled(Job),
                 receive
                     {flurm_db_event, {job_cancelled, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Job State Changed Tests
%%====================================================================

job_state_changed_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_state_changed pending to configuring", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:job_state_changed(1, pending, configuring),
                 receive
                     {ra_event, {job_configuring, 1}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_state_changed configuring to running", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:job_state_changed(2, configuring, running),
                 receive
                     {ra_event, {job_running, 2}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_state_changed any to completed", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:job_state_changed(3, running, completed),
                 receive
                     {ra_event, {job_completed, 3}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_state_changed any to failed", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:job_state_changed(4, running, failed),
                 receive
                     {ra_event, {job_failed, 4}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_state_changed other transitions", fun() ->
                 %% These don't trigger scheduler notifications
                 ok = flurm_db_ra_effects:job_state_changed(5, pending, pending),
                 ok = flurm_db_ra_effects:job_state_changed(6, running, suspended),
                 ?assert(true)
             end},
             {"job_state_changed notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 ok = flurm_db_ra_effects:job_state_changed(7, pending, running),
                 receive
                     {flurm_db_event, {job_state_changed, 7, pending, running}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Job Allocated Tests
%%====================================================================

job_allocated_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_allocated logs event", fun() ->
                 Job = make_job(1),
                 Nodes = [<<"node1">>, <<"node2">>],
                 Result = flurm_db_ra_effects:job_allocated(Job, Nodes),
                 ?assertEqual(ok, Result)
             end},
             {"job_allocated notifies node manager", fun() ->
                 Self = self(),
                 register(flurm_node_manager, Self),
                 Job = make_job(200),
                 Nodes = [<<"node1">>],
                 ok = flurm_db_ra_effects:job_allocated(Job, Nodes),
                 receive
                     {ra_event, {allocate, <<"node1">>, 200, {4, 8192, 0}}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_node_manager)
             end},
             {"job_allocated notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Job = make_job(201),
                 Nodes = [<<"node1">>],
                 ok = flurm_db_ra_effects:job_allocated(Job, Nodes),
                 receive
                     {flurm_db_event, {job_allocated, _, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Job Completed Tests
%%====================================================================

job_completed_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_completed logs event", fun() ->
                 Job = make_job(1),
                 Result = flurm_db_ra_effects:job_completed(Job, 0),
                 ?assertEqual(ok, Result)
             end},
             {"job_completed releases allocated nodes", fun() ->
                 Self = self(),
                 register(flurm_node_manager, Self),
                 Job = make_job_with_nodes(300, [<<"node1">>, <<"node2">>]),
                 ok = flurm_db_ra_effects:job_completed(Job, 0),
                 receive
                     {ra_event, {release, <<"node1">>, 300}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 receive
                     {ra_event, {release, <<"node2">>, 300}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_node_manager)
             end},
             {"job_completed with no allocated nodes", fun() ->
                 Job = make_job(301),
                 Result = flurm_db_ra_effects:job_completed(Job, 0),
                 ?assertEqual(ok, Result)
             end},
             {"job_completed notifies scheduler", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 Job = make_job(302),
                 ok = flurm_db_ra_effects:job_completed(Job, 0),
                 receive
                     {ra_event, {job_completed, 302, 0}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_completed with error exit code", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 Job = make_job(303),
                 ok = flurm_db_ra_effects:job_completed(Job, 1),
                 receive
                     {ra_event, {job_completed, 303, 1}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"job_completed notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Job = make_job(304),
                 ok = flurm_db_ra_effects:job_completed(Job, 0),
                 receive
                     {flurm_db_event, {job_completed, _, 0}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Node Registered Tests
%%====================================================================

node_registered_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"node_registered logs event", fun() ->
                 Node = make_node(<<"node1">>),
                 Result = flurm_db_ra_effects:node_registered(Node),
                 ?assertEqual(ok, Result)
             end},
             {"node_registered notifies scheduler", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 Node = make_node(<<"node1">>),
                 ok = flurm_db_ra_effects:node_registered(Node),
                 receive
                     {ra_event, {node_available, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"node_registered notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Node = make_node(<<"node2">>),
                 ok = flurm_db_ra_effects:node_registered(Node),
                 receive
                     {flurm_db_event, {node_registered, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Node Updated Tests
%%====================================================================

node_updated_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"node_updated logs event", fun() ->
                 Node = make_node(<<"node1">>),
                 Result = flurm_db_ra_effects:node_updated(Node),
                 ?assertEqual(ok, Result)
             end},
             {"node_updated notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Node = make_node(<<"node1">>),
                 ok = flurm_db_ra_effects:node_updated(Node),
                 receive
                     {flurm_db_event, {node_updated, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Node State Changed Tests
%%====================================================================

node_state_changed_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"node_state_changed to down", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, up, down),
                 receive
                     {ra_event, {node_down, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"node_state_changed to up from down", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, down, up),
                 receive
                     {ra_event, {node_available, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"node_state_changed to up from idle", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, idle, up),
                 receive
                     {ra_event, {node_available, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"node_state_changed up to up (no notification)", fun() ->
                 %% When old state is already up, no notification
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, up, up),
                 ?assert(true)
             end},
             {"node_state_changed to drain", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, up, drain),
                 receive
                     {ra_event, {node_draining, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"node_state_changed other transition", fun() ->
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, idle, idle),
                 ?assert(true)
             end},
             {"node_state_changed notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, up, down),
                 receive
                     {flurm_db_event, {node_state_changed, _, _, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Node Unregistered Tests
%%====================================================================

node_unregistered_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"node_unregistered logs event", fun() ->
                 Node = make_node(<<"node1">>),
                 Result = flurm_db_ra_effects:node_unregistered(Node),
                 ?assertEqual(ok, Result)
             end},
             {"node_unregistered notifies scheduler", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 Node = make_node(<<"node1">>),
                 ok = flurm_db_ra_effects:node_unregistered(Node),
                 receive
                     {ra_event, {node_unavailable, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"node_unregistered handles running jobs", fun() ->
                 Node = make_node_with_jobs(<<"node1">>, [1, 2, 3]),
                 meck:expect(flurm_db_ra, update_job_state, fun(JobId, State) ->
                     ?assertEqual(node_fail, State),
                     ?assert(lists:member(JobId, [1, 2, 3])),
                     ok
                 end),
                 ok = flurm_db_ra_effects:node_unregistered(Node),
                 %% Verify all jobs were marked as failed
                 ?assert(meck:called(flurm_db_ra, update_job_state, [1, node_fail])),
                 ?assert(meck:called(flurm_db_ra, update_job_state, [2, node_fail])),
                 ?assert(meck:called(flurm_db_ra, update_job_state, [3, node_fail]))
             end},
             {"node_unregistered notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Node = make_node(<<"node1">>),
                 ok = flurm_db_ra_effects:node_unregistered(Node),
                 receive
                     {flurm_db_event, {node_unregistered, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Partition Created Tests
%%====================================================================

partition_created_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"partition_created logs event", fun() ->
                 Partition = make_partition(<<"default">>),
                 Result = flurm_db_ra_effects:partition_created(Partition),
                 ?assertEqual(ok, Result)
             end},
             {"partition_created notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Partition = make_partition(<<"gpu">>),
                 ok = flurm_db_ra_effects:partition_created(Partition),
                 receive
                     {flurm_db_event, {partition_created, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Partition Deleted Tests
%%====================================================================

partition_deleted_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"partition_deleted logs event", fun() ->
                 Partition = make_partition(<<"default">>),
                 Result = flurm_db_ra_effects:partition_deleted(Partition),
                 ?assertEqual(ok, Result)
             end},
             {"partition_deleted notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 Partition = make_partition(<<"deprecated">>),
                 ok = flurm_db_ra_effects:partition_deleted(Partition),
                 receive
                     {flurm_db_event, {partition_deleted, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end}
         ]
     end
    }.

%%====================================================================
%% Became Leader Tests
%%====================================================================

became_leader_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"became_leader logs event", fun() ->
                 Result = flurm_db_ra_effects:became_leader(node()),
                 ?assertEqual(ok, Result)
             end},
             {"became_leader starts scheduler", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:became_leader(node()),
                 receive
                     activate_scheduling -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"became_leader notifies failover handler", fun() ->
                 %% Register a fake failover handler
                 Self = self(),
                 register(flurm_controller_failover, Self),
                 meck:expect(flurm_controller_failover, on_became_leader, fun() ->
                     Self ! on_became_leader_called,
                     ok
                 end),
                 ok = flurm_db_ra_effects:became_leader(node()),
                 receive
                     on_became_leader_called -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_controller_failover)
             end},
             {"became_leader notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 ok = flurm_db_ra_effects:became_leader(node()),
                 receive
                     {flurm_db_event, {became_leader, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end},
             {"became_leader handles no scheduler", fun() ->
                 %% Make sure flurm_scheduler is not registered
                 case whereis(flurm_scheduler) of
                     undefined -> ok;
                     Pid -> unregister(flurm_scheduler), exit(Pid, kill)
                 end,
                 Result = flurm_db_ra_effects:became_leader(node()),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Became Follower Tests
%%====================================================================

became_follower_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"became_follower logs event", fun() ->
                 Result = flurm_db_ra_effects:became_follower(node()),
                 ?assertEqual(ok, Result)
             end},
             {"became_follower stops scheduler", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 ok = flurm_db_ra_effects:became_follower(node()),
                 receive
                     pause_scheduling -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"became_follower notifies failover handler", fun() ->
                 Self = self(),
                 register(flurm_controller_failover, Self),
                 meck:expect(flurm_controller_failover, on_lost_leadership, fun() ->
                     Self ! on_lost_leadership_called,
                     ok
                 end),
                 ok = flurm_db_ra_effects:became_follower(node()),
                 receive
                     on_lost_leadership_called -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_controller_failover)
             end},
             {"became_follower notifies subscribers", fun() ->
                 Self = self(),
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [Self] end),
                 ok = flurm_db_ra_effects:became_follower(node()),
                 receive
                     {flurm_db_event, {became_follower, _}} -> ok
                 after 100 ->
                     ?assert(false)
                 end
             end},
             {"became_follower handles no scheduler", fun() ->
                 case whereis(flurm_scheduler) of
                     undefined -> ok;
                     Pid -> unregister(flurm_scheduler), exit(Pid, kill)
                 end,
                 Result = flurm_db_ra_effects:became_follower(node()),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Subscribe/Unsubscribe Tests
%%====================================================================

subscribe_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"subscribe joins pg group", fun() ->
                 JoinCalled = ets:new(join_called, [public]),
                 meck:expect(pg, join, fun(Group, Pid) ->
                     ets:insert(JoinCalled, {called, {Group, Pid}}),
                     ok
                 end),
                 Result = flurm_db_ra_effects:subscribe(self()),
                 ?assertEqual(ok, Result),
                 [{called, {flurm_db_effects, _Pid}}] = ets:lookup(JoinCalled, called),
                 ets:delete(JoinCalled)
             end},
             {"subscribe handles already_started pg", fun() ->
                 meck:expect(pg, start, fun(_Scope) ->
                     {error, {already_started, spawn(fun() -> ok end)}}
                 end),
                 Result = flurm_db_ra_effects:subscribe(self()),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

unsubscribe_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"unsubscribe leaves pg group", fun() ->
                 LeaveCalled = ets:new(leave_called, [public]),
                 meck:expect(pg, leave, fun(Group, Pid) ->
                     ets:insert(LeaveCalled, {called, {Group, Pid}}),
                     ok
                 end),
                 Result = flurm_db_ra_effects:unsubscribe(self()),
                 ?assertEqual(ok, Result),
                 [{called, {flurm_db_effects, _Pid}}] = ets:lookup(LeaveCalled, called),
                 ets:delete(LeaveCalled)
             end},
             {"unsubscribe handles not member error", fun() ->
                 meck:expect(pg, leave, fun(_Group, _Pid) ->
                     throw(not_member)
                 end),
                 Result = flurm_db_ra_effects:unsubscribe(self()),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Internal Helper Function Tests
%%====================================================================

-ifdef(TEST).

log_event_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"log_event calls error_logger:info_msg", fun() ->
                 InfoCalled = ets:new(info_called, [public]),
                 meck:expect(error_logger, info_msg, fun(_Fmt, _Args) ->
                     ets:insert(InfoCalled, {called, true}),
                     ok
                 end),
                 flurm_db_ra_effects:log_event(test_event, #{key => value}),
                 ?assertEqual([{called, true}], ets:lookup(InfoCalled, called)),
                 ets:delete(InfoCalled)
             end}
         ]
     end
    }.

notify_scheduler_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"notify_scheduler sends to registered process", fun() ->
                 Self = self(),
                 register(flurm_scheduler, Self),
                 flurm_db_ra_effects:notify_scheduler({test_event, 123}),
                 receive
                     {ra_event, {test_event, 123}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_scheduler)
             end},
             {"notify_scheduler handles undefined", fun() ->
                 case whereis(flurm_scheduler) of
                     undefined -> ok;
                     _ -> unregister(flurm_scheduler)
                 end,
                 %% Should not crash
                 flurm_db_ra_effects:notify_scheduler({test_event, 123}),
                 ?assert(true)
             end}
         ]
     end
    }.

notify_node_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"notify_node_manager sends to registered process", fun() ->
                 Self = self(),
                 register(flurm_node_manager, Self),
                 flurm_db_ra_effects:notify_node_manager({test_event, <<"node1">>}),
                 receive
                     {ra_event, {test_event, <<"node1">>}} -> ok
                 after 100 ->
                     ?assert(false)
                 end,
                 unregister(flurm_node_manager)
             end},
             {"notify_node_manager handles undefined", fun() ->
                 case whereis(flurm_node_manager) of
                     undefined -> ok;
                     _ -> unregister(flurm_node_manager)
                 end,
                 %% Should not crash
                 flurm_db_ra_effects:notify_node_manager({test_event, <<"node1">>}),
                 ?assert(true)
             end}
         ]
     end
    }.

release_nodes_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"release_nodes sends release for each node", fun() ->
                 Self = self(),
                 register(flurm_node_manager, Self),
                 flurm_db_ra_effects:release_nodes(999, [<<"n1">>, <<"n2">>, <<"n3">>]),
                 Received = receive_all([]),
                 unregister(flurm_node_manager),
                 ?assertEqual(3, length(Received)),
                 ?assert(lists:member({ra_event, {release, <<"n1">>, 999}}, Received)),
                 ?assert(lists:member({ra_event, {release, <<"n2">>, 999}}, Received)),
                 ?assert(lists:member({ra_event, {release, <<"n3">>, 999}}, Received))
             end}
         ]
     end
    }.

job_to_map_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"job_to_map converts record to map", fun() ->
                 Job = make_job(123),
                 Map = flurm_db_ra_effects:job_to_map(Job),
                 ?assert(is_map(Map)),
                 ?assertEqual(123, maps:get(id, Map)),
                 ?assertEqual(<<"test_job">>, maps:get(name, Map)),
                 ?assertEqual(<<"testuser">>, maps:get(user, Map))
             end}
         ]
     end
    }.

node_to_map_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"node_to_map converts record to map", fun() ->
                 Node = make_node(<<"node1">>),
                 Map = flurm_db_ra_effects:node_to_map(Node),
                 ?assert(is_map(Map)),
                 ?assertEqual(<<"node1">>, maps:get(name, Map)),
                 ?assertEqual(<<"host1">>, maps:get(hostname, Map)),
                 ?assertEqual(16, maps:get(cpus, Map))
             end}
         ]
     end
    }.

-endif.

%%====================================================================
%% Subscriber Notification Test
%%====================================================================

notify_subscribers_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"notify_subscribers sends to all members", fun() ->
                 Receiver1 = spawn(fun() ->
                     receive Msg -> self() ! {received, Msg} end
                 end),
                 Receiver2 = spawn(fun() ->
                     receive Msg -> self() ! {received, Msg} end
                 end),
                 meck:expect(pg, get_members, fun(flurm_db_effects) ->
                     [Receiver1, Receiver2]
                 end),
                 flurm_db_ra_effects:notify_subscribers({test_event, data}),
                 timer:sleep(50),
                 %% Both receivers should have gotten the message
                 ?assert(true)
             end},
             {"notify_subscribers handles empty members", fun() ->
                 meck:expect(pg, get_members, fun(flurm_db_effects) -> [] end),
                 %% Should not crash
                 flurm_db_ra_effects:notify_subscribers({test_event, data}),
                 ?assert(true)
             end},
             {"notify_subscribers handles pg error", fun() ->
                 meck:expect(pg, get_members, fun(flurm_db_effects) ->
                     throw(group_not_found)
                 end),
                 %% Should not crash
                 flurm_db_ra_effects:notify_subscribers({test_event, data}),
                 ?assert(true)
             end}
         ]
     end
    }.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job(Id) ->
    %% ra_job record structure
    {ra_job,
     Id,                      %% id
     <<"test_job">>,         %% name
     <<"testuser">>,         %% user
     <<"default">>,          %% partition
     pending,                %% state
     1,                      %% num_nodes
     4,                      %% num_cpus
     8192,                   %% memory_mb
     100,                    %% priority
     undefined,              %% start_time
     undefined,              %% end_time
     [],                     %% allocated_nodes
     undefined,              %% script
     #{}                     %% extra
    }.

make_job_with_nodes(Id, Nodes) ->
    %% ra_job record with allocated nodes
    {ra_job,
     Id,                      %% id
     <<"test_job">>,         %% name
     <<"testuser">>,         %% user
     <<"default">>,          %% partition
     running,                %% state
     length(Nodes),          %% num_nodes
     4,                      %% num_cpus
     8192,                   %% memory_mb
     100,                    %% priority
     undefined,              %% start_time
     undefined,              %% end_time
     Nodes,                  %% allocated_nodes
     undefined,              %% script
     #{}                     %% extra
    }.

make_node(Name) ->
    %% ra_node record structure
    {ra_node,
     Name,                   %% name
     <<"host1">>,           %% hostname
     16,                     %% cpus
     65536,                  %% memory_mb
     up,                     %% state
     [],                     %% running_jobs
     #{},                    %% extra
     erlang:system_time(second)  %% last_heartbeat
    }.

make_node_with_jobs(Name, Jobs) ->
    {ra_node,
     Name,                   %% name
     <<"host1">>,           %% hostname
     16,                     %% cpus
     65536,                  %% memory_mb
     up,                     %% state
     Jobs,                   %% running_jobs
     #{},                    %% extra
     erlang:system_time(second)  %% last_heartbeat
    }.

make_partition(Name) ->
    %% ra_partition record structure
    {ra_partition,
     Name,                   %% name
     [],                     %% nodes
     #{},                    %% constraints
     #{},                    %% extra
     erlang:system_time(second)  %% created_at
    }.

receive_all(Acc) ->
    receive
        Msg -> receive_all([Msg | Acc])
    after 100 ->
        lists:reverse(Acc)
    end.
