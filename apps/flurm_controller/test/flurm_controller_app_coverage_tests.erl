%%%-------------------------------------------------------------------
%%% @doc FLURM Controller App Coverage Tests
%%%
%%% Comprehensive coverage tests for flurm_controller_app module.
%%% Tests all exported -ifdef(TEST) helper functions for maximum coverage.
%%%
%%% These tests focus on:
%%% - get_config - configuration retrieval
%%% - count_jobs_by_state - job counting logic
%%% - count_nodes_by_state - node counting logic
%%% - status/0, config/0, cluster_status/0 - API functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_app_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% get_config Tests
%%====================================================================

get_config_test_() ->
    {setup,
     fun() ->
         %% Save current values
         application:ensure_all_started(sasl),
         ok
     end,
     fun(_) ->
         ok
     end,
     [
        {"returns default when not set", fun() ->
            application:unset_env(flurm_controller, test_key_xyz123),
            ?assertEqual(default_value,
                        flurm_controller_app:get_config(test_key_xyz123, default_value))
        end},
        {"returns set value", fun() ->
            application:set_env(flurm_controller, test_key_123, custom_value),
            ?assertEqual(custom_value,
                        flurm_controller_app:get_config(test_key_123, default_value)),
            application:unset_env(flurm_controller, test_key_123)
        end},
        {"listen_port default", fun() ->
            application:unset_env(flurm_controller, listen_port),
            ?assertEqual(6817, flurm_controller_app:get_config(listen_port, 6817))
        end},
        {"num_acceptors default", fun() ->
            application:unset_env(flurm_controller, num_acceptors),
            ?assertEqual(10, flurm_controller_app:get_config(num_acceptors, 10))
        end},
        {"max_connections default", fun() ->
            application:unset_env(flurm_controller, max_connections),
            ?assertEqual(1000, flurm_controller_app:get_config(max_connections, 1000))
        end},
        {"cluster_name default", fun() ->
            application:unset_env(flurm_controller, cluster_name),
            ?assertEqual(flurm, flurm_controller_app:get_config(cluster_name, flurm))
        end},
        {"cluster_nodes default", fun() ->
            application:unset_env(flurm_controller, cluster_nodes),
            Default = [node()],
            ?assertEqual(Default, flurm_controller_app:get_config(cluster_nodes, Default))
        end},
        {"ra_data_dir default", fun() ->
            application:unset_env(flurm_controller, ra_data_dir),
            ?assertEqual("/var/lib/flurm/ra",
                        flurm_controller_app:get_config(ra_data_dir, "/var/lib/flurm/ra"))
        end}
     ]}.

%%====================================================================
%% count_jobs_by_state Tests
%%====================================================================

count_jobs_by_state_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(0, flurm_controller_app:count_jobs_by_state([], pending))
        end},
        {"single pending job", fun() ->
            Job = create_mock_job(1, pending),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state([Job], pending))
        end},
        {"single running job", fun() ->
            Job = create_mock_job(1, running),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state([Job], running))
        end},
        {"single completed job", fun() ->
            Job = create_mock_job(1, completed),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state([Job], completed))
        end},
        {"multiple pending jobs", fun() ->
            Jobs = [create_mock_job(I, pending) || I <- lists:seq(1, 5)],
            ?assertEqual(5, flurm_controller_app:count_jobs_by_state(Jobs, pending))
        end},
        {"mixed job states", fun() ->
            Jobs = [
                create_mock_job(1, pending),
                create_mock_job(2, pending),
                create_mock_job(3, running),
                create_mock_job(4, running),
                create_mock_job(5, running),
                create_mock_job(6, completed),
                create_mock_job(7, failed),
                create_mock_job(8, cancelled)
            ],
            ?assertEqual(2, flurm_controller_app:count_jobs_by_state(Jobs, pending)),
            ?assertEqual(3, flurm_controller_app:count_jobs_by_state(Jobs, running)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, completed)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, failed)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, cancelled))
        end},
        {"no matching state", fun() ->
            Jobs = [
                create_mock_job(1, running),
                create_mock_job(2, running),
                create_mock_job(3, running)
            ],
            ?assertEqual(0, flurm_controller_app:count_jobs_by_state(Jobs, pending))
        end}
    ].

%%====================================================================
%% count_nodes_by_state Tests
%%====================================================================

count_nodes_by_state_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(0, flurm_controller_app:count_nodes_by_state([], idle))
        end},
        {"single idle node", fun() ->
            Node = create_mock_node(<<"node1">>, idle),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state([Node], idle))
        end},
        {"single down node", fun() ->
            Node = create_mock_node(<<"node1">>, down),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state([Node], down))
        end},
        {"single allocated node", fun() ->
            Node = create_mock_node(<<"node1">>, allocated),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state([Node], allocated))
        end},
        {"single mixed node", fun() ->
            Node = create_mock_node(<<"node1">>, mixed),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state([Node], mixed))
        end},
        {"multiple idle nodes", fun() ->
            Nodes = [create_mock_node(<<"node", (integer_to_binary(I))/binary>>, idle)
                    || I <- lists:seq(1, 5)],
            ?assertEqual(5, flurm_controller_app:count_nodes_by_state(Nodes, idle))
        end},
        {"mixed node states", fun() ->
            Nodes = [
                create_mock_node(<<"node1">>, idle),
                create_mock_node(<<"node2">>, idle),
                create_mock_node(<<"node3">>, allocated),
                create_mock_node(<<"node4">>, allocated),
                create_mock_node(<<"node5">>, allocated),
                create_mock_node(<<"node6">>, mixed),
                create_mock_node(<<"node7">>, down),
                create_mock_node(<<"node8">>, drain)
            ],
            ?assertEqual(2, flurm_controller_app:count_nodes_by_state(Nodes, idle)),
            ?assertEqual(3, flurm_controller_app:count_nodes_by_state(Nodes, allocated)),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state(Nodes, mixed)),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state(Nodes, down)),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state(Nodes, drain))
        end},
        {"no matching state", fun() ->
            Nodes = [
                create_mock_node(<<"node1">>, idle),
                create_mock_node(<<"node2">>, idle),
                create_mock_node(<<"node3">>, idle)
            ],
            ?assertEqual(0, flurm_controller_app:count_nodes_by_state(Nodes, down))
        end}
    ].

%%====================================================================
%% API Function Tests
%%====================================================================

status_test_() ->
    [
        {"returns map", fun() ->
            Result = flurm_controller_app:status(),
            ?assert(is_map(Result))
        end},
        {"contains application key", fun() ->
            Result = flurm_controller_app:status(),
            ?assertEqual(flurm_controller, maps:get(application, Result))
        end},
        {"contains status key", fun() ->
            Result = flurm_controller_app:status(),
            ?assertEqual(running, maps:get(status, Result))
        end},
        {"contains listener key", fun() ->
            Result = flurm_controller_app:status(),
            ?assert(maps:is_key(listener, Result))
        end},
        {"contains node_listener key", fun() ->
            Result = flurm_controller_app:status(),
            ?assert(maps:is_key(node_listener, Result))
        end},
        {"contains jobs key", fun() ->
            Result = flurm_controller_app:status(),
            ?assert(maps:is_key(jobs, Result))
        end},
        {"contains nodes key", fun() ->
            Result = flurm_controller_app:status(),
            ?assert(maps:is_key(nodes, Result))
        end},
        {"contains partitions key", fun() ->
            Result = flurm_controller_app:status(),
            ?assert(maps:is_key(partitions, Result))
        end}
    ].

config_test_() ->
    [
        {"returns map", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(is_map(Result))
        end},
        {"contains listen_port", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(listen_port, Result))
        end},
        {"contains listen_address", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(listen_address, Result))
        end},
        {"contains num_acceptors", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(num_acceptors, Result))
        end},
        {"contains max_connections", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(max_connections, Result))
        end},
        {"contains cluster_name", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(cluster_name, Result))
        end},
        {"contains cluster_nodes", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(cluster_nodes, Result))
        end},
        {"contains ra_data_dir", fun() ->
            Result = flurm_controller_app:config(),
            ?assert(maps:is_key(ra_data_dir, Result))
        end}
    ].

cluster_status_test_() ->
    [
        {"returns map", fun() ->
            Result = flurm_controller_app:cluster_status(),
            ?assert(is_map(Result))
        end},
        {"handles cluster not available", fun() ->
            %% When cluster module not running, should return not_available status
            Result = flurm_controller_app:cluster_status(),
            ?assert(is_map(Result))
        end}
    ].

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    [
        {"large job list", fun() ->
            Jobs = [create_mock_job(I, pending) || I <- lists:seq(1, 1000)],
            ?assertEqual(1000, flurm_controller_app:count_jobs_by_state(Jobs, pending))
        end},
        {"large node list", fun() ->
            Nodes = [create_mock_node(integer_to_binary(I), idle)
                    || I <- lists:seq(1, 1000)],
            ?assertEqual(1000, flurm_controller_app:count_nodes_by_state(Nodes, idle))
        end},
        {"various job states", fun() ->
            States = [pending, running, completed, failed, cancelled, timeout, node_fail],
            Jobs = [create_mock_job(I, lists:nth((I rem length(States)) + 1, States))
                   || I <- lists:seq(1, 70)],
            TotalCounted = lists:foldl(fun(S, Acc) ->
                Acc + flurm_controller_app:count_jobs_by_state(Jobs, S)
            end, 0, States),
            ?assertEqual(70, TotalCounted)
        end},
        {"various node states", fun() ->
            States = [idle, allocated, mixed, down, drain],
            Nodes = [create_mock_node(integer_to_binary(I),
                                     lists:nth((I rem length(States)) + 1, States))
                    || I <- lists:seq(1, 50)],
            TotalCounted = lists:foldl(fun(S, Acc) ->
                Acc + flurm_controller_app:count_nodes_by_state(Nodes, S)
            end, 0, States),
            ?assertEqual(50, TotalCounted)
        end}
    ].

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a mock job record (tuple) for testing
%% The count_jobs_by_state function expects element(5, J) to be the state
%% Record: #job{id, name, user, partition, state, ...}
create_mock_job(Id, State) ->
    {job,
     Id,                    % id (element 2)
     <<"test_job">>,       % name (element 3)
     <<"user">>,           % user (element 4)
     State,                % state (element 5) - this is what's tested
     <<"default">>,        % partition
     <<"#!/bin/bash">>,    % script
     1,                    % num_nodes
     1,                    % num_cpus
     1024,                 % memory_mb
     3600,                 % time_limit
     100,                  % priority
     erlang:system_time(second),  % submit_time
     undefined,            % start_time
     undefined,            % end_time
     [],                   % allocated_nodes
     undefined,            % exit_code
     <<"default">>,        % account
     <<"normal">>,         % qos
     []                    % licenses
    }.

%% Create a mock node record (tuple) for testing
%% The count_nodes_by_state function expects element(4, N) to be the state
%% Record: #node{hostname, ip_address, port, state, ...}
create_mock_node(Hostname, State) ->
    {node,
     Hostname,             % hostname (element 2)
     <<"127.0.0.1">>,     % ip_address (element 3)
     State,               % state (element 4) - this is what's tested
     32,                  % cpus
     65536,               % memory_mb
     [],                  % features
     [<<"default">>],     % partitions
     [],                  % running_jobs
     0.0,                 % load_avg
     65536,               % free_memory_mb
     erlang:system_time(second), % last_heartbeat
     #{},                 % extra
     6818                 % port
    }.

%%====================================================================
%% Boundary Value Tests
%%====================================================================

boundary_value_test_() ->
    [
        {"count_jobs with single element list", fun() ->
            Job = create_mock_job(1, pending),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state([Job], pending)),
            ?assertEqual(0, flurm_controller_app:count_jobs_by_state([Job], running))
        end},
        {"count_nodes with single element list", fun() ->
            Node = create_mock_node(<<"n1">>, idle),
            ?assertEqual(1, flurm_controller_app:count_nodes_by_state([Node], idle)),
            ?assertEqual(0, flurm_controller_app:count_nodes_by_state([Node], down))
        end},
        {"count with all same state", fun() ->
            Jobs = [create_mock_job(I, running) || I <- lists:seq(1, 10)],
            ?assertEqual(10, flurm_controller_app:count_jobs_by_state(Jobs, running)),
            ?assertEqual(0, flurm_controller_app:count_jobs_by_state(Jobs, pending)),
            ?assertEqual(0, flurm_controller_app:count_jobs_by_state(Jobs, completed))
        end},
        {"count with all different states", fun() ->
            Jobs = [
                create_mock_job(1, pending),
                create_mock_job(2, running),
                create_mock_job(3, completed),
                create_mock_job(4, failed),
                create_mock_job(5, cancelled)
            ],
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, pending)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, running)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, completed)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, failed)),
            ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, cancelled))
        end}
    ].

%%====================================================================
%% Configuration Validation Tests
%%====================================================================

config_validation_test_() ->
    [
        {"valid listen_port range", fun() ->
            application:set_env(flurm_controller, listen_port, 1),
            ?assertEqual(1, flurm_controller_app:get_config(listen_port, 6817)),
            application:set_env(flurm_controller, listen_port, 65535),
            ?assertEqual(65535, flurm_controller_app:get_config(listen_port, 6817)),
            application:unset_env(flurm_controller, listen_port)
        end},
        {"valid num_acceptors range", fun() ->
            application:set_env(flurm_controller, num_acceptors, 1),
            ?assertEqual(1, flurm_controller_app:get_config(num_acceptors, 10)),
            application:set_env(flurm_controller, num_acceptors, 1000),
            ?assertEqual(1000, flurm_controller_app:get_config(num_acceptors, 10)),
            application:unset_env(flurm_controller, num_acceptors)
        end},
        {"valid max_connections range", fun() ->
            application:set_env(flurm_controller, max_connections, 10),
            ?assertEqual(10, flurm_controller_app:get_config(max_connections, 1000)),
            application:set_env(flurm_controller, max_connections, 100000),
            ?assertEqual(100000, flurm_controller_app:get_config(max_connections, 1000)),
            application:unset_env(flurm_controller, max_connections)
        end},
        {"cluster_nodes with multiple nodes", fun() ->
            Nodes = [node1@host1, node2@host2, node3@host3],
            application:set_env(flurm_controller, cluster_nodes, Nodes),
            ?assertEqual(Nodes, flurm_controller_app:get_config(cluster_nodes, [node()])),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"custom ra_data_dir", fun() ->
            Dir = "/custom/path/to/ra",
            application:set_env(flurm_controller, ra_data_dir, Dir),
            ?assertEqual(Dir, flurm_controller_app:get_config(ra_data_dir, "/var/lib/flurm/ra")),
            application:unset_env(flurm_controller, ra_data_dir)
        end}
    ].

%%====================================================================
%% State Distribution Tests
%%====================================================================

state_distribution_test_() ->
    [
        {"count handles uniform distribution", fun() ->
            %% Create 100 jobs evenly distributed across 5 states
            Jobs = lists:flatten([
                [create_mock_job(I*5+1, pending) || I <- lists:seq(0, 19)],
                [create_mock_job(I*5+2, running) || I <- lists:seq(0, 19)],
                [create_mock_job(I*5+3, completed) || I <- lists:seq(0, 19)],
                [create_mock_job(I*5+4, failed) || I <- lists:seq(0, 19)],
                [create_mock_job(I*5+5, cancelled) || I <- lists:seq(0, 19)]
            ]),
            ?assertEqual(20, flurm_controller_app:count_jobs_by_state(Jobs, pending)),
            ?assertEqual(20, flurm_controller_app:count_jobs_by_state(Jobs, running)),
            ?assertEqual(20, flurm_controller_app:count_jobs_by_state(Jobs, completed)),
            ?assertEqual(20, flurm_controller_app:count_jobs_by_state(Jobs, failed)),
            ?assertEqual(20, flurm_controller_app:count_jobs_by_state(Jobs, cancelled))
        end},
        {"count handles skewed distribution", fun() ->
            %% Create heavily skewed distribution
            Jobs = lists:flatten([
                [create_mock_job(I, pending) || I <- lists:seq(1, 90)],
                [create_mock_job(I + 90, running) || I <- lists:seq(1, 5)],
                [create_mock_job(I + 95, completed) || I <- lists:seq(1, 3)],
                [create_mock_job(I + 98, failed) || I <- lists:seq(1, 2)]
            ]),
            ?assertEqual(90, flurm_controller_app:count_jobs_by_state(Jobs, pending)),
            ?assertEqual(5, flurm_controller_app:count_jobs_by_state(Jobs, running)),
            ?assertEqual(3, flurm_controller_app:count_jobs_by_state(Jobs, completed)),
            ?assertEqual(2, flurm_controller_app:count_jobs_by_state(Jobs, failed))
        end},
        {"node count handles uniform distribution", fun() ->
            %% Create nodes evenly distributed across states
            Nodes = lists:flatten([
                [create_mock_node(integer_to_binary(I*4+1), idle) || I <- lists:seq(0, 9)],
                [create_mock_node(integer_to_binary(I*4+2), allocated) || I <- lists:seq(0, 9)],
                [create_mock_node(integer_to_binary(I*4+3), mixed) || I <- lists:seq(0, 9)],
                [create_mock_node(integer_to_binary(I*4+4), down) || I <- lists:seq(0, 9)]
            ]),
            ?assertEqual(10, flurm_controller_app:count_nodes_by_state(Nodes, idle)),
            ?assertEqual(10, flurm_controller_app:count_nodes_by_state(Nodes, allocated)),
            ?assertEqual(10, flurm_controller_app:count_nodes_by_state(Nodes, mixed)),
            ?assertEqual(10, flurm_controller_app:count_nodes_by_state(Nodes, down))
        end}
    ].

%%====================================================================
%% API Function Extended Tests
%%====================================================================

api_extended_test_() ->
    [
        {"status returns consistent structure", fun() ->
            Status1 = flurm_controller_app:status(),
            Status2 = flurm_controller_app:status(),
            %% Static fields should match
            ?assertEqual(maps:get(application, Status1), maps:get(application, Status2)),
            ?assertEqual(maps:get(status, Status1), maps:get(status, Status2))
        end},
        {"config returns consistent structure", fun() ->
            Config1 = flurm_controller_app:config(),
            Config2 = flurm_controller_app:config(),
            %% Config should be identical between calls
            ?assertEqual(maps:keys(Config1), maps:keys(Config2))
        end},
        {"status has all required keys", fun() ->
            RequiredKeys = [application, status, listener, node_listener, jobs, nodes, partitions],
            Status = flurm_controller_app:status(),
            lists:foreach(fun(Key) ->
                ?assert(maps:is_key(Key, Status))
            end, RequiredKeys)
        end},
        {"config has all required keys", fun() ->
            RequiredKeys = [listen_port, listen_address, num_acceptors, max_connections,
                           cluster_name, cluster_nodes, ra_data_dir],
            Config = flurm_controller_app:config(),
            lists:foreach(fun(Key) ->
                ?assert(maps:is_key(Key, Config))
            end, RequiredKeys)
        end},
        {"cluster_status handles missing cluster gracefully", fun() ->
            %% Should not crash even if cluster is not configured
            Result = flurm_controller_app:cluster_status(),
            ?assert(is_map(Result)),
            ?assert(maps:is_key(status, Result) orelse maps:is_key(cluster_enabled, Result))
        end}
    ].

%%====================================================================
%% Stop Callback Tests
%%====================================================================

stop_callback_test_() ->
    [
        {"stop returns ok with undefined state", fun() ->
            ?assertEqual(ok, flurm_controller_app:stop(undefined))
        end},
        {"stop returns ok with any state", fun() ->
            ?assertEqual(ok, flurm_controller_app:stop(some_state)),
            ?assertEqual(ok, flurm_controller_app:stop({complex, state})),
            ?assertEqual(ok, flurm_controller_app:stop(#{key => value})),
            ?assertEqual(ok, flurm_controller_app:stop([1, 2, 3]))
        end}
    ].

%%====================================================================
%% Job State Enumeration Tests
%%====================================================================

job_state_enumeration_test_() ->
    [
        {"all standard job states", fun() ->
            %% Test all standard job states
            States = [pending, running, completed, failed, cancelled, timeout, node_fail, preempted],
            lists:foreach(fun(State) ->
                Job = create_mock_job(1, State),
                ?assertEqual(1, flurm_controller_app:count_jobs_by_state([Job], State))
            end, States)
        end},
        {"all standard node states", fun() ->
            %% Test all standard node states
            States = [idle, allocated, mixed, down, drain, maint, resume],
            lists:foreach(fun(State) ->
                Node = create_mock_node(<<"n1">>, State),
                ?assertEqual(1, flurm_controller_app:count_nodes_by_state([Node], State))
            end, States)
        end},
        {"unknown job state returns zero", fun() ->
            Jobs = [create_mock_job(I, pending) || I <- lists:seq(1, 10)],
            ?assertEqual(0, flurm_controller_app:count_jobs_by_state(Jobs, nonexistent_state))
        end},
        {"unknown node state returns zero", fun() ->
            Nodes = [create_mock_node(integer_to_binary(I), idle) || I <- lists:seq(1, 10)],
            ?assertEqual(0, flurm_controller_app:count_nodes_by_state(Nodes, nonexistent_state))
        end}
    ].

%%====================================================================
%% Performance Tests
%%====================================================================

performance_test_() ->
    [
        {"count_jobs scales with list size", fun() ->
            %% Test with increasing sizes
            Sizes = [100, 500, 1000],
            lists:foreach(fun(Size) ->
                Jobs = [create_mock_job(I, pending) || I <- lists:seq(1, Size)],
                ?assertEqual(Size, flurm_controller_app:count_jobs_by_state(Jobs, pending))
            end, Sizes)
        end},
        {"count_nodes scales with list size", fun() ->
            %% Test with increasing sizes
            Sizes = [100, 500, 1000],
            lists:foreach(fun(Size) ->
                Nodes = [create_mock_node(integer_to_binary(I), idle) || I <- lists:seq(1, Size)],
                ?assertEqual(Size, flurm_controller_app:count_nodes_by_state(Nodes, idle))
            end, Sizes)
        end}
    ].
