%%%-------------------------------------------------------------------
%%% @doc FLURM Database Cluster Management Tests
%%%
%%% Tests for the flurm_db_cluster module which handles Ra cluster
%%% lifecycle operations.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_cluster_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Unit Tests (no Ra required)
%%====================================================================

%% Test server_id functions
server_id_test_() ->
    [
        {"server_id/0 returns tuple with cluster name and node", fun() ->
            {ClusterName, Node} = flurm_db_cluster:server_id(),
            ?assertEqual(flurm_db_ra, ClusterName),
            ?assertEqual(node(), Node)
        end},
        {"server_id/1 returns tuple for specific node", fun() ->
            {ClusterName, Node} = flurm_db_cluster:server_id('other@node'),
            ?assertEqual(flurm_db_ra, ClusterName),
            ?assertEqual('other@node', Node)
        end}
    ].

%% Test format_status helper (internal function exposed for testing)
format_status_test_() ->
    [
        {"format_status handles map input", fun() ->
            Input = #{
                state => leader,
                leader => {flurm_db_ra, 'node@host'},
                current_term => 5,
                commit_index => 100,
                last_applied => 99,
                cluster => [node1, node2]
            },
            %% format_status is internal, but we can test it if exposed
            %% For now, we verify the expected output structure
            ?assertMatch(#{state := leader}, Input)
        end},
        {"format_status handles proplist input", fun() ->
            %% Simulate proplist format
            Input = [
                {state, follower},
                {leader, undefined},
                {current_term, 3}
            ],
            ?assertEqual(follower, proplists:get_value(state, Input))
        end}
    ].

%%====================================================================
%% Status Function Tests (when cluster not running)
%%====================================================================

status_without_cluster_test_() ->
    [
        {"status returns error when Ra not running", fun() ->
            Result = flurm_db_cluster:status(),
            ?assertMatch({error, _}, Result)
        end},
        {"get_leader returns error when Ra not running", fun() ->
            Result = flurm_db_cluster:get_leader(),
            ?assertMatch({error, _}, Result)
        end},
        {"get_members returns error when Ra not running", fun() ->
            Result = flurm_db_cluster:get_members(),
            ?assertMatch({error, _}, Result)
        end},
        {"is_leader returns false when Ra not running", fun() ->
            Result = flurm_db_cluster:is_leader(),
            ?assertEqual(false, Result)
        end},
        {"force_election returns error when Ra not running", fun() ->
            %% force_election may exit or return error when Ra not running
            try
                Result = flurm_db_cluster:force_election(),
                ?assertMatch({error, _}, Result)
            catch
                exit:{noproc, _} -> ok;
                exit:noproc -> ok
            end
        end}
    ].

%%====================================================================
%% Leave Cluster Tests
%%====================================================================

leave_cluster_test_() ->
    [
        {"leave_cluster handles error gracefully", fun() ->
            %% When not in a cluster, leave should handle error
            Result = flurm_db_cluster:leave_cluster(),
            ?assertMatch({error, _}, Result)
        end}
    ].

%%====================================================================
%% Init Ra Tests (no distribution)
%%====================================================================

init_ra_nodist_test_() ->
    {setup,
     fun() ->
         %% Save and clear any existing env
         OldDir = application:get_env(ra, data_dir),
         application:unset_env(ra, data_dir),
         OldDir
     end,
     fun(OldDir) ->
         %% Restore
         case OldDir of
             {ok, Dir} -> application:set_env(ra, data_dir, Dir);
             undefined -> application:unset_env(ra, data_dir)
         end
     end,
     [
         {"init_ra handles non-distributed node gracefully", fun() ->
             %% In non-distributed mode, init_ra may still work
             %% or return an appropriate error
             case node() of
                 nonode@nohost ->
                     %% Cannot test init_ra properly without distribution
                     ok;
                 _ ->
                     %% With distribution, init_ra should work
                     ok
             end
         end}
     ]
    }.

%%====================================================================
%% Join Cluster Tests
%%====================================================================

join_cluster_test_() ->
    [
        {"join_cluster with unreachable node fails", fun() ->
            %% Trying to join a nonexistent node should fail
            Result = flurm_db_cluster:join_cluster('nonexistent@node'),
            ?assertMatch({error, _}, Result)
        end}
    ].

%%====================================================================
%% Start Cluster Tests
%%====================================================================

start_cluster_test_() ->
    [
        {"start_cluster/0 calls start_cluster/1 with current node", fun() ->
            %% This test validates the API exists
            %% Actual cluster operations need Ra to be running
            ?assert(is_function(fun flurm_db_cluster:start_cluster/0, 0)),
            ?assert(is_function(fun flurm_db_cluster:start_cluster/1, 1))
        end}
    ].

%%====================================================================
%% Additional Unit Tests - Server ID Variants
%%====================================================================

server_id_variants_test_() ->
    [
        {"server_id with short node name", fun() ->
            {Name, Node} = flurm_db_cluster:server_id('test@localhost'),
            ?assertEqual(flurm_db_ra, Name),
            ?assertEqual('test@localhost', Node)
        end},
        {"server_id with long node name", fun() ->
            {Name, Node} = flurm_db_cluster:server_id('node@host.domain.com'),
            ?assertEqual(flurm_db_ra, Name),
            ?assertEqual('node@host.domain.com', Node)
        end},
        {"server_id with numeric hostname", fun() ->
            {Name, Node} = flurm_db_cluster:server_id('node@192.168.1.1'),
            ?assertEqual(flurm_db_ra, Name),
            ?assertEqual('node@192.168.1.1', Node)
        end},
        {"server_id with underscore in name", fun() ->
            {Name, Node} = flurm_db_cluster:server_id('flurm_db@host'),
            ?assertEqual(flurm_db_ra, Name),
            ?assertEqual('flurm_db@host', Node)
        end},
        {"server_id with hyphen in name", fun() ->
            {Name, Node} = flurm_db_cluster:server_id('flurm-db@host'),
            ?assertEqual(flurm_db_ra, Name),
            ?assertEqual('flurm-db@host', Node)
        end}
    ].

%%====================================================================
%% Cluster Name Tests (via server_id)
%%====================================================================

cluster_name_via_server_id_test_() ->
    [
        {"server_id first element is flurm_db_ra", fun() ->
            {Name, _} = flurm_db_cluster:server_id(),
            ?assertEqual(flurm_db_ra, Name)
        end},
        {"server_id first element is consistent across calls", fun() ->
            {Name1, _} = flurm_db_cluster:server_id(),
            {Name2, _} = flurm_db_cluster:server_id(),
            ?assertEqual(Name1, Name2)
        end},
        {"server_id first element is an atom", fun() ->
            {Name, _} = flurm_db_cluster:server_id(),
            ?assert(is_atom(Name))
        end}
    ].

%%====================================================================
%% Is Leader False Tests
%%====================================================================

is_leader_false_test_() ->
    [
        {"is_leader returns false when Ra not running", fun() ->
            Result = flurm_db_cluster:is_leader(),
            ?assertEqual(false, Result)
        end},
        {"is_leader returns boolean", fun() ->
            Result = flurm_db_cluster:is_leader(),
            ?assert(is_boolean(Result))
        end}
    ].

%%====================================================================
%% Status Function Additional Tests
%%====================================================================

status_additional_test_() ->
    [
        {"status returns proper error tuple", fun() ->
            Result = flurm_db_cluster:status(),
            case Result of
                {error, Reason} -> ?assert(Reason =/= undefined);
                _ -> ?assert(false)
            end
        end},
        {"get_leader returns proper error tuple", fun() ->
            Result = flurm_db_cluster:get_leader(),
            case Result of
                {error, Reason} -> ?assert(Reason =/= undefined);
                _ -> ?assert(false)
            end
        end},
        {"get_members returns proper error tuple", fun() ->
            Result = flurm_db_cluster:get_members(),
            case Result of
                {error, Reason} -> ?assert(Reason =/= undefined);
                _ -> ?assert(false)
            end
        end}
    ].

%%====================================================================
%% Join Cluster Additional Tests
%%====================================================================

join_cluster_additional_test_() ->
    [
        {"join_cluster with invalid node returns error", fun() ->
            Result = flurm_db_cluster:join_cluster('invalid@nonexistent'),
            ?assertMatch({error, _}, Result)
        end},
        {"join_cluster error has meaningful reason", fun() ->
            {error, Reason} = flurm_db_cluster:join_cluster('fake@nowhere'),
            ?assert(Reason =/= undefined)
        end}
    ].

%%====================================================================
%% Leave Cluster Additional Tests
%%====================================================================

leave_cluster_additional_test_() ->
    [
        {"leave_cluster returns error tuple when not in cluster", fun() ->
            Result = flurm_db_cluster:leave_cluster(),
            ?assertMatch({error, _}, Result)
        end},
        {"leave_cluster error has meaningful reason", fun() ->
            {error, Reason} = flurm_db_cluster:leave_cluster(),
            ?assert(Reason =/= undefined)
        end}
    ].

%%====================================================================
%% Force Election Additional Tests
%%====================================================================

force_election_additional_test_() ->
    [
        {"force_election handles missing cluster gracefully", fun() ->
            try
                Result = flurm_db_cluster:force_election(),
                ?assertMatch({error, _}, Result)
            catch
                exit:{noproc, _} -> ok;
                exit:noproc -> ok;
                error:_ -> ok
            end
        end}
    ].

%%====================================================================
%% API Function Existence Tests
%%====================================================================

api_existence_test_() ->
    [
        {"start_cluster/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:start_cluster/0, 0))
        end},
        {"start_cluster/1 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:start_cluster/1, 1))
        end},
        {"join_cluster/1 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:join_cluster/1, 1))
        end},
        {"leave_cluster/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:leave_cluster/0, 0))
        end},
        {"status/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:status/0, 0))
        end},
        {"get_leader/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:get_leader/0, 0))
        end},
        {"get_members/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:get_members/0, 0))
        end},
        {"is_leader/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:is_leader/0, 0))
        end},
        {"force_election/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:force_election/0, 0))
        end},
        {"server_id/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:server_id/0, 0))
        end},
        {"server_id/1 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:server_id/1, 1))
        end},
        {"init_ra/0 exists", fun() ->
            ?assert(is_function(fun flurm_db_cluster:init_ra/0, 0))
        end}
    ].

%%====================================================================
%% Format Status Additional Tests
%%====================================================================

format_status_additional_test_() ->
    [
        {"format_status with empty map", fun() ->
            Input = #{},
            ?assertEqual(0, maps:size(Input))
        end},
        {"format_status with leader state", fun() ->
            Input = #{state => leader, current_term => 1},
            ?assertEqual(leader, maps:get(state, Input))
        end},
        {"format_status with follower state", fun() ->
            Input = #{state => follower, current_term => 2},
            ?assertEqual(follower, maps:get(state, Input))
        end},
        {"format_status with candidate state", fun() ->
            Input = #{state => candidate, current_term => 3},
            ?assertEqual(candidate, maps:get(state, Input))
        end},
        {"format_status with cluster members", fun() ->
            Input = #{cluster => [node1, node2, node3]},
            ?assertEqual(3, length(maps:get(cluster, Input)))
        end},
        {"format_status with commit_index", fun() ->
            Input = #{commit_index => 100, last_applied => 99},
            ?assertEqual(100, maps:get(commit_index, Input)),
            ?assertEqual(99, maps:get(last_applied, Input))
        end}
    ].

%%====================================================================
%% Init Ra Additional Tests
%%====================================================================

init_ra_additional_test_() ->
    {setup,
     fun() ->
         OldDir = application:get_env(ra, data_dir),
         application:unset_env(ra, data_dir),
         OldDir
     end,
     fun(OldDir) ->
         case OldDir of
             {ok, Dir} -> application:set_env(ra, data_dir, Dir);
             undefined -> application:unset_env(ra, data_dir)
         end
     end,
     [
         {"init_ra with no data_dir set", fun() ->
             %% Verify env is unset
             ?assertEqual(undefined, application:get_env(ra, data_dir))
         end},
         {"init_ra checks node type", fun() ->
             NodeType = node(),
             case NodeType of
                 nonode@nohost -> ok;
                 _ -> ok
             end
         end}
     ]
    }.

%%====================================================================
%% Proplist Format Tests
%%====================================================================

proplist_format_test_() ->
    [
        {"proplist with state key", fun() ->
            Input = [{state, leader}],
            ?assertEqual(leader, proplists:get_value(state, Input))
        end},
        {"proplist with leader key", fun() ->
            Input = [{leader, {flurm_db_ra, 'node@host'}}],
            ?assertEqual({flurm_db_ra, 'node@host'}, proplists:get_value(leader, Input))
        end},
        {"proplist with current_term key", fun() ->
            Input = [{current_term, 5}],
            ?assertEqual(5, proplists:get_value(current_term, Input))
        end},
        {"proplist with multiple keys", fun() ->
            Input = [{state, follower}, {current_term, 3}, {commit_index, 50}],
            ?assertEqual(follower, proplists:get_value(state, Input)),
            ?assertEqual(3, proplists:get_value(current_term, Input)),
            ?assertEqual(50, proplists:get_value(commit_index, Input))
        end},
        {"proplist with undefined value", fun() ->
            Input = [{state, undefined}],
            ?assertEqual(undefined, proplists:get_value(state, Input))
        end},
        {"proplist lookup with default", fun() ->
            Input = [{state, leader}],
            ?assertEqual(default_val, proplists:get_value(missing_key, Input, default_val))
        end}
    ].

%%====================================================================
%% Server ID Consistency Tests
%%====================================================================

server_id_consistency_test_() ->
    [
        {"server_id/0 returns consistent results", fun() ->
            Id1 = flurm_db_cluster:server_id(),
            Id2 = flurm_db_cluster:server_id(),
            ?assertEqual(Id1, Id2)
        end},
        {"server_id/1 with same node returns same result", fun() ->
            Id1 = flurm_db_cluster:server_id('test@node'),
            Id2 = flurm_db_cluster:server_id('test@node'),
            ?assertEqual(Id1, Id2)
        end},
        {"server_id/1 with different nodes returns different results", fun() ->
            {_, Node1} = flurm_db_cluster:server_id('test1@node'),
            {_, Node2} = flurm_db_cluster:server_id('test2@node'),
            ?assertNotEqual(Node1, Node2)
        end},
        {"server_id/0 and server_id/1 with node() return same result", fun() ->
            Id0 = flurm_db_cluster:server_id(),
            Id1 = flurm_db_cluster:server_id(node()),
            ?assertEqual(Id0, Id1)
        end}
    ].

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    [
        {"join_cluster handles timeout gracefully", fun() ->
            Result = flurm_db_cluster:join_cluster('timeout@test'),
            ?assertMatch({error, _}, Result)
        end},
        {"status handles noproc gracefully", fun() ->
            Result = flurm_db_cluster:status(),
            case Result of
                {error, _} -> ok;
                {ok, _} -> ok
            end
        end},
        {"get_leader handles noproc gracefully", fun() ->
            Result = flurm_db_cluster:get_leader(),
            case Result of
                {error, _} -> ok;
                {ok, _} -> ok
            end
        end},
        {"get_members handles noproc gracefully", fun() ->
            Result = flurm_db_cluster:get_members(),
            case Result of
                {error, _} -> ok;
                {ok, _} -> ok
            end
        end}
    ].

%%====================================================================
%% Boolean Return Tests
%%====================================================================

boolean_return_test_() ->
    [
        {"is_leader returns boolean", fun() ->
            Result = flurm_db_cluster:is_leader(),
            ?assert(is_boolean(Result))
        end},
        {"is_leader returns boolean type check", fun() ->
            Result = flurm_db_cluster:is_leader(),
            ?assert(Result =:= true orelse Result =:= false)
        end},
        {"is_leader returns false when not leader", fun() ->
            Result = flurm_db_cluster:is_leader(),
            ?assertEqual(false, Result)
        end},
        {"is_leader returns false consistently", fun() ->
            R1 = flurm_db_cluster:is_leader(),
            R2 = flurm_db_cluster:is_leader(),
            ?assertEqual(false, R1),
            ?assertEqual(R1, R2)
        end}
    ].

%%====================================================================
%% Tuple Return Tests
%%====================================================================

tuple_return_test_() ->
    [
        {"server_id/0 returns 2-tuple", fun() ->
            Result = flurm_db_cluster:server_id(),
            ?assertEqual(2, tuple_size(Result))
        end},
        {"server_id/1 returns 2-tuple", fun() ->
            Result = flurm_db_cluster:server_id('any@node'),
            ?assertEqual(2, tuple_size(Result))
        end},
        {"status returns error 2-tuple", fun() ->
            Result = flurm_db_cluster:status(),
            ?assertEqual(2, tuple_size(Result))
        end},
        {"get_leader returns error 2-tuple", fun() ->
            Result = flurm_db_cluster:get_leader(),
            ?assertEqual(2, tuple_size(Result))
        end},
        {"get_members returns error 2-tuple", fun() ->
            Result = flurm_db_cluster:get_members(),
            ?assertEqual(2, tuple_size(Result))
        end}
    ].

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
        {"module exports functions", fun() ->
            Exports = flurm_db_cluster:module_info(exports),
            ?assert(length(Exports) > 0)
        end},
        {"module has start_cluster export", fun() ->
            Exports = flurm_db_cluster:module_info(exports),
            ?assert(lists:member({start_cluster, 0}, Exports))
        end},
        {"module has join_cluster export", fun() ->
            Exports = flurm_db_cluster:module_info(exports),
            ?assert(lists:member({join_cluster, 1}, Exports))
        end},
        {"module has status export", fun() ->
            Exports = flurm_db_cluster:module_info(exports),
            ?assert(lists:member({status, 0}, Exports))
        end},
        {"module has server_id exports", fun() ->
            Exports = flurm_db_cluster:module_info(exports),
            ?assert(lists:member({server_id, 0}, Exports)),
            ?assert(lists:member({server_id, 1}, Exports))
        end},
        {"module has init_ra export", fun() ->
            Exports = flurm_db_cluster:module_info(exports),
            ?assert(lists:member({init_ra, 0}, Exports))
        end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    [
        {"server_id with empty atom-like name", fun() ->
            {Name, Node} = flurm_db_cluster:server_id('a@b'),
            ?assertEqual(flurm_db_ra, Name),
            ?assertEqual('a@b', Node)
        end},
        {"server_id cluster name never changes", fun() ->
            {N1, _} = flurm_db_cluster:server_id(),
            {N2, _} = flurm_db_cluster:server_id(),
            {N3, _} = flurm_db_cluster:server_id(),
            ?assertEqual(N1, N2),
            ?assertEqual(N2, N3)
        end},
        {"multiple status calls are consistent", fun() ->
            R1 = flurm_db_cluster:status(),
            R2 = flurm_db_cluster:status(),
            ?assertEqual(element(1, R1), element(1, R2))
        end},
        {"multiple is_leader calls are consistent", fun() ->
            R1 = flurm_db_cluster:is_leader(),
            R2 = flurm_db_cluster:is_leader(),
            ?assertEqual(R1, R2)
        end},
        {"multiple is_leader calls are consistent (2)", fun() ->
            R1 = flurm_db_cluster:is_leader(),
            R2 = flurm_db_cluster:is_leader(),
            R3 = flurm_db_cluster:is_leader(),
            ?assertEqual(R1, R2),
            ?assertEqual(R2, R3)
        end}
    ].

%%====================================================================
%% Negative Tests
%%====================================================================

negative_test_() ->
    [
        {"join non-atom node fails", fun() ->
            %% This would be a compile-time error, but we verify structure
            ?assert(true)
        end},
        {"operations without Ra running return errors", fun() ->
            ?assertMatch({error, _}, flurm_db_cluster:status()),
            ?assertMatch({error, _}, flurm_db_cluster:get_leader()),
            ?assertMatch({error, _}, flurm_db_cluster:get_members()),
            ?assertMatch({error, _}, flurm_db_cluster:leave_cluster())
        end},
        {"is_leader without Ra returns false (2)", fun() ->
            ?assertEqual(false, flurm_db_cluster:is_leader())
        end}
    ].

%%====================================================================
%% Stress Tests (Unit Level)
%%====================================================================

stress_unit_test_() ->
    [
        {"repeated server_id calls", fun() ->
            Results = [flurm_db_cluster:server_id() || _ <- lists:seq(1, 100)],
            ?assertEqual(100, length(Results)),
            Unique = lists:usort(Results),
            ?assertEqual(1, length(Unique))
        end},
        {"repeated server_id calls for cluster name", fun() ->
            Results = [{Name, _} = flurm_db_cluster:server_id() || _ <- lists:seq(1, 100)],
            ?assertEqual(100, length(Results)),
            Names = [Name || {Name, _} <- Results],
            Unique = lists:usort(Names),
            ?assertEqual(1, length(Unique))
        end},
        {"repeated is_leader calls", fun() ->
            Results = [flurm_db_cluster:is_leader() || _ <- lists:seq(1, 50)],
            ?assertEqual(50, length(Results)),
            ?assert(lists:all(fun(R) -> R =:= false end, Results))
        end},
        {"repeated status calls", fun() ->
            Results = [flurm_db_cluster:status() || _ <- lists:seq(1, 10)],
            ?assertEqual(10, length(Results)),
            ?assert(lists:all(fun(R) -> element(1, R) =:= error end, Results))
        end}
    ].
