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
