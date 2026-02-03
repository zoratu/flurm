%%%-------------------------------------------------------------------
%%% @doc Unit Tests for Federated Job ID Handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_id_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Cases
%%%===================================================================

make_fed_job_id_test_() ->
    [
        {"cluster 0, local 1", fun() ->
            ?assertEqual(1, flurm_job_id:make_fed_job_id(0, 1))
        end},

        {"cluster 0, local 12345", fun() ->
            ?assertEqual(12345, flurm_job_id:make_fed_job_id(0, 12345))
        end},

        {"cluster 1, local 1", fun() ->
            %% Cluster 1 shifts left by 26 bits: 1 << 26 = 67108864
            Expected = 67108864 + 1,
            ?assertEqual(Expected, flurm_job_id:make_fed_job_id(1, 1))
        end},

        {"cluster 1, local 12345", fun() ->
            Expected = 67108864 + 12345,
            ?assertEqual(Expected, flurm_job_id:make_fed_job_id(1, 12345))
        end},

        {"cluster 63 (max), local 1", fun() ->
            %% Cluster 63 << 26 = 4227858432
            Expected = 4227858432 + 1,
            ?assertEqual(Expected, flurm_job_id:make_fed_job_id(63, 1))
        end},

        {"max local id", fun() ->
            MaxLocal = 67108863,  % 2^26 - 1
            Result = flurm_job_id:make_fed_job_id(0, MaxLocal),
            ?assertEqual(MaxLocal, Result)
        end},

        {"max cluster and local", fun() ->
            MaxCluster = 63,
            MaxLocal = 67108863,
            Result = flurm_job_id:make_fed_job_id(MaxCluster, MaxLocal),
            %% This should be the maximum possible job ID
            ?assertEqual(4294967295, Result)  % 2^32 - 1
        end}
    ].

get_cluster_id_test_() ->
    [
        {"cluster 0 job", fun() ->
            ?assertEqual(0, flurm_job_id:get_cluster_id(12345))
        end},

        {"cluster 1 job", fun() ->
            FedId = flurm_job_id:make_fed_job_id(1, 12345),
            ?assertEqual(1, flurm_job_id:get_cluster_id(FedId))
        end},

        {"cluster 63 job", fun() ->
            FedId = flurm_job_id:make_fed_job_id(63, 1),
            ?assertEqual(63, flurm_job_id:get_cluster_id(FedId))
        end}
    ].

get_local_id_test_() ->
    [
        {"local id extraction", fun() ->
            ?assertEqual(12345, flurm_job_id:get_local_id(12345))
        end},

        {"local id from cluster 1 job", fun() ->
            FedId = flurm_job_id:make_fed_job_id(1, 99999),
            ?assertEqual(99999, flurm_job_id:get_local_id(FedId))
        end},

        {"local id from max job", fun() ->
            FedId = flurm_job_id:make_fed_job_id(63, 67108863),
            ?assertEqual(67108863, flurm_job_id:get_local_id(FedId))
        end}
    ].

roundtrip_test_() ->
    [
        {"roundtrip cluster 0", fun() ->
            LocalId = 54321,
            FedId = flurm_job_id:make_fed_job_id(0, LocalId),
            ?assertEqual(0, flurm_job_id:get_cluster_id(FedId)),
            ?assertEqual(LocalId, flurm_job_id:get_local_id(FedId))
        end},

        {"roundtrip cluster 42", fun() ->
            ClusterId = 42,
            LocalId = 1000000,
            FedId = flurm_job_id:make_fed_job_id(ClusterId, LocalId),
            ?assertEqual(ClusterId, flurm_job_id:get_cluster_id(FedId)),
            ?assertEqual(LocalId, flurm_job_id:get_local_id(FedId))
        end}
    ].

is_federated_id_test_() ->
    [
        {"cluster 0 is not federated", fun() ->
            ?assertNot(flurm_job_id:is_federated_id(12345))
        end},

        {"cluster 1 is federated", fun() ->
            FedId = flurm_job_id:make_fed_job_id(1, 1),
            ?assert(flurm_job_id:is_federated_id(FedId))
        end}
    ].

parse_job_id_string_test_() ->
    [
        {"simple integer string", fun() ->
            ?assertEqual({ok, 12345}, flurm_job_id:parse_job_id_string("12345"))
        end},

        {"binary input", fun() ->
            ?assertEqual({ok, 12345}, flurm_job_id:parse_job_id_string(<<"12345">>))
        end},

        {"array job notation", fun() ->
            ?assertEqual({ok, 12345}, flurm_job_id:parse_job_id_string("12345_0"))
        end},

        {"array job range", fun() ->
            ?assertEqual({ok, 12345}, flurm_job_id:parse_job_id_string("12345_[0-9]"))
        end},

        {"with whitespace", fun() ->
            ?assertEqual({ok, 12345}, flurm_job_id:parse_job_id_string("  12345  "))
        end},

        {"invalid string", fun() ->
            ?assertEqual({error, invalid_job_id}, flurm_job_id:parse_job_id_string("abc"))
        end}
    ].

format_job_id_test_() ->
    [
        {"cluster 0 job", fun() ->
            ?assertEqual(<<"12345">>, flurm_job_id:format_job_id(12345))
        end},

        {"cluster 1 job", fun() ->
            FedId = flurm_job_id:make_fed_job_id(1, 12345),
            ?assertEqual(<<"1:12345">>, flurm_job_id:format_job_id(FedId))
        end},

        {"cluster 63 job", fun() ->
            FedId = flurm_job_id:make_fed_job_id(63, 1),
            ?assertEqual(<<"63:1">>, flurm_job_id:format_job_id(FedId))
        end}
    ].

error_handling_test_() ->
    [
        {"cluster id too large", fun() ->
            ?assertError({invalid_job_id_components, 64, 1},
                         flurm_job_id:make_fed_job_id(64, 1))
        end},

        {"local id too large", fun() ->
            ?assertError({invalid_job_id_components, 0, 67108864},
                         flurm_job_id:make_fed_job_id(0, 67108864))
        end},

        {"negative cluster id", fun() ->
            ?assertError({invalid_job_id_components, -1, 1},
                         flurm_job_id:make_fed_job_id(-1, 1))
        end}
    ].
