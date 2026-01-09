%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_cloud_scaler module
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaler_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the cloud scaler server
    case whereis(flurm_cloud_scaler) of
        undefined ->
            {ok, Pid} = flurm_cloud_scaler:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    catch ets:delete(flurm_cloud_config),
    gen_server:stop(flurm_cloud_scaler);
cleanup({existing, _Pid}) ->
    ok.

cloud_scaler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"initial state", fun test_initial_state/0},
      {"set provider", fun test_set_provider/0},
      {"enable without provider fails", fun test_enable_without_provider/0},
      {"enable with provider", fun test_enable_with_provider/0},
      {"set and get config", fun test_set_get_config/0},
      {"set and get limits", fun test_set_get_limits/0},
      {"get scaling status", fun test_get_scaling_status/0},
      {"aws scale up", fun test_aws_scale_up/0},
      {"gcp scale up", fun test_gcp_scale_up/0},
      {"azure scale up", fun test_azure_scale_up/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_initial_state() ->
    %% Should be disabled initially
    ?assertNot(flurm_cloud_scaler:is_enabled()),

    %% No provider set
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()).

test_set_provider() ->
    %% Set AWS provider
    ok = flurm_cloud_scaler:set_provider(aws),
    ?assertEqual(aws, flurm_cloud_scaler:get_provider()),

    %% Change to GCP
    ok = flurm_cloud_scaler:set_provider(gcp),
    ?assertEqual(gcp, flurm_cloud_scaler:get_provider()),

    %% Change to Azure
    ok = flurm_cloud_scaler:set_provider(azure),
    ?assertEqual(azure, flurm_cloud_scaler:get_provider()).

test_enable_without_provider() ->
    %% Ensure no provider
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()),

    %% Enable should fail
    Result = flurm_cloud_scaler:enable(),
    ?assertEqual({error, no_provider}, Result).

test_enable_with_provider() ->
    %% Set provider first
    ok = flurm_cloud_scaler:set_provider(aws),

    %% Enable should succeed
    ok = flurm_cloud_scaler:enable(),
    ?assert(flurm_cloud_scaler:is_enabled()),

    %% Disable
    ok = flurm_cloud_scaler:disable(),
    ?assertNot(flurm_cloud_scaler:is_enabled()).

test_set_get_config() ->
    %% Set AWS config
    Config = #{
        asg_name => <<"my-asg">>,
        region => <<"us-west-2">>,
        instance_type => <<"c5.xlarge">>
    },
    ok = flurm_cloud_scaler:set_config(Config),

    %% Get config
    Retrieved = flurm_cloud_scaler:get_config(),
    ?assertEqual(<<"my-asg">>, maps:get(asg_name, Retrieved)),
    ?assertEqual(<<"us-west-2">>, maps:get(region, Retrieved)).

test_set_get_limits() ->
    %% Default limits
    {Min0, Max0} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(0, Min0),
    ?assertEqual(100, Max0),

    %% Set new limits
    ok = flurm_cloud_scaler:set_limits(5, 50),
    {Min1, Max1} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(5, Min1),
    ?assertEqual(50, Max1).

test_get_scaling_status() ->
    Status = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(provider, Status)),
    ?assert(maps:is_key(min_nodes, Status)),
    ?assert(maps:is_key(max_nodes, Status)).

test_aws_scale_up() ->
    %% Set AWS provider and config
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"test-asg">>,
        region => <<"us-east-1">>
    }),

    %% Scale up
    {ok, InstanceIds} = flurm_cloud_scaler:scale_up(3),
    ?assert(is_list(InstanceIds)),
    ?assertEqual(3, length(InstanceIds)).

test_gcp_scale_up() ->
    %% Set GCP provider and config
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{
        mig_name => <<"test-mig">>,
        zone => <<"us-central1-a">>
    }),

    %% Scale up
    {ok, InstanceNames} = flurm_cloud_scaler:scale_up(2),
    ?assert(is_list(InstanceNames)),
    ?assertEqual(2, length(InstanceNames)).

test_azure_scale_up() ->
    %% Set Azure provider and config
    ok = flurm_cloud_scaler:set_provider(azure),
    ok = flurm_cloud_scaler:set_config(#{
        vmss_name => <<"test-vmss">>,
        resource_group => <<"test-rg">>
    }),

    %% Scale up
    {ok, VmIds} = flurm_cloud_scaler:scale_up(4),
    ?assert(is_list(VmIds)),
    ?assertEqual(4, length(VmIds)).
