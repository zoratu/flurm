%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_cloud_scaling internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve coverage of pure logic functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaling_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Provider Config Validation Tests
%%====================================================================

validate_aws_config_test() ->
    ValidConfig = #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIAIOSFODNN7EXAMPLE">>,
        secret_access_key => <<"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY">>
    },
    ?assertEqual(ok, flurm_cloud_scaling:validate_provider_config(aws, ValidConfig)).

validate_aws_config_missing_key_test() ->
    InvalidConfig = #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIAIOSFODNN7EXAMPLE">>
        %% Missing secret_access_key
    },
    ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(aws, InvalidConfig)).

validate_gcp_config_test() ->
    ValidConfig = #{
        project_id => <<"my-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    },
    ?assertEqual(ok, flurm_cloud_scaling:validate_provider_config(gcp, ValidConfig)).

validate_gcp_config_missing_key_test() ->
    InvalidConfig = #{
        project_id => <<"my-project">>
        %% Missing zone and credentials_file
    },
    ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(gcp, InvalidConfig)).

validate_azure_config_test() ->
    ValidConfig = #{
        subscription_id => <<"sub-123">>,
        resource_group => <<"my-rg">>,
        tenant_id => <<"tenant-456">>
    },
    ?assertEqual(ok, flurm_cloud_scaling:validate_provider_config(azure, ValidConfig)).

validate_azure_config_missing_key_test() ->
    InvalidConfig = #{
        subscription_id => <<"sub-123">>
    },
    ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(azure, InvalidConfig)).

validate_generic_config_test() ->
    ValidConfig = #{
        scale_up_webhook => <<"https://example.com/scale-up">>,
        scale_down_webhook => <<"https://example.com/scale-down">>
    },
    ?assertEqual(ok, flurm_cloud_scaling:validate_provider_config(generic, ValidConfig)).

validate_generic_config_missing_key_test() ->
    InvalidConfig = #{
        scale_up_webhook => <<"https://example.com/scale-up">>
    },
    ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(generic, InvalidConfig)).

validate_unknown_provider_test() ->
    ?assertEqual({error, unknown_provider},
                 flurm_cloud_scaling:validate_provider_config(unknown, #{})).

%%====================================================================
%% Instance ID Generation Tests
%%====================================================================

generate_action_id_test() ->
    Id1 = flurm_cloud_scaling:generate_action_id(),
    Id2 = flurm_cloud_scaling:generate_action_id(),
    ?assert(is_binary(Id1)),
    ?assert(is_binary(Id2)),
    ?assertMatch(<<"scale-", _/binary>>, Id1),
    %% IDs should be unique
    ?assertNotEqual(Id1, Id2).

generate_instance_id_aws_test() ->
    Id = flurm_cloud_scaling:generate_instance_id(aws),
    ?assert(is_binary(Id)),
    ?assertMatch(<<"i-", _/binary>>, Id).

generate_instance_id_gcp_test() ->
    Id = flurm_cloud_scaling:generate_instance_id(gcp),
    ?assert(is_binary(Id)),
    ?assertMatch(<<"gce-", _/binary>>, Id).

generate_instance_id_azure_test() ->
    Id = flurm_cloud_scaling:generate_instance_id(azure),
    ?assert(is_binary(Id)),
    ?assertMatch(<<"azure-", _/binary>>, Id).

generate_instance_id_generic_test() ->
    Id = flurm_cloud_scaling:generate_instance_id(generic),
    ?assert(is_binary(Id)),
    ?assertMatch(<<"node-", _/binary>>, Id).

%%====================================================================
%% Cost Functions Tests
%%====================================================================

default_instance_costs_test() ->
    Costs = flurm_cloud_scaling:default_instance_costs(),
    ?assert(is_map(Costs)),
    %% Check some known instance types
    ?assert(maps:is_key(<<"t3.micro">>, Costs)),
    ?assert(maps:is_key(<<"c5.xlarge">>, Costs)),
    ?assert(maps:is_key(<<"m5.large">>, Costs)),
    %% Check values are positive floats
    ?assert(maps:get(<<"t3.micro">>, Costs) > 0),
    ?assert(maps:get(<<"c5.xlarge">>, Costs) > 0).

%%====================================================================
%% Date/Time Formatting Tests
%%====================================================================

date_to_binary_test() ->
    Date = {2024, 1, 15},
    ?assertEqual(<<"2024-01-15">>, flurm_cloud_scaling:date_to_binary(Date)).

date_to_binary_single_digit_test() ->
    Date = {2024, 3, 5},
    ?assertEqual(<<"2024-03-05">>, flurm_cloud_scaling:date_to_binary(Date)).

month_to_binary_test() ->
    Date = {2024, 12, 25},
    ?assertEqual(<<"2024-12">>, flurm_cloud_scaling:month_to_binary(Date)).

month_to_binary_single_digit_test() ->
    Date = {2024, 1, 1},
    ?assertEqual(<<"2024-01">>, flurm_cloud_scaling:month_to_binary(Date)).

%%====================================================================
%% Query String Building Tests
%%====================================================================

build_query_string_empty_test() ->
    ?assertEqual(<<>>, flurm_cloud_scaling:build_query_string([])).

build_query_string_single_param_test() ->
    Params = [{<<"Action">>, <<"RunInstances">>}],
    Result = flurm_cloud_scaling:build_query_string(Params),
    ?assert(is_binary(Result)),
    ?assertMatch(<<"Action=RunInstances">>, Result).

build_query_string_multiple_params_test() ->
    Params = [
        {<<"Action">>, <<"RunInstances">>},
        {<<"Version">>, <<"2016-11-15">>}
    ],
    Result = flurm_cloud_scaling:build_query_string(Params),
    ?assert(is_binary(Result)),
    %% Should contain both params with &
    ?assert(binary:match(Result, <<"&">>) =/= nomatch).

%%====================================================================
%% URI Encoding Tests
%%====================================================================

uri_encode_simple_test() ->
    Result = flurm_cloud_scaling:uri_encode(<<"hello">>),
    ?assertEqual(<<"hello">>, Result).

uri_encode_special_chars_test() ->
    Result = flurm_cloud_scaling:uri_encode(<<"hello world">>),
    %% Space should be encoded
    ?assert(binary:match(Result, <<" ">>) =:= nomatch).

uri_encode_list_test() ->
    Result = flurm_cloud_scaling:uri_encode("hello"),
    ?assert(is_binary(Result)).

%%====================================================================
%% Host Extraction Tests
%%====================================================================

extract_host_https_test() ->
    Url = <<"https://ec2.us-east-1.amazonaws.com/api">>,
    Host = flurm_cloud_scaling:extract_host(Url),
    ?assertEqual(<<"ec2.us-east-1.amazonaws.com">>, Host).

extract_host_http_test() ->
    Url = <<"http://example.com/path">>,
    Host = flurm_cloud_scaling:extract_host(Url),
    ?assertEqual(<<"example.com">>, Host).

extract_host_no_path_test() ->
    Url = <<"https://example.com">>,
    Host = flurm_cloud_scaling:extract_host(Url),
    ?assertEqual(<<"example.com">>, Host).

extract_host_iolist_test() ->
    Url = ["https://", "ec2.us-west-2.amazonaws.com", "/"],
    Host = flurm_cloud_scaling:extract_host(Url),
    ?assertEqual(<<"ec2.us-west-2.amazonaws.com">>, Host).

%%====================================================================
%% Header Formatting Tests
%%====================================================================

format_canonical_headers_test() ->
    Headers = [
        {<<"host">>, <<"example.com">>},
        {<<"content-type">>, <<"application/json">>}
    ],
    Result = flurm_cloud_scaling:format_canonical_headers(Headers),
    ?assert(is_binary(Result)),
    %% Headers should be sorted alphabetically
    ?assertMatch(<<"content-type:application/json\n", _/binary>>, Result).

format_canonical_headers_empty_test() ->
    Result = flurm_cloud_scaling:format_canonical_headers([]),
    ?assertEqual(<<>>, Result).

%%====================================================================
%% Binary to Hex Conversion Tests
%%====================================================================

binary_to_hex_test() ->
    Bin = <<255, 0, 15>>,
    Hex = flurm_cloud_scaling:binary_to_hex(Bin),
    ?assert(is_binary(Hex)),
    %% Result should be uppercase hex: 255=FF, 0=00, 15=0F
    ?assertEqual(<<"FF000F">>, Hex).

binary_to_hex_empty_test() ->
    Hex = flurm_cloud_scaling:binary_to_hex(<<>>),
    ?assertEqual(<<>>, Hex).

%%====================================================================
%% Security Groups Param Building Tests
%%====================================================================

add_security_groups_empty_test() ->
    Params = [{<<"Action">>, <<"RunInstances">>}],
    Result = flurm_cloud_scaling:add_security_groups(Params, [], 1),
    ?assertEqual(Params, Result).

add_security_groups_single_test() ->
    Params = [],
    Result = flurm_cloud_scaling:add_security_groups(Params, [<<"sg-123">>], 1),
    ?assertEqual([{<<"SecurityGroupId.1">>, <<"sg-123">>}], Result).

add_security_groups_multiple_test() ->
    Params = [],
    Result = flurm_cloud_scaling:add_security_groups(Params, [<<"sg-1">>, <<"sg-2">>], 1),
    ?assertEqual(2, length(Result)).

%%====================================================================
%% Instance IDs Param Building Tests
%%====================================================================

add_instance_ids_empty_test() ->
    Params = [{<<"Action">>, <<"TerminateInstances">>}],
    Result = flurm_cloud_scaling:add_instance_ids(Params, [], 1),
    ?assertEqual(Params, Result).

add_instance_ids_single_test() ->
    Params = [],
    Result = flurm_cloud_scaling:add_instance_ids(Params, [<<"i-123">>], 1),
    ?assertEqual([{<<"InstanceId.1">>, <<"i-123">>}], Result).

add_instance_ids_multiple_test() ->
    Params = [],
    Result = flurm_cloud_scaling:add_instance_ids(Params, [<<"i-1">>, <<"i-2">>, <<"i-3">>], 1),
    ?assertEqual(3, length(Result)).

%%====================================================================
%% Format Cloud Instances Tests
%%====================================================================

format_cloud_instances_empty_test() ->
    Result = flurm_cloud_scaling:format_cloud_instances(#{}),
    ?assertEqual([], Result).
