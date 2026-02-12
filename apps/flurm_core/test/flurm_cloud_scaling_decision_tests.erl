%%%-------------------------------------------------------------------
%%% @doc Scaling Decision Logic Tests for flurm_cloud_scaling
%%%
%%% Tests the internal scaling decision logic using -ifdef(TEST)
%%% exported functions and direct handle_info/2 calls.
%%%
%%% Uses meck to mock flurm_job_registry, flurm_node_registry, and
%%% lager for the handle_info-based scaling decision tests.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaling_decision_tests).

-include_lib("eunit/include/eunit.hrl").

%% Record definitions copied from flurm_cloud_scaling source for testing
-record(scaling_policy, {
    min_nodes :: non_neg_integer(),
    max_nodes :: non_neg_integer(),
    target_pending_jobs :: non_neg_integer(),
    target_idle_nodes :: non_neg_integer(),
    scale_up_increment :: pos_integer(),
    scale_down_increment :: pos_integer(),
    cooldown_seconds :: pos_integer(),
    instance_types :: [binary()],
    spot_enabled :: boolean(),
    spot_max_price :: float(),
    idle_threshold_seconds :: pos_integer(),
    queue_depth_threshold :: pos_integer()
}).

-record(scaling_action, {
    id :: binary(),
    type :: scale_up | scale_down,
    requested_count :: pos_integer(),
    actual_count :: non_neg_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer() | undefined,
    status :: pending | in_progress | completed | failed,
    error :: binary() | undefined,
    instance_ids :: [binary()]
}).

-record(cloud_instance, {
    instance_id :: binary(),
    provider :: atom(),
    instance_type :: binary(),
    launch_time :: non_neg_integer(),
    state :: pending | running | stopping | terminated,
    public_ip :: binary() | undefined,
    private_ip :: binary() | undefined,
    node_name :: binary() | undefined,
    last_job_time :: non_neg_integer(),
    hourly_cost :: float()
}).

-record(cost_tracking, {
    total_instance_hours :: float(),
    total_cost :: float(),
    budget_limit :: float(),
    budget_alert_threshold :: float(),
    cost_per_hour :: map(),
    daily_costs :: map(),
    monthly_costs :: map()
}).

-record(state, {
    enabled :: boolean(),
    provider :: atom() | undefined,
    provider_config :: map(),
    policy :: #scaling_policy{},
    current_cloud_nodes :: non_neg_integer(),
    pending_actions :: [#scaling_action{}],
    action_history :: [#scaling_action{}],
    last_scale_time :: non_neg_integer(),
    check_timer :: reference() | undefined,
    idle_check_timer :: reference() | undefined,
    cloud_instances :: map(),
    cost_tracking :: #cost_tracking{}
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

default_policy() ->
    #scaling_policy{
        min_nodes = 0,
        max_nodes = 100,
        target_pending_jobs = 10,
        target_idle_nodes = 2,
        scale_up_increment = 5,
        scale_down_increment = 1,
        cooldown_seconds = 300,
        instance_types = [<<"c5.xlarge">>],
        spot_enabled = false,
        spot_max_price = 0.0,
        idle_threshold_seconds = 300,
        queue_depth_threshold = 10
    }.

default_cost_tracking() ->
    #cost_tracking{
        total_instance_hours = 0.0,
        total_cost = 0.0,
        budget_limit = 0.0,
        budget_alert_threshold = 0.8,
        cost_per_hour = #{
            <<"c5.xlarge">> => 0.17,
            <<"t3.medium">> => 0.0416
        },
        daily_costs = #{},
        monthly_costs = #{}
    }.

default_test_state() ->
    #state{
        enabled = false,
        provider = undefined,
        provider_config = #{},
        policy = default_policy(),
        current_cloud_nodes = 0,
        pending_actions = [],
        action_history = [],
        last_scale_time = 0,
        check_timer = undefined,
        idle_check_timer = undefined,
        cloud_instances = #{},
        cost_tracking = default_cost_tracking()
    }.

aws_configured_state() ->
    State = default_test_state(),
    State#state{
        provider = aws,
        provider_config = #{
            region => <<"us-east-1">>,
            access_key_id => <<"test-key">>,
            secret_access_key => <<"test-secret">>
        }
    }.

%%====================================================================
%% Setup / Teardown for mocked tests
%%====================================================================

setup_mocks() ->
    %% Mock lager to suppress log output during tests
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),

    %% Mock flurm_job_registry
    meck:new(flurm_job_registry, [non_strict, no_link]),
    meck:expect(flurm_job_registry, count_by_state,
                fun() -> #{pending => 0, running => 0} end),

    %% Mock flurm_node_registry
    meck:new(flurm_node_registry, [non_strict, no_link]),
    meck:expect(flurm_node_registry, count_by_state,
                fun() -> #{up => 0} end),
    meck:expect(flurm_node_registry, count_nodes, fun() -> 0 end),
    ok.

teardown_mocks(_) ->
    meck:unload(lager),
    meck:unload(flurm_job_registry),
    meck:unload(flurm_node_registry),
    ok.

%%====================================================================
%% AWS Sig V4 Helper Tests (exported under -ifdef(TEST))
%%====================================================================

build_query_string_test_() ->
    {"build_query_string/1 tests", [
        {"builds query string from key-value tuples",
         fun() ->
             Result = flurm_cloud_scaling:build_query_string([
                 {<<"Action">>, <<"RunInstances">>},
                 {<<"Version">>, <<"2016-11-15">>}
             ]),
             ?assert(is_binary(Result)),
             %% Should contain both key=value pairs joined by &
             ?assertNotEqual(nomatch, binary:match(Result, <<"Action=RunInstances">>)),
             ?assertNotEqual(nomatch, binary:match(Result, <<"Version=2016-11-15">>)),
             ?assertNotEqual(nomatch, binary:match(Result, <<"&">>))
         end},

        {"builds empty query string from empty list",
         fun() ->
             Result = flurm_cloud_scaling:build_query_string([]),
             ?assertEqual(<<>>, Result)
         end},

        {"builds single-param query string without ampersand",
         fun() ->
             Result = flurm_cloud_scaling:build_query_string([
                 {<<"Key">>, <<"Value">>}
             ]),
             ?assertEqual(nomatch, binary:match(Result, <<"&">>)),
             ?assertNotEqual(nomatch, binary:match(Result, <<"Key=Value">>))
         end}
    ]}.

uri_encode_test_() ->
    {"uri_encode/1 tests", [
        {"encodes binary value",
         fun() ->
             Result = flurm_cloud_scaling:uri_encode(<<"hello world">>),
             ?assert(is_binary(Result) orelse is_list(Result)),
             %% Space should be encoded (either as + or %20)
             Str = iolist_to_binary([Result]),
             ?assertEqual(nomatch, binary:match(Str, <<" ">>))
         end},

        {"encodes list value",
         fun() ->
             Result = flurm_cloud_scaling:uri_encode("hello world"),
             ?assert(is_binary(Result) orelse is_list(Result)),
             Str = iolist_to_binary([Result]),
             ?assertEqual(nomatch, binary:match(Str, <<" ">>))
         end},

        {"encodes plain alphanumeric string unchanged",
         fun() ->
             Result = flurm_cloud_scaling:uri_encode(<<"simple">>),
             Str = iolist_to_binary([Result]),
             ?assertNotEqual(nomatch, binary:match(Str, <<"simple">>))
         end},

        {"encodes special characters",
         fun() ->
             Result = flurm_cloud_scaling:uri_encode(<<"a/b=c&d">>),
             Str = iolist_to_binary([Result]),
             %% Raw / = & should not appear unencoded
             ?assertEqual(nomatch, binary:match(Str, <<"/">>)),
             ?assertEqual(nomatch, binary:match(Str, <<"&">>))
         end}
    ]}.

extract_host_test_() ->
    {"extract_host/1 tests", [
        {"extracts host from HTTPS URL with trailing slash",
         fun() ->
             Result = flurm_cloud_scaling:extract_host(
                 "https://ec2.us-east-1.amazonaws.com/"),
             ?assertEqual(<<"ec2.us-east-1.amazonaws.com">>, Result)
         end},

        {"extracts host from HTTPS URL without trailing slash",
         fun() ->
             Result = flurm_cloud_scaling:extract_host(
                 "https://ec2.us-west-2.amazonaws.com"),
             ?assertEqual(<<"ec2.us-west-2.amazonaws.com">>, Result)
         end},

        {"extracts host from HTTP URL",
         fun() ->
             Result = flurm_cloud_scaling:extract_host(
                 "http://example.com/path/to/resource"),
             ?assertEqual(<<"example.com">>, Result)
         end},

        {"extracts host from binary URL",
         fun() ->
             Result = flurm_cloud_scaling:extract_host(
                 <<"https://compute.googleapis.com/">>),
             ?assertEqual(<<"compute.googleapis.com">>, Result)
         end},

        {"handles iolist URL",
         fun() ->
             Result = flurm_cloud_scaling:extract_host(
                 [<<"https://ec2.">>, <<"eu-west-1">>, <<".amazonaws.com/">>]),
             ?assertEqual(<<"ec2.eu-west-1.amazonaws.com">>, Result)
         end}
    ]}.

format_canonical_headers_test_() ->
    {"format_canonical_headers/1 tests", [
        {"sorts headers and formats as key:value\\n",
         fun() ->
             Headers = [
                 {<<"x-amz-date">>, <<"20260101T000000Z">>},
                 {<<"content-type">>, <<"application/x-www-form-urlencoded">>},
                 {<<"host">>, <<"ec2.us-east-1.amazonaws.com">>}
             ],
             Result = flurm_cloud_scaling:format_canonical_headers(Headers),
             ?assert(is_binary(Result)),
             %% Headers should be sorted alphabetically by key
             %% content-type < host < x-amz-date
             ContentPos = binary:match(Result, <<"content-type:">>),
             HostPos = binary:match(Result, <<"host:">>),
             AmzDatePos = binary:match(Result, <<"x-amz-date:">>),
             ?assertNotEqual(nomatch, ContentPos),
             ?assertNotEqual(nomatch, HostPos),
             ?assertNotEqual(nomatch, AmzDatePos),
             {ContentStart, _} = ContentPos,
             {HostStart, _} = HostPos,
             {AmzDateStart, _} = AmzDatePos,
             ?assert(ContentStart < HostStart),
             ?assert(HostStart < AmzDateStart)
         end},

        {"each header line ends with newline",
         fun() ->
             Headers = [
                 {<<"host">>, <<"example.com">>},
                 {<<"content-type">>, <<"text/plain">>}
             ],
             Result = flurm_cloud_scaling:format_canonical_headers(Headers),
             %% Should end with a newline
             Size = byte_size(Result),
             ?assertEqual(<<"\n">>, binary:part(Result, Size - 1, 1)),
             %% Should contain exactly 2 newlines (one per header)
             Splits = binary:split(Result, <<"\n">>, [global]),
             %% split on \n gives N+1 parts for N newlines; last part is empty
             ?assertEqual(3, length(Splits))
         end},

        {"handles single header",
         fun() ->
             Headers = [{<<"host">>, <<"example.com">>}],
             Result = flurm_cloud_scaling:format_canonical_headers(Headers),
             ?assertEqual(<<"host:example.com\n">>, Result)
         end}
    ]}.

binary_to_hex_test_() ->
    {"binary_to_hex/1 tests", [
        {"converts empty binary to empty binary",
         fun() ->
             Result = flurm_cloud_scaling:binary_to_hex(<<>>),
             ?assertEqual(<<>>, Result)
         end},

        {"converts known byte to hex",
         fun() ->
             %% 0xFF = 255 = "FF"
             Result = flurm_cloud_scaling:binary_to_hex(<<255>>),
             ?assertEqual(<<"FF">>, Result)
         end},

        {"converts zero byte to hex",
         fun() ->
             Result = flurm_cloud_scaling:binary_to_hex(<<0>>),
             ?assertEqual(<<"00">>, Result)
         end},

        {"converts multi-byte binary to hex string",
         fun() ->
             %% <<1, 2, 3>> = "010203"
             Result = flurm_cloud_scaling:binary_to_hex(<<1, 2, 3>>),
             Expected = <<"010203">>,
             ?assertEqual(Expected, Result)
         end},

        {"converts SHA-256 hash to 64-char hex string",
         fun() ->
             Hash = crypto:hash(sha256, <<"test">>),
             Result = flurm_cloud_scaling:binary_to_hex(Hash),
             ?assertEqual(64, byte_size(Result))
         end}
    ]}.

add_security_groups_test_() ->
    {"add_security_groups/3 tests", [
        {"returns params unchanged when no security groups",
         fun() ->
             Params = [{<<"Action">>, <<"RunInstances">>}],
             Result = flurm_cloud_scaling:add_security_groups(Params, [], 1),
             ?assertEqual(Params, Result)
         end},

        {"adds single security group",
         fun() ->
             Params = [{<<"Action">>, <<"RunInstances">>}],
             Result = flurm_cloud_scaling:add_security_groups(
                 Params, [<<"sg-12345">>], 1),
             %% Result should have the SG prepended
             ?assertEqual(2, length(Result)),
             %% Find the security group param
             SGParam = lists:keyfind(<<"SecurityGroupId.1">>, 1, Result),
             ?assertNotEqual(false, SGParam),
             {_, SGValue} = SGParam,
             ?assertEqual(<<"sg-12345">>, SGValue)
         end},

        {"adds multiple security groups with incrementing indices",
         fun() ->
             Params = [],
             Result = flurm_cloud_scaling:add_security_groups(
                 Params, [<<"sg-aaa">>, <<"sg-bbb">>, <<"sg-ccc">>], 1),
             ?assertEqual(3, length(Result)),
             %% All three should be present with correct indices
             ?assertNotEqual(false, lists:keyfind(<<"SecurityGroupId.1">>, 1, Result)),
             ?assertNotEqual(false, lists:keyfind(<<"SecurityGroupId.2">>, 1, Result)),
             ?assertNotEqual(false, lists:keyfind(<<"SecurityGroupId.3">>, 1, Result))
         end},

        {"starts numbering from given index",
         fun() ->
             Params = [],
             Result = flurm_cloud_scaling:add_security_groups(
                 Params, [<<"sg-first">>], 5),
             SGParam = lists:keyfind(<<"SecurityGroupId.5">>, 1, Result),
             ?assertNotEqual(false, SGParam),
             {_, SGValue} = SGParam,
             ?assertEqual(<<"sg-first">>, SGValue)
         end}
    ]}.

add_instance_ids_test_() ->
    {"add_instance_ids/3 tests", [
        {"returns params unchanged when no instance IDs",
         fun() ->
             Params = [{<<"Action">>, <<"TerminateInstances">>}],
             Result = flurm_cloud_scaling:add_instance_ids(Params, [], 1),
             ?assertEqual(Params, Result)
         end},

        {"adds single instance ID",
         fun() ->
             Params = [{<<"Action">>, <<"TerminateInstances">>}],
             Result = flurm_cloud_scaling:add_instance_ids(
                 Params, [<<"i-abcdef">>], 1),
             ?assertEqual(2, length(Result)),
             IdParam = lists:keyfind(<<"InstanceId.1">>, 1, Result),
             ?assertNotEqual(false, IdParam),
             {_, IdValue} = IdParam,
             ?assertEqual(<<"i-abcdef">>, IdValue)
         end},

        {"adds multiple instance IDs with incrementing indices",
         fun() ->
             Params = [],
             Result = flurm_cloud_scaling:add_instance_ids(
                 Params, [<<"i-111">>, <<"i-222">>, <<"i-333">>], 1),
             ?assertEqual(3, length(Result)),
             ?assertNotEqual(false, lists:keyfind(<<"InstanceId.1">>, 1, Result)),
             ?assertNotEqual(false, lists:keyfind(<<"InstanceId.2">>, 1, Result)),
             ?assertNotEqual(false, lists:keyfind(<<"InstanceId.3">>, 1, Result))
         end},

        {"starts numbering from given index",
         fun() ->
             Params = [],
             Result = flurm_cloud_scaling:add_instance_ids(
                 Params, [<<"i-first">>], 3),
             IdParam = lists:keyfind(<<"InstanceId.3">>, 1, Result),
             ?assertNotEqual(false, IdParam)
         end}
    ]}.

%%====================================================================
%% Cost Tracking Tests (exported under -ifdef(TEST))
%%====================================================================

default_instance_costs_test_() ->
    {"default_instance_costs/0 tests", [
        {"returns a map",
         fun() ->
             Result = flurm_cloud_scaling:default_instance_costs(),
             ?assert(is_map(Result))
         end},

        {"contains known AWS instance types",
         fun() ->
             Costs = flurm_cloud_scaling:default_instance_costs(),
             ?assert(maps:is_key(<<"t3.micro">>, Costs)),
             ?assert(maps:is_key(<<"c5.xlarge">>, Costs)),
             ?assert(maps:is_key(<<"m5.large">>, Costs)),
             ?assert(maps:is_key(<<"r5.large">>, Costs)),
             ?assert(maps:is_key(<<"p3.2xlarge">>, Costs)),
             ?assert(maps:is_key(<<"g4dn.xlarge">>, Costs))
         end},

        {"all costs are positive floats",
         fun() ->
             Costs = flurm_cloud_scaling:default_instance_costs(),
             maps:fold(
                 fun(_Type, Cost, _Acc) ->
                     ?assert(is_float(Cost)),
                     ?assert(Cost > 0)
                 end,
                 ok,
                 Costs
             )
         end},

        {"GPU instances cost more than general purpose",
         fun() ->
             Costs = flurm_cloud_scaling:default_instance_costs(),
             T3Micro = maps:get(<<"t3.micro">>, Costs),
             P3_2xl = maps:get(<<"p3.2xlarge">>, Costs),
             ?assert(P3_2xl > T3Micro)
         end}
    ]}.

get_instance_cost_test_() ->
    {"get_instance_cost/2 tests", [
        {"returns cost for known instance type",
         fun() ->
             CT = default_cost_tracking(),
             Cost = flurm_cloud_scaling:get_instance_cost(<<"c5.xlarge">>, CT),
             ?assertEqual(0.17, Cost)
         end},

        {"returns default cost for unknown instance type",
         fun() ->
             CT = default_cost_tracking(),
             Cost = flurm_cloud_scaling:get_instance_cost(<<"x99.mega">>, CT),
             %% Default is $0.10/hr as per source
             ?assertEqual(0.10, Cost)
         end},

        {"returns cost for another known type",
         fun() ->
             CT = default_cost_tracking(),
             Cost = flurm_cloud_scaling:get_instance_cost(<<"t3.medium">>, CT),
             ?assertEqual(0.0416, Cost)
         end}
    ]}.

calculate_budget_status_test_() ->
    {"calculate_budget_status/1 tests", [
        {"returns unlimited when budget is 0.0",
         fun() ->
             CT = #cost_tracking{
                 total_instance_hours = 10.0,
                 total_cost = 5.0,
                 budget_limit = 0.0,
                 budget_alert_threshold = 0.8,
                 cost_per_hour = #{},
                 daily_costs = #{},
                 monthly_costs = #{}
             },
             Status = flurm_cloud_scaling:calculate_budget_status(CT),
             ?assertEqual(0.0, maps:get(budget_limit, Status)),
             ?assertEqual(5.0, maps:get(total_cost, Status)),
             ?assertEqual(0.0, maps:get(percentage_used, Status)),
             ?assertEqual(unlimited, maps:get(remaining, Status)),
             ?assertEqual(0.8, maps:get(alert_threshold, Status))
         end},

        {"returns percentage used when budget is set",
         fun() ->
             CT = #cost_tracking{
                 total_instance_hours = 50.0,
                 total_cost = 200.0,
                 budget_limit = 1000.0,
                 budget_alert_threshold = 0.8,
                 cost_per_hour = #{},
                 daily_costs = #{},
                 monthly_costs = #{}
             },
             Status = flurm_cloud_scaling:calculate_budget_status(CT),
             ?assertEqual(1000.0, maps:get(budget_limit, Status)),
             ?assertEqual(200.0, maps:get(total_cost, Status)),
             ?assertEqual(20.0, maps:get(percentage_used, Status)),
             ?assertEqual(800.0, maps:get(remaining, Status)),
             %% alert_threshold is multiplied by 100 in the function
             ?assertEqual(80.0, maps:get(alert_threshold, Status)),
             ?assertEqual(false, maps:get(over_budget, Status))
         end},

        {"reports over budget when exceeded",
         fun() ->
             CT = #cost_tracking{
                 total_instance_hours = 200.0,
                 total_cost = 1500.0,
                 budget_limit = 1000.0,
                 budget_alert_threshold = 0.8,
                 cost_per_hour = #{},
                 daily_costs = #{},
                 monthly_costs = #{}
             },
             Status = flurm_cloud_scaling:calculate_budget_status(CT),
             ?assertEqual(true, maps:get(over_budget, Status)),
             ?assertEqual(150.0, maps:get(percentage_used, Status)),
             ?assertEqual(0, maps:get(remaining, Status))
         end}
    ]}.

%%====================================================================
%% Policy / Utility Tests (exported under -ifdef(TEST))
%%====================================================================

date_to_binary_test_() ->
    {"date_to_binary/1 tests", [
        {"formats date as YYYY-MM-DD",
         fun() ->
             Result = flurm_cloud_scaling:date_to_binary({2026, 2, 11}),
             ?assertEqual(<<"2026-02-11">>, Result)
         end},

        {"pads single-digit month and day",
         fun() ->
             Result = flurm_cloud_scaling:date_to_binary({2025, 1, 5}),
             ?assertEqual(<<"2025-01-05">>, Result)
         end},

        {"handles end of year date",
         fun() ->
             Result = flurm_cloud_scaling:date_to_binary({2026, 12, 31}),
             ?assertEqual(<<"2026-12-31">>, Result)
         end}
    ]}.

month_to_binary_test_() ->
    {"month_to_binary/1 tests", [
        {"formats date as YYYY-MM (ignoring day)",
         fun() ->
             Result = flurm_cloud_scaling:month_to_binary({2026, 2, 11}),
             ?assertEqual(<<"2026-02">>, Result)
         end},

        {"pads single-digit month",
         fun() ->
             Result = flurm_cloud_scaling:month_to_binary({2025, 3, 15}),
             ?assertEqual(<<"2025-03">>, Result)
         end},

        {"handles December",
         fun() ->
             Result = flurm_cloud_scaling:month_to_binary({2026, 12, 1}),
             ?assertEqual(<<"2026-12">>, Result)
         end}
    ]}.

generate_instance_id_test_() ->
    {"generate_instance_id/1 tests", [
        {"generates AWS instance ID with i- prefix",
         fun() ->
             Result = flurm_cloud_scaling:generate_instance_id(aws),
             ?assert(is_binary(Result)),
             ?assertNotEqual(nomatch, binary:match(Result, <<"i-">>)),
             %% Prefix should be at position 0
             {0, 2} = binary:match(Result, <<"i-">>)
         end},

        {"generates GCP instance ID with gce- prefix",
         fun() ->
             Result = flurm_cloud_scaling:generate_instance_id(gcp),
             ?assert(is_binary(Result)),
             {0, 4} = binary:match(Result, <<"gce-">>)
         end},

        {"generates Azure instance ID with azure- prefix",
         fun() ->
             Result = flurm_cloud_scaling:generate_instance_id(azure),
             ?assert(is_binary(Result)),
             {0, 6} = binary:match(Result, <<"azure-">>)
         end},

        {"generates generic instance ID with node- prefix",
         fun() ->
             Result = flurm_cloud_scaling:generate_instance_id(generic),
             ?assert(is_binary(Result)),
             {0, 5} = binary:match(Result, <<"node-">>)
         end},

        {"generates unique IDs across calls",
         fun() ->
             Ids = [flurm_cloud_scaling:generate_instance_id(aws)
                    || _ <- lists:seq(1, 10)],
             UniqueIds = lists:usort(Ids),
             %% All should be unique (very high probability)
             ?assertEqual(length(Ids), length(UniqueIds))
         end}
    ]}.

generate_action_id_test_() ->
    {"generate_action_id/0 tests", [
        {"returns binary",
         fun() ->
             Result = flurm_cloud_scaling:generate_action_id(),
             ?assert(is_binary(Result))
         end},

        {"has scale- prefix",
         fun() ->
             Result = flurm_cloud_scaling:generate_action_id(),
             {0, 6} = binary:match(Result, <<"scale-">>)
         end},

        {"generates unique IDs",
         fun() ->
             Ids = [flurm_cloud_scaling:generate_action_id()
                    || _ <- lists:seq(1, 10)],
             UniqueIds = lists:usort(Ids),
             ?assertEqual(length(Ids), length(UniqueIds))
         end}
    ]}.

format_policy_test_() ->
    {"format_policy/1 tests", [
        {"returns a map from scaling_policy record",
         fun() ->
             Policy = default_policy(),
             Result = flurm_cloud_scaling:format_policy(Policy),
             ?assert(is_map(Result)),
             ?assertEqual(0, maps:get(min_nodes, Result)),
             ?assertEqual(100, maps:get(max_nodes, Result)),
             ?assertEqual(10, maps:get(target_pending_jobs, Result)),
             ?assertEqual(2, maps:get(target_idle_nodes, Result)),
             ?assertEqual(5, maps:get(scale_up_increment, Result)),
             ?assertEqual(1, maps:get(scale_down_increment, Result)),
             ?assertEqual(300, maps:get(cooldown_seconds, Result)),
             ?assertEqual([<<"c5.xlarge">>], maps:get(instance_types, Result)),
             ?assertEqual(false, maps:get(spot_enabled, Result)),
             ?assertEqual(0.0, maps:get(spot_max_price, Result)),
             ?assertEqual(300, maps:get(idle_threshold_seconds, Result)),
             ?assertEqual(10, maps:get(queue_depth_threshold, Result))
         end},

        {"contains all 12 expected keys",
         fun() ->
             Policy = default_policy(),
             Result = flurm_cloud_scaling:format_policy(Policy),
             ExpectedKeys = [min_nodes, max_nodes, target_pending_jobs,
                             target_idle_nodes, scale_up_increment,
                             scale_down_increment, cooldown_seconds,
                             instance_types, spot_enabled, spot_max_price,
                             idle_threshold_seconds, queue_depth_threshold],
             lists:foreach(
                 fun(Key) ->
                     ?assert(maps:is_key(Key, Result))
                 end,
                 ExpectedKeys
             ),
             ?assertEqual(12, maps:size(Result))
         end}
    ]}.

update_policy_test_() ->
    {"update_policy/2 tests", [
        {"merges map values into policy record",
         fun() ->
             Policy = default_policy(),
             Updated = flurm_cloud_scaling:update_policy(Policy,
                 #{min_nodes => 5, max_nodes => 50}),
             ?assertEqual(5, Updated#scaling_policy.min_nodes),
             ?assertEqual(50, Updated#scaling_policy.max_nodes),
             %% Unchanged fields should remain
             ?assertEqual(10, Updated#scaling_policy.target_pending_jobs),
             ?assertEqual(300, Updated#scaling_policy.cooldown_seconds)
         end},

        {"preserves all fields when given empty map",
         fun() ->
             Policy = default_policy(),
             Updated = flurm_cloud_scaling:update_policy(Policy, #{}),
             ?assertEqual(Policy#scaling_policy.min_nodes,
                          Updated#scaling_policy.min_nodes),
             ?assertEqual(Policy#scaling_policy.max_nodes,
                          Updated#scaling_policy.max_nodes),
             ?assertEqual(Policy#scaling_policy.cooldown_seconds,
                          Updated#scaling_policy.cooldown_seconds),
             ?assertEqual(Policy#scaling_policy.instance_types,
                          Updated#scaling_policy.instance_types)
         end},

        {"updates spot settings",
         fun() ->
             Policy = default_policy(),
             Updated = flurm_cloud_scaling:update_policy(Policy,
                 #{spot_enabled => true, spot_max_price => 0.05}),
             ?assertEqual(true, Updated#scaling_policy.spot_enabled),
             ?assertEqual(0.05, Updated#scaling_policy.spot_max_price)
         end},

        {"updates instance_types list",
         fun() ->
             Policy = default_policy(),
             NewTypes = [<<"m5.large">>, <<"m5.xlarge">>],
             Updated = flurm_cloud_scaling:update_policy(Policy,
                 #{instance_types => NewTypes}),
             ?assertEqual(NewTypes, Updated#scaling_policy.instance_types)
         end},

        {"updates idle_threshold_seconds and queue_depth_threshold",
         fun() ->
             Policy = default_policy(),
             Updated = flurm_cloud_scaling:update_policy(Policy,
                 #{idle_threshold_seconds => 600,
                   queue_depth_threshold => 20}),
             ?assertEqual(600, Updated#scaling_policy.idle_threshold_seconds),
             ?assertEqual(20, Updated#scaling_policy.queue_depth_threshold)
         end}
    ]}.

%%====================================================================
%% Additional exported function tests
%%====================================================================

validate_provider_config_test_() ->
    {"validate_provider_config/2 tests", [
        {"validates AWS config with all required keys",
         fun() ->
             Config = #{region => <<"us-east-1">>,
                        access_key_id => <<"AKIA">>,
                        secret_access_key => <<"secret">>},
             ?assertEqual(ok,
                 flurm_cloud_scaling:validate_provider_config(aws, Config))
         end},

        {"rejects AWS config missing key",
         fun() ->
             Config = #{region => <<"us-east-1">>},
             ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(aws, Config))
         end},

        {"validates GCP config",
         fun() ->
             Config = #{project_id => <<"proj">>,
                        zone => <<"us-central1-a">>,
                        credentials_file => <<"/creds.json">>},
             ?assertEqual(ok,
                 flurm_cloud_scaling:validate_provider_config(gcp, Config))
         end},

        {"rejects GCP config missing key",
         fun() ->
             Config = #{project_id => <<"proj">>},
             ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(gcp, Config))
         end},

        {"validates Azure config",
         fun() ->
             Config = #{subscription_id => <<"sub">>,
                        resource_group => <<"rg">>,
                        tenant_id => <<"tid">>},
             ?assertEqual(ok,
                 flurm_cloud_scaling:validate_provider_config(azure, Config))
         end},

        {"rejects Azure config missing key",
         fun() ->
             Config = #{subscription_id => <<"sub">>},
             ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(azure, Config))
         end},

        {"validates generic config",
         fun() ->
             Config = #{scale_up_webhook => <<"https://up">>,
                        scale_down_webhook => <<"https://down">>},
             ?assertEqual(ok,
                 flurm_cloud_scaling:validate_provider_config(generic, Config))
         end},

        {"rejects generic config missing key",
         fun() ->
             Config = #{scale_up_webhook => <<"https://up">>},
             ?assertEqual({error, missing_required_config},
                 flurm_cloud_scaling:validate_provider_config(generic, Config))
         end},

        {"rejects unknown provider",
         fun() ->
             ?assertEqual({error, unknown_provider},
                 flurm_cloud_scaling:validate_provider_config(openstack, #{}))
         end}
    ]}.

collect_stats_test_() ->
    {"collect_stats/1 tests", [
        {"returns stats map for empty state",
         fun() ->
             State = default_test_state(),
             Stats = flurm_cloud_scaling:collect_stats(State),
             ?assert(is_map(Stats)),
             ?assertEqual(false, maps:get(enabled, Stats)),
             ?assertEqual(0, maps:get(current_cloud_nodes, Stats)),
             ?assertEqual(0, maps:get(pending_actions, Stats)),
             ?assertEqual(0, maps:get(completed_scale_ups, Stats)),
             ?assertEqual(0, maps:get(completed_scale_downs, Stats)),
             ?assertEqual(0, maps:get(failed_actions, Stats)),
             ?assertEqual(0, maps:get(total_instances_launched, Stats)),
             ?assertEqual(0, maps:get(total_instances_terminated, Stats))
         end},

        {"counts completed actions correctly",
         fun() ->
             State = aws_configured_state(),
             History = [
                 #scaling_action{id = <<"a1">>, type = scale_up,
                     requested_count = 5, actual_count = 5,
                     start_time = 100, end_time = 110,
                     status = completed, instance_ids = []},
                 #scaling_action{id = <<"a2">>, type = scale_up,
                     requested_count = 3, actual_count = 3,
                     start_time = 200, end_time = 210,
                     status = completed, instance_ids = []},
                 #scaling_action{id = <<"a3">>, type = scale_down,
                     requested_count = 2, actual_count = 2,
                     start_time = 300, end_time = 310,
                     status = completed, instance_ids = []},
                 #scaling_action{id = <<"a4">>, type = scale_up,
                     requested_count = 1, actual_count = 0,
                     start_time = 400, end_time = 410,
                     status = failed, error = <<"err">>, instance_ids = []}
             ],
             StateWithHistory = State#state{action_history = History},
             Stats = flurm_cloud_scaling:collect_stats(StateWithHistory),
             ?assertEqual(2, maps:get(completed_scale_ups, Stats)),
             ?assertEqual(1, maps:get(completed_scale_downs, Stats)),
             ?assertEqual(1, maps:get(failed_actions, Stats)),
             ?assertEqual(8, maps:get(total_instances_launched, Stats)),
             ?assertEqual(2, maps:get(total_instances_terminated, Stats))
         end}
    ]}.

format_instance_test_() ->
    {"format_instance/1 tests", [
        {"formats cloud_instance record to map",
         fun() ->
             Now = erlang:system_time(second),
             Instance = #cloud_instance{
                 instance_id = <<"i-test123">>,
                 provider = aws,
                 instance_type = <<"c5.xlarge">>,
                 launch_time = Now - 3600,
                 state = running,
                 public_ip = <<"1.2.3.4">>,
                 private_ip = <<"10.0.0.1">>,
                 node_name = <<"node1@host">>,
                 last_job_time = Now - 60,
                 hourly_cost = 0.17
             },
             Result = flurm_cloud_scaling:format_instance(Instance),
             ?assert(is_map(Result)),
             ?assertEqual(<<"i-test123">>, maps:get(instance_id, Result)),
             ?assertEqual(aws, maps:get(provider, Result)),
             ?assertEqual(<<"c5.xlarge">>, maps:get(instance_type, Result)),
             ?assertEqual(running, maps:get(state, Result)),
             ?assertEqual(<<"1.2.3.4">>, maps:get(public_ip, Result)),
             ?assertEqual(<<"10.0.0.1">>, maps:get(private_ip, Result)),
             ?assertEqual(<<"node1@host">>, maps:get(node_name, Result)),
             ?assertEqual(0.17, maps:get(hourly_cost, Result)),
             ?assert(maps:is_key(running_hours, Result)),
             RunningHours = maps:get(running_hours, Result),
             ?assert(is_float(RunningHours)),
             %% Should be approximately 1.0 hour
             ?assert(RunningHours > 0.9),
             ?assert(RunningHours < 1.1)
         end}
    ]}.

format_cloud_instances_test_() ->
    {"format_cloud_instances/1 tests", [
        {"returns empty list for empty map",
         fun() ->
             Result = flurm_cloud_scaling:format_cloud_instances(#{}),
             ?assertEqual([], Result)
         end},

        {"returns list of formatted instances",
         fun() ->
             Now = erlang:system_time(second),
             Instances = #{
                 <<"i-1">> => #cloud_instance{
                     instance_id = <<"i-1">>,
                     provider = aws,
                     instance_type = <<"c5.xlarge">>,
                     launch_time = Now,
                     state = running,
                     last_job_time = Now,
                     hourly_cost = 0.17
                 },
                 <<"i-2">> => #cloud_instance{
                     instance_id = <<"i-2">>,
                     provider = aws,
                     instance_type = <<"t3.medium">>,
                     launch_time = Now,
                     state = pending,
                     last_job_time = Now,
                     hourly_cost = 0.0416
                 }
             },
             Result = flurm_cloud_scaling:format_cloud_instances(Instances),
             ?assertEqual(2, length(Result)),
             ?assert(lists:all(fun is_map/1, Result))
         end}
    ]}.

%%====================================================================
%% Scaling Decision via handle_info (mocked tests)
%%====================================================================

handle_info_check_scaling_disabled_test_() ->
    {"handle_info(check_scaling, State) when disabled",
     {setup,
      fun setup_mocks/0,
      fun teardown_mocks/1,
      fun(_) ->
          [{"returns unchanged state when enabled=false",
            fun() ->
                State = default_test_state(),
                ?assertEqual(false, State#state.enabled),
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    check_scaling, State),
                %% State should be unchanged since enabled=false
                ?assertEqual(State, NewState)
            end}]
      end}}.

handle_info_check_scaling_cooldown_test_() ->
    {"handle_info(check_scaling, State) when cooldown not expired",
     {setup,
      fun setup_mocks/0,
      fun teardown_mocks/1,
      fun(_) ->
          [{"returns state with only new timer when cooldown not expired",
            fun() ->
                %% Set up enabled state where cooldown has NOT expired
                %% last_scale_time is very recent (now), so cooldown_seconds
                %% (300) has not elapsed
                Now = erlang:system_time(second),
                State = aws_configured_state(),
                EnabledState = State#state{
                    enabled = true,
                    last_scale_time = Now,
                    check_timer = undefined,
                    idle_check_timer = undefined
                },
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    check_scaling, EnabledState),
                %% The auto_scale_check function should return early due
                %% to cooldown. Only the timer should be different.
                %% cloud_instances and current_cloud_nodes should be unchanged
                ?assertEqual(0, NewState#state.current_cloud_nodes),
                ?assertEqual(#{}, NewState#state.cloud_instances),
                %% A new timer should have been set
                ?assertNotEqual(undefined, NewState#state.check_timer),
                %% Clean up timer
                erlang:cancel_timer(NewState#state.check_timer)
            end}]
      end}}.

handle_info_check_idle_nodes_disabled_test_() ->
    {"handle_info(check_idle_nodes, State) when disabled",
     {setup,
      fun setup_mocks/0,
      fun teardown_mocks/1,
      fun(_) ->
          [{"returns unchanged state when enabled=false",
            fun() ->
                State = default_test_state(),
                ?assertEqual(false, State#state.enabled),
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    check_idle_nodes, State),
                ?assertEqual(State, NewState)
            end}]
      end}}.

handle_info_check_idle_nodes_no_idle_test_() ->
    {"handle_info(check_idle_nodes, State) with no idle instances",
     {setup,
      fun setup_mocks/0,
      fun teardown_mocks/1,
      fun(_) ->
          [{"returns state with only new timer when no cloud instances exist",
            fun() ->
                State = aws_configured_state(),
                EnabledState = State#state{
                    enabled = true,
                    cloud_instances = #{},
                    current_cloud_nodes = 0,
                    check_timer = undefined,
                    idle_check_timer = undefined
                },
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    check_idle_nodes, EnabledState),
                %% No instances to terminate, so cloud state is unchanged
                ?assertEqual(0, NewState#state.current_cloud_nodes),
                ?assertEqual(#{}, NewState#state.cloud_instances),
                %% A new idle check timer should have been set
                ?assertNotEqual(undefined, NewState#state.idle_check_timer),
                %% Clean up timer
                erlang:cancel_timer(NewState#state.idle_check_timer)
            end},

           {"returns unchanged instance count with recently-active instances",
            fun() ->
                Now = erlang:system_time(second),
                State = aws_configured_state(),
                %% Create instances that were active very recently
                %% (not idle beyond threshold)
                Instances = #{
                    <<"i-active1">> => #cloud_instance{
                        instance_id = <<"i-active1">>,
                        provider = aws,
                        instance_type = <<"c5.xlarge">>,
                        launch_time = Now - 600,
                        state = running,
                        last_job_time = Now - 10,  % Active 10 seconds ago
                        hourly_cost = 0.17
                    },
                    <<"i-active2">> => #cloud_instance{
                        instance_id = <<"i-active2">>,
                        provider = aws,
                        instance_type = <<"c5.xlarge">>,
                        launch_time = Now - 600,
                        state = running,
                        last_job_time = Now - 5,   % Active 5 seconds ago
                        hourly_cost = 0.17
                    }
                },
                EnabledState = State#state{
                    enabled = true,
                    cloud_instances = Instances,
                    current_cloud_nodes = 2,
                    check_timer = undefined,
                    idle_check_timer = undefined
                },
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    check_idle_nodes, EnabledState),
                %% No instances should have been terminated (none idle)
                ?assertEqual(2, NewState#state.current_cloud_nodes),
                ?assertEqual(2, maps:size(NewState#state.cloud_instances)),
                %% Clean up timer
                erlang:cancel_timer(NewState#state.idle_check_timer)
            end}]
      end}}.

%%====================================================================
%% Additional handle_info mocked tests
%%====================================================================

handle_info_update_costs_disabled_test_() ->
    {"handle_info(update_costs, State) when disabled",
     {setup,
      fun setup_mocks/0,
      fun teardown_mocks/1,
      fun(_) ->
          [{"returns unchanged state when enabled=false",
            fun() ->
                State = default_test_state(),
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    update_costs, State),
                ?assertEqual(State, NewState)
            end}]
      end}}.

handle_info_unknown_message_test_() ->
    {"handle_info with unknown message",
     {setup,
      fun setup_mocks/0,
      fun teardown_mocks/1,
      fun(_) ->
          [{"ignores unknown messages",
            fun() ->
                State = default_test_state(),
                {noreply, NewState} = flurm_cloud_scaling:handle_info(
                    {some_unknown, message}, State),
                ?assertEqual(State, NewState)
            end}]
      end}}.
