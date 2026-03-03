%%%-------------------------------------------------------------------
%%% @doc FLURM Global Spot Price Optimizer
%%%
%%% Finds the cheapest AWS spot instance across all regions that meets
%%% job resource requirements.
%%%
%%% Features:
%%% - Queries spot prices across all AWS regions in parallel
%%% - Instance type catalog with vCPUs, memory, architecture specs
%%% - Selects cheapest instance meeting min_cpus, min_memory requirements
%%% - Caches spot prices with configurable TTL
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_spot_optimizer).

-export([
    find_cheapest_spot/2,
    find_cheapest_spot/3,
    get_spot_prices/1,
    get_spot_prices/2,
    get_instance_specs/1,
    list_instance_types/0,
    list_regions/0,
    refresh_prices/0
]).

%% For testing
-ifdef(TEST).
-export([
    instance_catalog/0,
    aws_regions/0,
    parse_spot_price_response/1,
    filter_instances_by_requirements/2
]).
-endif.

-define(SPOT_PRICE_CACHE, flurm_spot_price_cache).
-define(CACHE_TTL_SECONDS, 300).  % 5 minute cache

%%====================================================================
%% Types
%%====================================================================

-type instance_spec() :: #{
    vcpus := pos_integer(),
    memory_gb := number(),
    arch := x86_64 | arm64,
    network_gbps := number(),
    storage := ephemeral | ebs_only
}.

-type spot_option() :: #{
    region := binary(),
    availability_zone := binary(),
    instance_type := binary(),
    price_per_hour := float(),
    specs := instance_spec()
}.

-type requirements() :: #{
    min_cpus => pos_integer(),
    min_memory_gb => number(),
    arch => x86_64 | arm64 | any,
    max_price_per_hour => float()
}.

-export_type([instance_spec/0, spot_option/0, requirements/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Find the cheapest spot instance meeting requirements across all regions
-spec find_cheapest_spot(requirements(), map()) -> {ok, spot_option()} | {error, term()}.
find_cheapest_spot(Requirements, AwsConfig) ->
    find_cheapest_spot(Requirements, AwsConfig, aws_regions()).

%% @doc Find cheapest spot in specific regions
-spec find_cheapest_spot(requirements(), map(), [binary()]) -> {ok, spot_option()} | {error, term()}.
find_cheapest_spot(Requirements, AwsConfig, Regions) ->
    %% Get candidate instance types that meet requirements
    Candidates = filter_instances_by_requirements(instance_catalog(), Requirements),
    case Candidates of
        [] ->
            {error, no_instance_types_meet_requirements};
        _ ->
            %% Query spot prices for candidates across regions in parallel
            InstanceTypes = [maps:get(type, C) || C <- Candidates],
            PriceResults = query_prices_parallel(Regions, InstanceTypes, AwsConfig),

            %% Flatten and find cheapest
            AllPrices = lists:flatten(PriceResults),
            case AllPrices of
                [] ->
                    {error, no_spot_prices_available};
                _ ->
                    %% Filter by max_price if specified
                    MaxPrice = maps:get(max_price_per_hour, Requirements, infinity),
                    Filtered = [P || P <- AllPrices,
                                maps:get(price_per_hour, P) =< MaxPrice],
                    case Filtered of
                        [] ->
                            {error, all_prices_exceed_max};
                        _ ->
                            Cheapest = lists:foldl(
                                fun(P, Best) ->
                                    case maps:get(price_per_hour, P) < maps:get(price_per_hour, Best) of
                                        true -> P;
                                        false -> Best
                                    end
                                end,
                                hd(Filtered),
                                tl(Filtered)
                            ),
                            {ok, Cheapest}
                    end
            end
    end.

%% @doc Get current spot prices for instance types in a region
-spec get_spot_prices(binary()) -> {ok, [spot_option()]} | {error, term()}.
get_spot_prices(Region) ->
    get_spot_prices(Region, []).

-spec get_spot_prices(binary(), [binary()]) -> {ok, [spot_option()]} | {error, term()}.
get_spot_prices(Region, InstanceTypes) ->
    %% Check cache first
    CacheKey = {Region, InstanceTypes},
    case get_cached_prices(CacheKey) of
        {ok, Prices} ->
            {ok, Prices};
        miss ->
            %% Query AWS
            case get_aws_config() of
                {ok, Config} ->
                    query_spot_prices(Region, InstanceTypes, Config);
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Get specs for an instance type
-spec get_instance_specs(binary()) -> {ok, instance_spec()} | {error, not_found}.
get_instance_specs(InstanceType) ->
    case lists:keyfind(InstanceType, 1, instance_catalog_raw()) of
        {InstanceType, Specs} ->
            {ok, Specs};
        false ->
            {error, not_found}
    end.

%% @doc List all known instance types
-spec list_instance_types() -> [binary()].
list_instance_types() ->
    [Type || #{type := Type} <- instance_catalog()].

%% @doc List all AWS regions
-spec list_regions() -> [binary()].
list_regions() ->
    aws_regions().

%% @doc Force refresh of cached prices
-spec refresh_prices() -> ok.
refresh_prices() ->
    catch ets:delete_all_objects(?SPOT_PRICE_CACHE),
    ok.

%%====================================================================
%% Internal - Price Querying
%%====================================================================

query_prices_parallel(Regions, InstanceTypes, AwsConfig) ->
    Parent = self(),
    Refs = [begin
        Ref = make_ref(),
        spawn_link(fun() ->
            Result = query_spot_prices(Region, InstanceTypes, AwsConfig),
            Parent ! {Ref, Result}
        end),
        {Ref, Region}
    end || Region <- Regions],

    %% Collect results with timeout
    collect_results(Refs, [], 10000).

collect_results([], Acc, _Timeout) ->
    Acc;
collect_results([{Ref, Region} | Rest], Acc, Timeout) ->
    receive
        {Ref, {ok, Prices}} ->
            collect_results(Rest, [Prices | Acc], Timeout);
        {Ref, {error, Reason}} ->
            lager:warning("Failed to get spot prices for ~s: ~p", [Region, Reason]),
            collect_results(Rest, Acc, Timeout)
    after Timeout ->
        lager:warning("Timeout getting spot prices for ~s", [Region]),
        collect_results(Rest, Acc, Timeout)
    end.

query_spot_prices(Region, InstanceTypes, AwsConfig) ->
    AccessKey = maps:get(access_key_id, AwsConfig),
    SecretKey = maps:get(secret_access_key, AwsConfig),

    %% Build DescribeSpotPriceHistory request
    Params0 = [
        {<<"Action">>, <<"DescribeSpotPriceHistory">>},
        {<<"Version">>, <<"2016-11-15">>},
        {<<"ProductDescription.1">>, <<"Linux/UNIX">>},
        %% Get prices from last hour only
        {<<"StartTime">>, format_iso8601(erlang:system_time(second) - 3600)}
    ],

    %% Add instance type filters
    Params1 = add_instance_type_filters(Params0, InstanceTypes, 1),

    Endpoint = iolist_to_binary([<<"https://ec2.">>, Region, <<".amazonaws.com/">>]),

    case aws_signed_request(Endpoint, Params1, AccessKey, SecretKey, Region, <<"ec2">>) of
        {ok, Body} ->
            Prices = parse_spot_price_response(Body),
            %% Add specs to each price entry
            PricesWithSpecs = lists:filtermap(
                fun(#{instance_type := Type} = P) ->
                    case get_instance_specs(Type) of
                        {ok, Specs} ->
                            {true, P#{specs => Specs, region => Region}};
                        {error, _} ->
                            false  % Skip unknown instance types
                    end
                end,
                Prices
            ),
            %% Cache the results
            cache_prices({Region, InstanceTypes}, PricesWithSpecs),
            {ok, PricesWithSpecs};
        {error, Reason} ->
            {error, Reason}
    end.

add_instance_type_filters(Params, [], _N) ->
    Params;
add_instance_type_filters(Params, [Type | Rest], N) ->
    Key = iolist_to_binary([<<"InstanceType.">>, integer_to_binary(N)]),
    add_instance_type_filters([{Key, Type} | Params], Rest, N + 1).

parse_spot_price_response(Body) ->
    %% Parse XML response - extract spotPriceHistorySet items
    %% <spotPriceHistorySet>
    %%   <item>
    %%     <instanceType>c5.xlarge</instanceType>
    %%     <spotPrice>0.0432</spotPrice>
    %%     <availabilityZone>us-west-2a</availabilityZone>
    %%     ...
    %%   </item>
    %% </spotPriceHistorySet>

    %% Extract all items
    ItemPattern = <<"<item>([\\s\\S]*?)</item>">>,
    case re:run(Body, ItemPattern, [global, {capture, all_but_first, binary}]) of
        {match, Matches} ->
            lists:filtermap(fun([ItemXml]) ->
                parse_spot_item(ItemXml)
            end, Matches);
        nomatch ->
            []
    end.

parse_spot_item(ItemXml) ->
    case {extract_xml_value(ItemXml, <<"instanceType">>),
          extract_xml_value(ItemXml, <<"spotPrice">>),
          extract_xml_value(ItemXml, <<"availabilityZone">>)} of
        {{ok, Type}, {ok, PriceStr}, {ok, AZ}} ->
            try
                Price = binary_to_float(PriceStr),
                {true, #{
                    instance_type => Type,
                    price_per_hour => Price,
                    availability_zone => AZ
                }}
            catch
                _:_ -> false
            end;
        _ ->
            false
    end.

extract_xml_value(Xml, Tag) ->
    Pattern = iolist_to_binary([<<"<">>, Tag, <<">([^<]*)</">>, Tag, <<">">>]),
    case re:run(Xml, Pattern, [{capture, all_but_first, binary}]) of
        {match, [Value]} -> {ok, Value};
        nomatch -> {error, not_found}
    end.

format_iso8601(Timestamp) ->
    {{Y, M, D}, {H, Min, S}} = calendar:system_time_to_universal_time(Timestamp, second),
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ",
                                   [Y, M, D, H, Min, S])).

%%====================================================================
%% Internal - Caching
%%====================================================================

get_cached_prices(Key) ->
    try
        case ets:lookup(?SPOT_PRICE_CACHE, Key) of
            [{Key, Prices, Timestamp}] ->
                Now = erlang:system_time(second),
                case Now - Timestamp < ?CACHE_TTL_SECONDS of
                    true -> {ok, Prices};
                    false -> miss
                end;
            [] ->
                miss
        end
    catch
        error:badarg ->
            %% ETS table doesn't exist, create it
            catch ets:new(?SPOT_PRICE_CACHE, [named_table, public, set]),
            miss
    end.

cache_prices(Key, Prices) ->
    try
        ets:insert(?SPOT_PRICE_CACHE, {Key, Prices, erlang:system_time(second)})
    catch
        error:badarg ->
            catch ets:new(?SPOT_PRICE_CACHE, [named_table, public, set]),
            ets:insert(?SPOT_PRICE_CACHE, {Key, Prices, erlang:system_time(second)})
    end.

%%====================================================================
%% Internal - AWS Signed Request (copied from flurm_cloud_scaling)
%%====================================================================

aws_signed_request(Endpoint, Params, AccessKey, SecretKey, Region, Service) ->
    QueryString = build_query_string(Params),

    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:universal_time(),
    AmzDate = io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ",
                           [Year, Month, Day, Hour, Min, Sec]),
    DateStamp = io_lib:format("~4..0B~2..0B~2..0B", [Year, Month, Day]),

    Method = <<"POST">>,
    CanonicalUri = <<"/">>,
    CanonicalQuerystring = <<>>,
    PayloadHash = crypto:hash(sha256, QueryString),
    PayloadHashHex = binary_to_hex(PayloadHash),

    Host = extract_host(Endpoint),
    Headers = [
        {<<"content-type">>, <<"application/x-www-form-urlencoded">>},
        {<<"host">>, Host},
        {<<"x-amz-date">>, iolist_to_binary(AmzDate)}
    ],

    SignedHeaders = <<"content-type;host;x-amz-date">>,
    CanonicalHeaders = format_canonical_headers(Headers),

    CanonicalRequest = iolist_to_binary([
        Method, <<"\n">>,
        CanonicalUri, <<"\n">>,
        CanonicalQuerystring, <<"\n">>,
        CanonicalHeaders, <<"\n">>,
        SignedHeaders, <<"\n">>,
        PayloadHashHex
    ]),

    Algorithm = <<"AWS4-HMAC-SHA256">>,
    CredentialScope = iolist_to_binary([DateStamp, <<"/">>, Region, <<"/">>,
                                        Service, <<"/aws4_request">>]),
    CanonicalRequestHash = binary_to_hex(crypto:hash(sha256, CanonicalRequest)),

    StringToSign = iolist_to_binary([
        Algorithm, <<"\n">>,
        AmzDate, <<"\n">>,
        CredentialScope, <<"\n">>,
        CanonicalRequestHash
    ]),

    KDate = crypto:mac(hmac, sha256, <<"AWS4", SecretKey/binary>>, iolist_to_binary(DateStamp)),
    KRegion = crypto:mac(hmac, sha256, KDate, Region),
    KService = crypto:mac(hmac, sha256, KRegion, Service),
    KSigning = crypto:mac(hmac, sha256, KService, <<"aws4_request">>),
    Signature = binary_to_hex(crypto:mac(hmac, sha256, KSigning, StringToSign)),

    AuthHeader = iolist_to_binary([
        Algorithm, <<" ">>,
        <<"Credential=">>, AccessKey, <<"/">>, CredentialScope, <<", ">>,
        <<"SignedHeaders=">>, SignedHeaders, <<", ">>,
        <<"Signature=">>, Signature
    ]),

    HttpHeaders = [
        {"content-type", "application/x-www-form-urlencoded"},
        {"x-amz-date", binary_to_list(iolist_to_binary(AmzDate))},
        {"authorization", binary_to_list(AuthHeader)}
    ],

    Request = {binary_to_list(iolist_to_binary(Endpoint)), HttpHeaders,
               "application/x-www-form-urlencoded", QueryString},

    case httpc:request(post, Request, [{timeout, 30000}], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, list_to_binary(Body)};
        {ok, {{_, Code, _}, _, Body}} ->
            {error, {aws_error, Code, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

build_query_string(Params) ->
    Parts = [iolist_to_binary([K, <<"=">>, uri_encode(V)]) || {K, V} <- Params],
    iolist_to_binary(lists:join(<<"&">>, Parts)).

uri_encode(Value) when is_binary(Value) ->
    uri_string:quote(Value);
uri_encode(Value) when is_list(Value) ->
    uri_string:quote(list_to_binary(Value)).

extract_host(Url) ->
    UrlBin = iolist_to_binary(Url),
    case binary:split(UrlBin, <<"://">>) of
        [_, Rest] ->
            case binary:split(Rest, <<"/">>) of
                [Host | _] -> Host;
                _ -> Rest
            end;
        _ -> UrlBin
    end.

format_canonical_headers(Headers) ->
    Sorted = lists:sort(fun({A, _}, {B, _}) -> A =< B end, Headers),
    iolist_to_binary([[K, <<":">>, V, <<"\n">>] || {K, V} <- Sorted]).

binary_to_hex(Bin) ->
    << <<(hex_char(B))>> || <<B:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.

%%====================================================================
%% Internal - Config
%%====================================================================

get_aws_config() ->
    case application:get_env(flurm_controller, cloud_scaling_config) of
        {ok, Config} when is_map(Config) ->
            {ok, Config};
        _ ->
            {error, no_aws_config}
    end.

%%====================================================================
%% Internal - Instance Requirements Filtering
%%====================================================================

filter_instances_by_requirements(Catalog, Requirements) ->
    MinCpus = maps:get(min_cpus, Requirements, 1),
    MinMemory = maps:get(min_memory_gb, Requirements, 1),
    RequiredArch = maps:get(arch, Requirements, any),

    lists:filter(
        fun(#{vcpus := VCpus, memory_gb := Mem, arch := Arch}) ->
            VCpus >= MinCpus andalso
            Mem >= MinMemory andalso
            (RequiredArch =:= any orelse RequiredArch =:= Arch)
        end,
        Catalog
    ).

%%====================================================================
%% Instance Type Catalog
%%====================================================================

%% All AWS regions (current as of 2024)
aws_regions() ->
    [
        <<"us-east-1">>,      % N. Virginia
        <<"us-east-2">>,      % Ohio
        <<"us-west-1">>,      % N. California
        <<"us-west-2">>,      % Oregon
        <<"ap-south-1">>,     % Mumbai
        <<"ap-northeast-1">>, % Tokyo
        <<"ap-northeast-2">>, % Seoul
        <<"ap-northeast-3">>, % Osaka
        <<"ap-southeast-1">>, % Singapore
        <<"ap-southeast-2">>, % Sydney
        <<"ca-central-1">>,   % Canada
        <<"eu-central-1">>,   % Frankfurt
        <<"eu-west-1">>,      % Ireland
        <<"eu-west-2">>,      % London
        <<"eu-west-3">>,      % Paris
        <<"eu-north-1">>,     % Stockholm
        <<"sa-east-1">>       % Sao Paulo
    ].

%% Instance type catalog - comprehensive list of compute-optimized instances
%% Format: #{type, vcpus, memory_gb, arch, network_gbps, storage}
instance_catalog() ->
    [maps:merge(#{type => Type}, Specs) || {Type, Specs} <- instance_catalog_raw()].

instance_catalog_raw() ->
    [
        %% C5 - Intel Xeon (x86_64)
        {<<"c5.large">>,    #{vcpus => 2,   memory_gb => 4,    arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"c5.xlarge">>,   #{vcpus => 4,   memory_gb => 8,    arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"c5.2xlarge">>,  #{vcpus => 8,   memory_gb => 16,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"c5.4xlarge">>,  #{vcpus => 16,  memory_gb => 32,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"c5.9xlarge">>,  #{vcpus => 36,  memory_gb => 72,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"c5.12xlarge">>, #{vcpus => 48,  memory_gb => 96,   arch => x86_64, network_gbps => 12,  storage => ebs_only}},
        {<<"c5.18xlarge">>, #{vcpus => 72,  memory_gb => 144,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},
        {<<"c5.24xlarge">>, #{vcpus => 96,  memory_gb => 192,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},
        {<<"c5.metal">>,    #{vcpus => 96,  memory_gb => 192,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},

        %% C6i - Intel Xeon 3rd Gen (x86_64)
        {<<"c6i.large">>,    #{vcpus => 2,   memory_gb => 4,    arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c6i.xlarge">>,   #{vcpus => 4,   memory_gb => 8,    arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c6i.2xlarge">>,  #{vcpus => 8,   memory_gb => 16,   arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c6i.4xlarge">>,  #{vcpus => 16,  memory_gb => 32,   arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c6i.8xlarge">>,  #{vcpus => 32,  memory_gb => 64,   arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c6i.12xlarge">>, #{vcpus => 48,  memory_gb => 96,   arch => x86_64, network_gbps => 18.75, storage => ebs_only}},
        {<<"c6i.16xlarge">>, #{vcpus => 64,  memory_gb => 128,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},
        {<<"c6i.24xlarge">>, #{vcpus => 96,  memory_gb => 192,  arch => x86_64, network_gbps => 37.5, storage => ebs_only}},
        {<<"c6i.32xlarge">>, #{vcpus => 128, memory_gb => 256,  arch => x86_64, network_gbps => 50,  storage => ebs_only}},
        {<<"c6i.metal">>,    #{vcpus => 128, memory_gb => 256,  arch => x86_64, network_gbps => 50,  storage => ebs_only}},

        %% C7i - Intel Xeon 4th Gen (x86_64)
        {<<"c7i.large">>,    #{vcpus => 2,   memory_gb => 4,    arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7i.xlarge">>,   #{vcpus => 4,   memory_gb => 8,    arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7i.2xlarge">>,  #{vcpus => 8,   memory_gb => 16,   arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7i.4xlarge">>,  #{vcpus => 16,  memory_gb => 32,   arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7i.8xlarge">>,  #{vcpus => 32,  memory_gb => 64,   arch => x86_64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7i.12xlarge">>, #{vcpus => 48,  memory_gb => 96,   arch => x86_64, network_gbps => 18.75, storage => ebs_only}},
        {<<"c7i.16xlarge">>, #{vcpus => 64,  memory_gb => 128,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},
        {<<"c7i.24xlarge">>, #{vcpus => 96,  memory_gb => 192,  arch => x86_64, network_gbps => 37.5, storage => ebs_only}},
        {<<"c7i.48xlarge">>, #{vcpus => 192, memory_gb => 384,  arch => x86_64, network_gbps => 50,  storage => ebs_only}},
        {<<"c7i.metal-24xl">>, #{vcpus => 96,  memory_gb => 192,  arch => x86_64, network_gbps => 37.5, storage => ebs_only}},
        {<<"c7i.metal-48xl">>, #{vcpus => 192, memory_gb => 384,  arch => x86_64, network_gbps => 50,  storage => ebs_only}},

        %% C8g - AWS Graviton4 (ARM64) - newest generation
        {<<"c8g.medium">>,   #{vcpus => 1,   memory_gb => 2,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c8g.large">>,    #{vcpus => 2,   memory_gb => 4,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c8g.xlarge">>,   #{vcpus => 4,   memory_gb => 8,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c8g.2xlarge">>,  #{vcpus => 8,   memory_gb => 16,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c8g.4xlarge">>,  #{vcpus => 16,  memory_gb => 32,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c8g.8xlarge">>,  #{vcpus => 32,  memory_gb => 64,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c8g.12xlarge">>, #{vcpus => 48,  memory_gb => 96,   arch => arm64, network_gbps => 18.75, storage => ebs_only}},
        {<<"c8g.16xlarge">>, #{vcpus => 64,  memory_gb => 128,  arch => arm64, network_gbps => 25,  storage => ebs_only}},
        {<<"c8g.24xlarge">>, #{vcpus => 96,  memory_gb => 192,  arch => arm64, network_gbps => 37.5, storage => ebs_only}},
        {<<"c8g.48xlarge">>, #{vcpus => 192, memory_gb => 384,  arch => arm64, network_gbps => 50,  storage => ebs_only}},
        {<<"c8g.metal-24xl">>, #{vcpus => 96,  memory_gb => 192,  arch => arm64, network_gbps => 37.5, storage => ebs_only}},
        {<<"c8g.metal-48xl">>, #{vcpus => 192, memory_gb => 384,  arch => arm64, network_gbps => 50,  storage => ebs_only}},

        %% C7g - AWS Graviton3 (ARM64)
        {<<"c7g.medium">>,   #{vcpus => 1,   memory_gb => 2,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7g.large">>,    #{vcpus => 2,   memory_gb => 4,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7g.xlarge">>,   #{vcpus => 4,   memory_gb => 8,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7g.2xlarge">>,  #{vcpus => 8,   memory_gb => 16,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7g.4xlarge">>,  #{vcpus => 16,  memory_gb => 32,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7g.8xlarge">>,  #{vcpus => 32,  memory_gb => 64,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"c7g.12xlarge">>, #{vcpus => 48,  memory_gb => 96,   arch => arm64, network_gbps => 18.75, storage => ebs_only}},
        {<<"c7g.16xlarge">>, #{vcpus => 64,  memory_gb => 128,  arch => arm64, network_gbps => 25,  storage => ebs_only}},
        {<<"c7g.metal">>,    #{vcpus => 64,  memory_gb => 128,  arch => arm64, network_gbps => 25,  storage => ebs_only}},

        %% C6g - AWS Graviton2 (ARM64)
        {<<"c6g.medium">>,   #{vcpus => 1,   memory_gb => 2,    arch => arm64, network_gbps => 10,  storage => ebs_only}},
        {<<"c6g.large">>,    #{vcpus => 2,   memory_gb => 4,    arch => arm64, network_gbps => 10,  storage => ebs_only}},
        {<<"c6g.xlarge">>,   #{vcpus => 4,   memory_gb => 8,    arch => arm64, network_gbps => 10,  storage => ebs_only}},
        {<<"c6g.2xlarge">>,  #{vcpus => 8,   memory_gb => 16,   arch => arm64, network_gbps => 10,  storage => ebs_only}},
        {<<"c6g.4xlarge">>,  #{vcpus => 16,  memory_gb => 32,   arch => arm64, network_gbps => 10,  storage => ebs_only}},
        {<<"c6g.8xlarge">>,  #{vcpus => 32,  memory_gb => 64,   arch => arm64, network_gbps => 12,  storage => ebs_only}},
        {<<"c6g.12xlarge">>, #{vcpus => 48,  memory_gb => 96,   arch => arm64, network_gbps => 20,  storage => ebs_only}},
        {<<"c6g.16xlarge">>, #{vcpus => 64,  memory_gb => 128,  arch => arm64, network_gbps => 25,  storage => ebs_only}},
        {<<"c6g.metal">>,    #{vcpus => 64,  memory_gb => 128,  arch => arm64, network_gbps => 25,  storage => ebs_only}},

        %% M5 - General Purpose (x86_64)
        {<<"m5.large">>,    #{vcpus => 2,   memory_gb => 8,    arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"m5.xlarge">>,   #{vcpus => 4,   memory_gb => 16,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"m5.2xlarge">>,  #{vcpus => 8,   memory_gb => 32,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"m5.4xlarge">>,  #{vcpus => 16,  memory_gb => 64,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"m5.8xlarge">>,  #{vcpus => 32,  memory_gb => 128,  arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"m5.12xlarge">>, #{vcpus => 48,  memory_gb => 192,  arch => x86_64, network_gbps => 12,  storage => ebs_only}},
        {<<"m5.16xlarge">>, #{vcpus => 64,  memory_gb => 256,  arch => x86_64, network_gbps => 20,  storage => ebs_only}},
        {<<"m5.24xlarge">>, #{vcpus => 96,  memory_gb => 384,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},
        {<<"m5.metal">>,    #{vcpus => 96,  memory_gb => 384,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},

        %% M7g - Graviton3 General Purpose (ARM64)
        {<<"m7g.medium">>,   #{vcpus => 1,   memory_gb => 4,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"m7g.large">>,    #{vcpus => 2,   memory_gb => 8,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"m7g.xlarge">>,   #{vcpus => 4,   memory_gb => 16,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"m7g.2xlarge">>,  #{vcpus => 8,   memory_gb => 32,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"m7g.4xlarge">>,  #{vcpus => 16,  memory_gb => 64,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"m7g.8xlarge">>,  #{vcpus => 32,  memory_gb => 128,  arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"m7g.12xlarge">>, #{vcpus => 48,  memory_gb => 192,  arch => arm64, network_gbps => 18.75, storage => ebs_only}},
        {<<"m7g.16xlarge">>, #{vcpus => 64,  memory_gb => 256,  arch => arm64, network_gbps => 25,  storage => ebs_only}},
        {<<"m7g.metal">>,    #{vcpus => 64,  memory_gb => 256,  arch => arm64, network_gbps => 25,  storage => ebs_only}},

        %% R5 - Memory Optimized (x86_64) - good for model checking
        {<<"r5.large">>,    #{vcpus => 2,   memory_gb => 16,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"r5.xlarge">>,   #{vcpus => 4,   memory_gb => 32,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"r5.2xlarge">>,  #{vcpus => 8,   memory_gb => 64,   arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"r5.4xlarge">>,  #{vcpus => 16,  memory_gb => 128,  arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"r5.8xlarge">>,  #{vcpus => 32,  memory_gb => 256,  arch => x86_64, network_gbps => 10,  storage => ebs_only}},
        {<<"r5.12xlarge">>, #{vcpus => 48,  memory_gb => 384,  arch => x86_64, network_gbps => 12,  storage => ebs_only}},
        {<<"r5.16xlarge">>, #{vcpus => 64,  memory_gb => 512,  arch => x86_64, network_gbps => 20,  storage => ebs_only}},
        {<<"r5.24xlarge">>, #{vcpus => 96,  memory_gb => 768,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},
        {<<"r5.metal">>,    #{vcpus => 96,  memory_gb => 768,  arch => x86_64, network_gbps => 25,  storage => ebs_only}},

        %% R7g - Graviton3 Memory Optimized (ARM64)
        {<<"r7g.medium">>,   #{vcpus => 1,   memory_gb => 8,    arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"r7g.large">>,    #{vcpus => 2,   memory_gb => 16,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"r7g.xlarge">>,   #{vcpus => 4,   memory_gb => 32,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"r7g.2xlarge">>,  #{vcpus => 8,   memory_gb => 64,   arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"r7g.4xlarge">>,  #{vcpus => 16,  memory_gb => 128,  arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"r7g.8xlarge">>,  #{vcpus => 32,  memory_gb => 256,  arch => arm64, network_gbps => 12.5, storage => ebs_only}},
        {<<"r7g.12xlarge">>, #{vcpus => 48,  memory_gb => 384,  arch => arm64, network_gbps => 18.75, storage => ebs_only}},
        {<<"r7g.16xlarge">>, #{vcpus => 64,  memory_gb => 512,  arch => arm64, network_gbps => 25,  storage => ebs_only}},
        {<<"r7g.metal">>,    #{vcpus => 64,  memory_gb => 512,  arch => arm64, network_gbps => 25,  storage => ebs_only}}
    ].
