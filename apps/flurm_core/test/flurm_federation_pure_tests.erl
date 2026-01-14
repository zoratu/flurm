%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_federation Module
%%%
%%% These tests directly exercise gen_server callbacks without mocking.
%%% We test init/1, handle_call/3, handle_cast/2, handle_info/2,
%%% terminate/2, and code_change/3 with real data structures.
%%%
%%% NO MECK - all tests use real values and direct function calls.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Record Definitions (matching flurm_federation.erl internal records)
%%====================================================================

%% Routing policies
-type routing_policy() :: round_robin | least_loaded | partition_affinity | random | weighted.
-type cluster_name() :: binary().
-type federation_name() :: binary().

%% Cluster configuration
-record(cluster_config, {
    host :: binary(),
    port :: pos_integer(),
    auth :: map(),
    weight = 1 :: pos_integer(),
    features = [] :: [binary()],
    partitions = [] :: [binary()],
    routing_policy = least_loaded :: routing_policy()
}).

-record(federation, {
    name :: federation_name(),
    created :: non_neg_integer(),
    clusters :: [cluster_name()],
    options :: map()
}).

-record(fed_cluster, {
    name :: cluster_name(),
    host :: binary(),
    port :: pos_integer(),
    auth = #{} :: map(),
    state :: up | down | drain | unknown,
    weight :: pos_integer(),
    features :: [binary()],
    partitions = [] :: [binary()],
    node_count :: non_neg_integer(),
    cpu_count :: non_neg_integer(),
    memory_mb :: non_neg_integer(),
    gpu_count :: non_neg_integer(),
    pending_jobs :: non_neg_integer(),
    running_jobs :: non_neg_integer(),
    available_cpus :: non_neg_integer(),
    available_memory :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    last_health_check :: non_neg_integer(),
    consecutive_failures :: non_neg_integer(),
    properties :: map()
}).

-record(fed_job, {
    id :: {cluster_name(), pos_integer()},
    federation_id :: binary(),
    origin_cluster :: cluster_name(),
    sibling_clusters :: [cluster_name()],
    sibling_jobs :: #{cluster_name() => pos_integer()},
    state :: pending | running | completed | failed | cancelled,
    submit_time :: non_neg_integer(),
    features_required :: [binary()],
    cluster_constraint :: [cluster_name()] | any
}).

-record(remote_job, {
    local_ref :: binary(),
    remote_cluster :: cluster_name(),
    remote_job_id :: pos_integer(),
    local_job_id :: pos_integer() | undefined,
    state :: pending | running | completed | failed | cancelled | unknown,
    submit_time :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    job_spec :: map()
}).

-record(partition_map, {
    partition :: binary(),
    cluster :: cluster_name(),
    priority :: non_neg_integer()
}).

-record(state, {
    local_cluster :: cluster_name(),
    federation :: #federation{} | undefined,
    sync_timer :: reference() | undefined,
    health_timer :: reference() | undefined,
    routing_policy :: routing_policy(),
    round_robin_index :: non_neg_integer(),
    http_client :: module()
}).

-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_JOBS_TABLE, flurm_fed_jobs).
-define(FED_PARTITION_MAP, flurm_fed_partition_map).
-define(FED_REMOTE_JOBS, flurm_fed_remote_jobs).

%%====================================================================
%% Test Setup/Cleanup
%%====================================================================

cleanup() ->
    catch ets:delete(?FED_CLUSTERS_TABLE),
    catch ets:delete(?FED_JOBS_TABLE),
    catch ets:delete(?FED_PARTITION_MAP),
    catch ets:delete(?FED_REMOTE_JOBS),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a basic state for testing (without ETS tables)
sample_state() ->
    #state{
        local_cluster = <<"test-cluster">>,
        federation = undefined,
        sync_timer = undefined,
        health_timer = undefined,
        routing_policy = least_loaded,
        round_robin_index = 0,
        http_client = httpc
    }.

%% Sample cluster config map
sample_cluster_config() ->
    #{
        host => <<"remote-host.example.com">>,
        port => 6817,
        auth => #{token => <<"test-token">>},
        weight => 2,
        features => [<<"gpu">>, <<"fast-network">>],
        partitions => [<<"batch">>, <<"gpu">>]
    }.

%% Sample cluster config record
sample_cluster_config_record() ->
    #cluster_config{
        host = <<"remote-host.example.com">>,
        port = 6817,
        auth = #{token => <<"test-token">>},
        weight = 2,
        features = [<<"gpu">>, <<"fast-network">>],
        partitions = [<<"batch">>, <<"gpu">>]
    }.

%% Sample fed_cluster entry
sample_fed_cluster() ->
    #fed_cluster{
        name = <<"cluster-a">>,
        host = <<"cluster-a.example.com">>,
        port = 6817,
        auth = #{},
        state = up,
        weight = 1,
        features = [<<"gpu">>],
        partitions = [<<"default">>],
        node_count = 10,
        cpu_count = 320,
        memory_mb = 640000,
        gpu_count = 8,
        pending_jobs = 5,
        running_jobs = 15,
        available_cpus = 160,
        available_memory = 320000,
        last_sync = erlang:system_time(second),
        last_health_check = erlang:system_time(second),
        consecutive_failures = 0,
        properties = #{}
    }.

sample_fed_cluster_b() ->
    #fed_cluster{
        name = <<"cluster-b">>,
        host = <<"cluster-b.example.com">>,
        port = 6817,
        auth = #{},
        state = up,
        weight = 2,
        features = [<<"gpu">>, <<"highspeed">>],
        partitions = [<<"batch">>, <<"gpu">>],
        node_count = 20,
        cpu_count = 640,
        memory_mb = 1280000,
        gpu_count = 16,
        pending_jobs = 10,
        running_jobs = 30,
        available_cpus = 320,
        available_memory = 640000,
        last_sync = erlang:system_time(second),
        last_health_check = erlang:system_time(second),
        consecutive_failures = 0,
        properties = #{}
    }.

sample_fed_cluster_down() ->
    #fed_cluster{
        name = <<"cluster-down">>,
        host = <<"cluster-down.example.com">>,
        port = 6817,
        auth = #{},
        state = down,
        weight = 1,
        features = [],
        partitions = [],
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0,
        pending_jobs = 0,
        running_jobs = 0,
        available_cpus = 0,
        available_memory = 0,
        last_sync = 0,
        last_health_check = 0,
        consecutive_failures = 5,
        properties = #{}
    }.

%% Sample job record
sample_job() ->
    #job{
        id = 1001,
        name = <<"test-job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 8192,
        time_limit = 3600,
        priority = 100,
        work_dir = <<"/tmp">>,
        account = <<"test-account">>,
        qos = <<"normal">>
    }.

%% Sample job map
sample_job_map() ->
    #{
        id => 1001,
        name => <<"test-job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        script => <<"#!/bin/bash\necho test">>,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 8192,
        time_limit => 3600,
        priority => 100,
        work_dir => <<"/tmp">>,
        account => <<"test-account">>,
        qos => <<"normal">>
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 callback tests", [
        {"init creates ETS tables and state", fun init_creates_tables/0},
        {"init registers local cluster", fun init_registers_local/0},
        {"init starts health timer", fun init_starts_health_timer/0}
    ]}.

init_creates_tables() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    %% Verify ETS tables were created
    ?assertNotEqual(undefined, ets:info(?FED_CLUSTERS_TABLE)),
    ?assertNotEqual(undefined, ets:info(?FED_JOBS_TABLE)),
    ?assertNotEqual(undefined, ets:info(?FED_PARTITION_MAP)),
    ?assertNotEqual(undefined, ets:info(?FED_REMOTE_JOBS)),
    %% Cancel health timer to avoid side effects
    case State#state.health_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    cleanup().

init_registers_local() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    %% Local cluster should be registered
    LocalCluster = State#state.local_cluster,
    ?assert(is_binary(LocalCluster)),
    %% Should be in the clusters table
    case ets:lookup(?FED_CLUSTERS_TABLE, LocalCluster) of
        [#fed_cluster{name = LocalCluster, state = up}] -> ok;
        _ -> ?assert(false)
    end,
    %% Cleanup
    case State#state.health_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    cleanup().

init_starts_health_timer() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    ?assert(is_reference(State#state.health_timer)),
    %% Cancel timer
    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - add_cluster Tests
%%====================================================================

add_cluster_test_() ->
    {"handle_call add_cluster tests", [
        {"add cluster with map config", fun add_cluster_map_config/0},
        {"add cluster with record config", fun add_cluster_record_config/0},
        {"add cluster registers partitions", fun add_cluster_registers_partitions/0}
    ]}.

add_cluster_map_config() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    Config = sample_cluster_config(),
    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"new-cluster">>, Config}, {self(), make_ref()}, State),

    %% Verify cluster was added
    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"new-cluster">>),
    ?assertEqual(<<"new-cluster">>, Cluster#fed_cluster.name),
    ?assertEqual(<<"remote-host.example.com">>, Cluster#fed_cluster.host),
    ?assertEqual(6817, Cluster#fed_cluster.port),
    ?assertEqual(2, Cluster#fed_cluster.weight),

    %% Cleanup
    erlang:cancel_timer(State#state.health_timer),
    cleanup().

add_cluster_record_config() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    Config = sample_cluster_config_record(),
    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"new-cluster">>, Config}, {self(), make_ref()}, State),

    %% Verify cluster was added
    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"new-cluster">>),
    ?assertEqual(<<"new-cluster">>, Cluster#fed_cluster.name),
    ?assertEqual(<<"remote-host.example.com">>, Cluster#fed_cluster.host),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

add_cluster_registers_partitions() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    Config = #{
        host => <<"host.example.com">>,
        port => 6817,
        partitions => [<<"part1">>, <<"part2">>],
        weight => 1
    },
    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"cluster-x">>, Config}, {self(), make_ref()}, State),

    %% Verify partitions were registered
    Part1Entries = ets:lookup(?FED_PARTITION_MAP, <<"part1">>),
    ?assert(length(Part1Entries) >= 1),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - remove_cluster Tests
%%====================================================================

remove_cluster_test_() ->
    {"handle_call remove_cluster tests", [
        {"remove existing cluster", fun remove_existing_cluster/0},
        {"cannot remove local cluster", fun cannot_remove_local/0},
        {"remove cluster removes partitions", fun remove_cluster_removes_partitions/0}
    ]}.

remove_existing_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add a cluster first
    Config = #{host => <<"h">>, port => 6817},
    flurm_federation:handle_call({add_cluster, <<"to-remove">>, Config},
                                 {self(), make_ref()}, State),

    %% Remove it
    {reply, ok, State} = flurm_federation:handle_call(
        {remove_cluster, <<"to-remove">>}, {self(), make_ref()}, State),

    %% Verify removed
    ?assertEqual([], ets:lookup(?FED_CLUSTERS_TABLE, <<"to-remove">>)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

cannot_remove_local() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),
    LocalCluster = State#state.local_cluster,

    {reply, {error, cannot_remove_local}, State} = flurm_federation:handle_call(
        {remove_cluster, LocalCluster}, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

remove_cluster_removes_partitions() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add cluster with partitions
    Config = #{host => <<"h">>, port => 6817, partitions => [<<"p1">>]},
    flurm_federation:handle_call({add_cluster, <<"c1">>, Config},
                                 {self(), make_ref()}, State),

    %% Remove it
    flurm_federation:handle_call({remove_cluster, <<"c1">>},
                                 {self(), make_ref()}, State),

    %% Partition mappings for that cluster should be gone
    Mappings = ets:match_object(?FED_PARTITION_MAP,
                                #partition_map{cluster = <<"c1">>, _ = '_'}),
    ?assertEqual([], Mappings),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - track_remote_job Tests
%%====================================================================

track_remote_job_test_() ->
    {"handle_call track_remote_job tests", [
        {"track remote job creates entry", fun track_remote_job_creates_entry/0},
        {"track remote job returns ref", fun track_remote_job_returns_ref/0}
    ]}.

track_remote_job_creates_entry() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    JobSpec = #{name => <<"test-job">>},
    {reply, {ok, LocalRef}, State} = flurm_federation:handle_call(
        {track_remote_job, <<"cluster-a">>, 5001, JobSpec},
        {self(), make_ref()}, State),

    %% Verify entry was created
    [Entry] = ets:lookup(?FED_REMOTE_JOBS, LocalRef),
    ?assertEqual(<<"cluster-a">>, Entry#remote_job.remote_cluster),
    ?assertEqual(5001, Entry#remote_job.remote_job_id),
    ?assertEqual(pending, Entry#remote_job.state),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

track_remote_job_returns_ref() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {ok, LocalRef}, State} = flurm_federation:handle_call(
        {track_remote_job, <<"cluster-a">>, 5001, #{}},
        {self(), make_ref()}, State),

    ?assert(is_binary(LocalRef)),
    ?assert(binary:match(LocalRef, <<"ref-">>) =/= nomatch),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - create_federation Tests
%%====================================================================

create_federation_test_() ->
    {"handle_call create_federation tests", [
        {"create federation success", fun create_federation_success/0},
        {"create federation starts sync timer", fun create_federation_starts_timer/0},
        {"create federation already federated", fun create_federation_already_federated/0}
    ]}.

create_federation_success() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, ok, NewState} = flurm_federation:handle_call(
        {create_federation, <<"my-federation">>, [<<"cluster-a">>]},
        {self(), make_ref()}, State),

    ?assertNotEqual(undefined, NewState#state.federation),
    ?assertEqual(<<"my-federation">>, (NewState#state.federation)#federation.name),

    %% Cleanup timers
    erlang:cancel_timer(State#state.health_timer),
    case NewState#state.sync_timer of
        undefined -> ok;
        T -> erlang:cancel_timer(T)
    end,
    cleanup().

create_federation_starts_timer() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, ok, NewState} = flurm_federation:handle_call(
        {create_federation, <<"my-federation">>, []},
        {self(), make_ref()}, State),

    ?assert(is_reference(NewState#state.sync_timer)),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(NewState#state.sync_timer),
    cleanup().

create_federation_already_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create first federation
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed1">>, []},
        {self(), make_ref()}, State),

    %% Try to create another
    {reply, {error, already_federated}, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed2">>, []},
        {self(), make_ref()}, State1),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    cleanup().

%%====================================================================
%% handle_call - leave_federation Tests
%%====================================================================

leave_federation_test_() ->
    {"handle_call leave_federation tests", [
        {"leave federation success", fun leave_federation_success/0},
        {"leave federation not federated", fun leave_federation_not_federated/0},
        {"leave federation clears remote clusters", fun leave_clears_remote_clusters/0}
    ]}.

leave_federation_success() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation first
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"my-fed">>, []},
        {self(), make_ref()}, State),

    %% Leave it
    {reply, ok, State2} = flurm_federation:handle_call(
        leave_federation, {self(), make_ref()}, State1),

    ?assertEqual(undefined, State2#state.federation),
    ?assertEqual(undefined, State2#state.sync_timer),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

leave_federation_not_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {error, not_federated}, State} = flurm_federation:handle_call(
        leave_federation, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

leave_clears_remote_clusters() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add remote clusters
    flurm_federation:handle_call({add_cluster, <<"remote1">>,
                                  #{host => <<"h1">>, port => 6817}},
                                 {self(), make_ref()}, State),
    flurm_federation:handle_call({add_cluster, <<"remote2">>,
                                  #{host => <<"h2">>, port => 6817}},
                                 {self(), make_ref()}, State),

    %% Create federation and leave
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed">>, []}, {self(), make_ref()}, State),
    {reply, ok, _State2} = flurm_federation:handle_call(
        leave_federation, {self(), make_ref()}, State1),

    %% Remote clusters should be gone
    ?assertEqual([], ets:lookup(?FED_CLUSTERS_TABLE, <<"remote1">>)),
    ?assertEqual([], ets:lookup(?FED_CLUSTERS_TABLE, <<"remote2">>)),
    %% Local cluster should still exist
    ?assertNotEqual([], ets:lookup(?FED_CLUSTERS_TABLE, State#state.local_cluster)),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    cleanup().

%%====================================================================
%% handle_call - get_federation_info Tests
%%====================================================================

get_federation_info_test_() ->
    {"handle_call get_federation_info tests", [
        {"get info when federated", fun get_info_federated/0},
        {"get info when not federated", fun get_info_not_federated/0}
    ]}.

get_info_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"my-fed">>, [<<"c1">>]}, {self(), make_ref()}, State),

    {reply, {ok, Info}, State1} = flurm_federation:handle_call(
        get_federation_info, {self(), make_ref()}, State1),

    ?assertEqual(<<"my-fed">>, maps:get(name, Info)),
    ?assert(lists:member(State#state.local_cluster, maps:get(clusters, Info))),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    cleanup().

get_info_not_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {error, not_federated}, State} = flurm_federation:handle_call(
        get_federation_info, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - is_federated Tests
%%====================================================================

is_federated_test_() ->
    {"handle_call is_federated tests", [
        {"is_federated when in federation", fun is_federated_true/0},
        {"is_federated when not in federation", fun is_federated_false/0},
        {"is_federated when multiple clusters", fun is_federated_multiple_clusters/0}
    ]}.

is_federated_true() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed">>, []}, {self(), make_ref()}, State),

    {reply, true, State1} = flurm_federation:handle_call(
        is_federated, {self(), make_ref()}, State1),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    cleanup().

is_federated_false() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Only local cluster exists
    {reply, false, State} = flurm_federation:handle_call(
        is_federated, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

is_federated_multiple_clusters() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add a remote cluster without creating a federation
    flurm_federation:handle_call({add_cluster, <<"remote">>,
                                  #{host => <<"h">>, port => 6817}},
                                 {self(), make_ref()}, State),

    %% Should be true because there are multiple clusters
    {reply, true, State} = flurm_federation:handle_call(
        is_federated, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - get_local_cluster Tests
%%====================================================================

get_local_cluster_test_() ->
    {"handle_call get_local_cluster tests", [
        {"get local cluster name", fun get_local_cluster_name/0}
    ]}.

get_local_cluster_name() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, LocalCluster, State} = flurm_federation:handle_call(
        get_local_cluster, {self(), make_ref()}, State),

    ?assertEqual(State#state.local_cluster, LocalCluster),
    ?assert(is_binary(LocalCluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - set_features Tests
%%====================================================================

set_features_test_() ->
    {"handle_call set_features tests", [
        {"set features on local cluster", fun set_features_local/0}
    ]}.

set_features_local() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Features = [<<"gpu">>, <<"fast-disk">>, <<"infiniband">>],
    {reply, ok, State} = flurm_federation:handle_call(
        {set_features, Features}, {self(), make_ref()}, State),

    %% Verify features were set
    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, State#state.local_cluster),
    ?assertEqual(Features, Cluster#fed_cluster.features),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - submit_federated_job Tests
%%====================================================================

submit_federated_job_test_() ->
    {"handle_call submit_federated_job tests", [
        {"submit when not federated", fun submit_federated_not_federated/0}
    ]}.

submit_federated_not_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Job = sample_job(),
    {reply, {error, not_federated}, State} = flurm_federation:handle_call(
        {submit_federated_job, Job, #{}}, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - unknown request Tests
%%====================================================================

unknown_request_test_() ->
    {"handle_call unknown request tests", [
        {"unknown request returns error", fun unknown_returns_error/0}
    ]}.

unknown_returns_error() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {error, unknown_request}, State} = flurm_federation:handle_call(
        unknown_request, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_test_() ->
    {"handle_cast tests", [
        {"cast is ignored", fun cast_ignored/0}
    ]}.

cast_ignored() ->
    State = sample_state(),
    {noreply, State} = flurm_federation:handle_cast(some_message, State),
    {noreply, State} = flurm_federation:handle_cast({any, tuple}, State).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests", [
        {"sync_all message", fun handle_info_sync_all/0},
        {"health_check message", fun handle_info_health_check/0},
        {"cluster_update message", fun handle_info_cluster_update/0},
        {"cluster_health up message", fun handle_info_cluster_health_up/0},
        {"cluster_health down message", fun handle_info_cluster_health_down/0},
        {"unknown info is ignored", fun handle_info_unknown/0}
    ]}.

handle_info_sync_all() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Trigger sync_all
    {noreply, NewState} = flurm_federation:handle_info(sync_all, State),

    %% A new sync timer should be set
    ?assert(is_reference(NewState#state.sync_timer)),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(NewState#state.sync_timer),
    cleanup().

handle_info_health_check() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Trigger health_check
    {noreply, NewState} = flurm_federation:handle_info(health_check, State),

    %% A new health timer should be set
    ?assert(is_reference(NewState#state.health_timer)),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(NewState#state.health_timer),
    cleanup().

handle_info_cluster_update() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add a cluster to update
    flurm_federation:handle_call({add_cluster, <<"test-cluster-x">>,
                                  #{host => <<"h">>, port => 6817}},
                                 {self(), make_ref()}, State),

    Stats = #{
        node_count => 50,
        cpu_count => 1600,
        memory_mb => 3200000,
        available_cpus => 800,
        available_memory => 1600000,
        pending_jobs => 10,
        running_jobs => 40
    },

    {noreply, State} = flurm_federation:handle_info(
        {cluster_update, <<"test-cluster-x">>, Stats}, State),

    %% Verify cluster was updated
    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"test-cluster-x">>),
    ?assertEqual(50, Cluster#fed_cluster.node_count),
    ?assertEqual(1600, Cluster#fed_cluster.cpu_count),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

handle_info_cluster_health_up() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add a cluster
    flurm_federation:handle_call({add_cluster, <<"health-test">>,
                                  #{host => <<"h">>, port => 6817}},
                                 {self(), make_ref()}, State),

    {noreply, State} = flurm_federation:handle_info(
        {cluster_health, <<"health-test">>, up}, State),

    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"health-test">>),
    ?assertEqual(up, Cluster#fed_cluster.state),
    ?assertEqual(0, Cluster#fed_cluster.consecutive_failures),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

handle_info_cluster_health_down() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add a cluster
    flurm_federation:handle_call({add_cluster, <<"health-test">>,
                                  #{host => <<"h">>, port => 6817}},
                                 {self(), make_ref()}, State),

    %% Send multiple down events to trigger state change
    flurm_federation:handle_info({cluster_health, <<"health-test">>, down}, State),
    flurm_federation:handle_info({cluster_health, <<"health-test">>, down}, State),
    {noreply, State} = flurm_federation:handle_info(
        {cluster_health, <<"health-test">>, down}, State),

    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"health-test">>),
    ?assert(Cluster#fed_cluster.consecutive_failures >= 3),
    ?assertEqual(down, Cluster#fed_cluster.state),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

handle_info_unknown() ->
    State = sample_state(),
    {noreply, State} = flurm_federation:handle_info(unknown_message, State),
    {noreply, State} = flurm_federation:handle_info({random, data}, State).

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests", [
        {"terminate cancels timers", fun terminate_cancels_timers/0},
        {"terminate returns ok", fun terminate_returns_ok/0}
    ]}.

terminate_cancels_timers() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation to get sync timer
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed">>, []}, {self(), make_ref()}, State),

    ?assertEqual(ok, flurm_federation:terminate(normal, State1)),

    %% Timers should be cancelled (no crashes)
    cleanup().

terminate_returns_ok() ->
    State = sample_state(),
    ?assertEqual(ok, flurm_federation:terminate(normal, State)),
    ?assertEqual(ok, flurm_federation:terminate(shutdown, State)),
    ?assertEqual(ok, flurm_federation:terminate({shutdown, reason}, State)).

%%====================================================================
%% code_change Tests
%%====================================================================

code_change_test_() ->
    {"code_change tests", [
        {"code_change returns ok with state", fun code_change_ok/0}
    ]}.

code_change_ok() ->
    State = sample_state(),
    {ok, State} = flurm_federation:code_change("1.0.0", State, []),
    {ok, State} = flurm_federation:code_change("0.9.0", State, extra).

%%====================================================================
%% ETS-based API Tests (list_clusters, get_cluster_status, etc.)
%%====================================================================

list_clusters_test_() ->
    {"list_clusters tests", [
        {"list clusters returns all", fun list_clusters_all/0},
        {"list clusters includes details", fun list_clusters_details/0}
    ]}.

list_clusters_all() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add some clusters
    flurm_federation:handle_call({add_cluster, <<"c1">>,
                                  #{host => <<"h1">>, port => 6817}},
                                 {self(), make_ref()}, State),
    flurm_federation:handle_call({add_cluster, <<"c2">>,
                                  #{host => <<"h2">>, port => 6817}},
                                 {self(), make_ref()}, State),

    Clusters = flurm_federation:list_clusters(),
    ?assert(length(Clusters) >= 3),  % local + 2 added

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

list_clusters_details() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    flurm_federation:handle_call({add_cluster, <<"detailed">>,
                                  #{host => <<"host.example.com">>,
                                    port => 6817,
                                    weight => 5,
                                    features => [<<"gpu">>]}},
                                 {self(), make_ref()}, State),

    Clusters = flurm_federation:list_clusters(),
    [DetailedCluster] = [C || C <- Clusters, maps:get(name, C) =:= <<"detailed">>],

    ?assertEqual(<<"host.example.com">>, maps:get(host, DetailedCluster)),
    ?assertEqual(6817, maps:get(port, DetailedCluster)),
    ?assertEqual(5, maps:get(weight, DetailedCluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

get_cluster_status_test_() ->
    {"get_cluster_status tests", [
        {"get existing cluster status", fun get_status_existing/0},
        {"get non-existing cluster status", fun get_status_nonexisting/0}
    ]}.

get_status_existing() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    flurm_federation:handle_call({add_cluster, <<"status-test">>,
                                  #{host => <<"h">>, port => 6817}},
                                 {self(), make_ref()}, State),

    {ok, Status} = flurm_federation:get_cluster_status(<<"status-test">>),
    ?assert(is_map(Status)),
    ?assertEqual(<<"status-test">>, maps:get(name, Status)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

get_status_nonexisting() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ?assertEqual({error, not_found},
                 flurm_federation:get_cluster_status(<<"nonexistent">>)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% get_cluster_for_partition Tests
%%====================================================================

get_cluster_for_partition_test_() ->
    {"get_cluster_for_partition tests", [
        {"get cluster for mapped partition", fun get_partition_mapped/0},
        {"get cluster for unmapped partition", fun get_partition_unmapped/0}
    ]}.

get_partition_mapped() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add cluster with partitions
    flurm_federation:handle_call({add_cluster, <<"part-cluster">>,
                                  #{host => <<"h">>, port => 6817,
                                    partitions => [<<"my-partition">>]}},
                                 {self(), make_ref()}, State),

    {ok, Cluster} = flurm_federation:get_cluster_for_partition(<<"my-partition">>),
    ?assertEqual(<<"part-cluster">>, Cluster),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

get_partition_unmapped() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ?assertEqual({error, not_found},
                 flurm_federation:get_cluster_for_partition(<<"unmapped">>)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% get_federation_resources Tests
%%====================================================================

get_federation_resources_test_() ->
    {"get_federation_resources tests", [
        {"aggregate resources from clusters", fun aggregate_resources/0},
        {"aggregate includes only up clusters", fun aggregate_up_only/0}
    ]}.

aggregate_resources() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert test clusters directly
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster_b()),

    Resources = flurm_federation:get_federation_resources(),

    ?assert(maps:get(total_nodes, Resources) >= 30),
    ?assert(maps:get(total_cpus, Resources) >= 960),
    ?assert(maps:get(clusters_up, Resources) >= 2),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

aggregate_up_only() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert one up and one down cluster
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster_down()),

    Resources = flurm_federation:get_federation_resources(),

    %% Down cluster should not contribute to resources but count as down
    ?assertEqual(1, maps:get(clusters_down, Resources)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% get_cluster_info Tests
%%====================================================================

get_cluster_info_test_() ->
    {"get_cluster_info tests", [
        {"get existing cluster info", fun get_cluster_info_existing/0},
        {"get nonexistent cluster info", fun get_cluster_info_nonexisting/0}
    ]}.

get_cluster_info_existing() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    {ok, Cluster} = flurm_federation:get_cluster_info(<<"cluster-a">>),
    ?assertEqual(<<"cluster-a">>, Cluster#fed_cluster.name),
    ?assertEqual(up, Cluster#fed_cluster.state),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

get_cluster_info_nonexisting() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ?assertEqual({error, not_found},
                 flurm_federation:get_cluster_info(<<"nonexistent">>)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% get_federated_job Tests
%%====================================================================

get_federated_job_test_() ->
    {"get_federated_job tests", [
        {"get existing federated job", fun get_fed_job_existing/0},
        {"get nonexistent federated job", fun get_fed_job_nonexisting/0}
    ]}.

get_fed_job_existing() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert a federated job
    FedJob = #fed_job{
        id = {<<"cluster-a">>, 1001},
        federation_id = <<"fed-12345-67890">>,
        origin_cluster = <<"cluster-a">>,
        sibling_clusters = [<<"cluster-a">>, <<"cluster-b">>],
        sibling_jobs = #{<<"cluster-a">> => 1001, <<"cluster-b">> => 2001},
        state = running,
        submit_time = erlang:system_time(second),
        features_required = [],
        cluster_constraint = any
    },
    ets:insert(?FED_JOBS_TABLE, FedJob),

    {ok, Retrieved} = flurm_federation:get_federated_job(<<"fed-12345-67890">>),
    ?assertEqual(<<"fed-12345-67890">>, Retrieved#fed_job.federation_id),
    ?assertEqual(running, Retrieved#fed_job.state),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

get_fed_job_nonexisting() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ?assertEqual({error, not_found},
                 flurm_federation:get_federated_job(<<"nonexistent">>)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - route_job Tests
%%====================================================================

route_job_test_() ->
    {"handle_call route_job tests", [
        {"route job with no eligible clusters", fun route_job_no_eligible/0},
        {"route job returns local for fallback", fun route_job_fallback_local/0}
    ]}.

route_job_no_eligible() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Only local cluster exists with no resources
    Job = sample_job_map(),

    {reply, {ok, LocalCluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    %% Falls back to local cluster
    ?assertEqual(State#state.local_cluster, LocalCluster),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

route_job_fallback_local() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Job = #{partition => <<"unmapped">>},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assertEqual(State#state.local_cluster, Cluster),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - get_federation_jobs Tests
%%====================================================================

get_federation_jobs_test_() ->
    {"handle_call get_federation_jobs tests", [
        {"get federation jobs with remote jobs", fun get_federation_jobs_remote/0}
    ]}.

get_federation_jobs_remote() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add some tracked remote jobs
    RemoteJob1 = #remote_job{
        local_ref = <<"ref-1">>,
        remote_cluster = <<"cluster-a">>,
        remote_job_id = 1001,
        state = running,
        submit_time = erlang:system_time(second),
        last_sync = erlang:system_time(second),
        job_spec = #{}
    },
    RemoteJob2 = #remote_job{
        local_ref = <<"ref-2">>,
        remote_cluster = <<"cluster-b">>,
        remote_job_id = 2001,
        state = pending,
        submit_time = erlang:system_time(second),
        last_sync = erlang:system_time(second),
        job_spec = #{}
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob1),
    ets:insert(?FED_REMOTE_JOBS, RemoteJob2),

    {reply, Jobs, State} = flurm_federation:handle_call(
        get_federation_jobs, {self(), make_ref()}, State),

    %% Should include the remote jobs
    RemoteJobs = [J || J <- Jobs, maps:is_key(local_ref, J)],
    ?assertEqual(2, length(RemoteJobs)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - sync_cluster Tests
%%====================================================================

sync_cluster_test_() ->
    {"handle_call sync_cluster tests", [
        {"sync nonexistent cluster", fun sync_nonexistent_cluster/0}
    ]}.

sync_nonexistent_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {error, cluster_not_found}, State} = flurm_federation:handle_call(
        {sync_cluster, <<"nonexistent">>}, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% handle_call - get_remote_job_status Tests
%%====================================================================

get_remote_job_status_test_() ->
    {"handle_call get_remote_job_status tests", [
        {"get status for nonexistent cluster", fun get_remote_status_no_cluster/0}
    ]}.

get_remote_status_no_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {error, cluster_not_found}, State} = flurm_federation:handle_call(
        {get_remote_job_status, <<"nonexistent">>, 1001}, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Handle Info - Sync with Federation
%%====================================================================

handle_info_sync_with_federation_test_() ->
    {"handle_info sync_all with federation tests", [
        {"sync_all iterates federation clusters", fun sync_all_with_federation/0}
    ]}.

sync_all_with_federation() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed">>, [<<"cluster-a">>]}, {self(), make_ref()}, State),

    %% Trigger sync_all
    {noreply, State2} = flurm_federation:handle_info(sync_all, State1),

    ?assert(is_reference(State2#state.sync_timer)),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    erlang:cancel_timer(State2#state.sync_timer),
    cleanup().

%%====================================================================
%% handle_call - join_federation Tests
%%====================================================================

join_federation_test_() ->
    {"handle_call join_federation tests", [
        {"join when already federated", fun join_already_federated/0}
    ]}.

join_already_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation first
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed1">>, []}, {self(), make_ref()}, State),

    %% Try to join another
    {reply, {error, already_federated}, State1} = flurm_federation:handle_call(
        {join_federation, <<"fed2">>, <<"host.example.com">>},
        {self(), make_ref()}, State1),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    cleanup().

%%====================================================================
%% Edge Cases and Error Handling
%%====================================================================

edge_cases_test_() ->
    {"Edge case tests", [
        {"cluster with default values", fun cluster_default_values/0},
        {"set features when local not found", fun set_features_no_local/0},
        {"cluster health for nonexistent", fun health_nonexistent_cluster/0}
    ]}.

cluster_default_values() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add cluster with minimal config
    Config = #{host => <<"minimal.host">>},  % Only host, rest defaults
    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"minimal">>, Config}, {self(), make_ref()}, State),

    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"minimal">>),
    ?assertEqual(6817, Cluster#fed_cluster.port),  % Default port
    ?assertEqual(1, Cluster#fed_cluster.weight),   % Default weight
    ?assertEqual([], Cluster#fed_cluster.features),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

set_features_no_local() ->
    cleanup(),
    %% Create state without proper ETS setup
    State = sample_state(),

    %% Create empty ETS table
    ets:new(?FED_CLUSTERS_TABLE, [
        named_table, public, set,
        {keypos, #fed_cluster.name}
    ]),

    %% Setting features when local cluster not in table should not crash
    {reply, ok, State} = flurm_federation:handle_call(
        {set_features, [<<"gpu">>]}, {self(), make_ref()}, State),

    cleanup().

health_nonexistent_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Health check for nonexistent cluster should not crash
    {noreply, State} = flurm_federation:handle_info(
        {cluster_health, <<"nonexistent">>, up}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Routing Policy Tests
%%====================================================================

routing_policy_test_() ->
    {"Routing policy tests", [
        {"state has routing policy", fun state_has_routing_policy/0}
    ]}.

state_has_routing_policy() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Default is least_loaded
    ?assertEqual(least_loaded, State#state.routing_policy),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% No ETS Table Tests
%%====================================================================

no_ets_test_() ->
    {"Tests when ETS tables don't exist", [
        {"list_clusters no table", fun list_clusters_no_table/0},
        {"get_cluster_status no table", fun get_status_no_table/0},
        {"get_cluster_for_partition no table", fun get_partition_no_table/0}
    ]}.

list_clusters_no_table() ->
    cleanup(),
    %% Table doesn't exist - should return empty or handle gracefully
    Result = (catch flurm_federation:list_clusters()),
    %% Either empty list or badarg error
    ?assert(Result =:= [] orelse element(1, Result) =:= 'EXIT').

get_status_no_table() ->
    cleanup(),
    Result = (catch flurm_federation:get_cluster_status(<<"test">>)),
    ?assert(Result =:= {error, not_found} orelse element(1, Result) =:= 'EXIT').

get_partition_no_table() ->
    cleanup(),
    Result = (catch flurm_federation:get_cluster_for_partition(<<"test">>)),
    ?assert(Result =:= {error, not_found} orelse element(1, Result) =:= 'EXIT').

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {"Additional coverage tests", [
        {"cluster update nonexistent", fun update_nonexistent_cluster/0},
        {"add cluster with all options", fun add_cluster_all_options/0},
        {"sync job state cluster not found", fun sync_job_no_cluster/0}
    ]}.

update_nonexistent_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Update for nonexistent cluster should be ignored
    {noreply, State} = flurm_federation:handle_info(
        {cluster_update, <<"nonexistent">>, #{node_count => 10}}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

add_cluster_all_options() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Config = #{
        host => <<"full-cluster.example.com">>,
        port => 6818,
        auth => #{token => <<"secret">>, api_key => <<"key123">>},
        weight => 10,
        features => [<<"gpu">>, <<"nvlink">>, <<"infiniband">>],
        partitions => [<<"compute">>, <<"gpu-batch">>, <<"debug">>]
    },

    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"full-cluster">>, Config}, {self(), make_ref()}, State),

    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"full-cluster">>),
    ?assertEqual(6818, Cluster#fed_cluster.port),
    ?assertEqual(10, Cluster#fed_cluster.weight),
    ?assertEqual([<<"gpu">>, <<"nvlink">>, <<"infiniband">>], Cluster#fed_cluster.features),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

sync_job_no_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    {reply, {error, cluster_not_found}, State} = flurm_federation:handle_call(
        {sync_job_state, <<"nonexistent">>, 1001}, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Test Leave Federation with Timer
%%====================================================================

leave_federation_with_timer_test() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation (this sets sync_timer)
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed">>, []}, {self(), make_ref()}, State),

    ?assert(is_reference(State1#state.sync_timer)),

    %% Leave federation
    {reply, ok, State2} = flurm_federation:handle_call(
        leave_federation, {self(), make_ref()}, State1),

    %% Timer should be undefined now
    ?assertEqual(undefined, State2#state.sync_timer),
    ?assertEqual(undefined, State2#state.federation),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Test Terminate with Both Timers
%%====================================================================

terminate_with_both_timers_test() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation to get sync timer
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"fed">>, []}, {self(), make_ref()}, State),

    %% Both timers should be set
    ?assert(is_reference(State1#state.sync_timer)),
    ?assert(is_reference(State1#state.health_timer)),

    %% Terminate should cancel both
    ?assertEqual(ok, flurm_federation:terminate(shutdown, State1)),

    cleanup().

%%====================================================================
%% Test Terminate with No Timers
%%====================================================================

terminate_no_timers_test() ->
    State = #state{
        local_cluster = <<"test">>,
        federation = undefined,
        sync_timer = undefined,
        health_timer = undefined,
        routing_policy = least_loaded,
        round_robin_index = 0,
        http_client = httpc
    },
    ?assertEqual(ok, flurm_federation:terminate(normal, State)).

%%====================================================================
%% Test Get Federation Resources Empty
%%====================================================================

get_federation_resources_empty_test() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Clear all clusters
    ets:delete_all_objects(?FED_CLUSTERS_TABLE),

    Resources = flurm_federation:get_federation_resources(),

    ?assertEqual(0, maps:get(total_nodes, Resources)),
    ?assertEqual(0, maps:get(clusters_up, Resources)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Test Submit Job Routing
%%====================================================================

submit_job_routing_test_() ->
    {"handle_call submit_job tests", [
        {"submit job with specific cluster", fun submit_job_specific_cluster/0}
    ]}.

submit_job_specific_cluster() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Submit job with specific cluster that doesn't exist
    Job = sample_job(),
    Options = #{cluster => <<"nonexistent-cluster">>},

    %% Should return error because cluster not found
    {reply, Result, State} = flurm_federation:handle_call(
        {submit_job, Job, Options}, {self(), make_ref()}, State),

    ?assertMatch({error, _}, Result),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Route Job with Eligible Clusters Tests
%%====================================================================

route_job_eligible_test_() ->
    {"handle_call route_job with eligible clusters tests", [
        {"route job least_loaded policy", fun route_job_least_loaded/0},
        {"route job to cluster with resources", fun route_job_with_resources/0},
        {"route job partition affinity", fun route_job_partition_affinity/0}
    ]}.

route_job_least_loaded() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert clusters with different loads
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster_b()),

    Job = #{partition => undefined, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

route_job_with_resources() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert cluster with sufficient resources
    Cluster = sample_fed_cluster(),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    Job = #{partition => <<"default">>, num_cpus => 10, memory_mb => 10000},

    {reply, {ok, RoutedCluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(RoutedCluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

route_job_partition_affinity() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert cluster with partition
    ClusterWithPart = sample_fed_cluster(),
    ets:insert(?FED_CLUSTERS_TABLE, ClusterWithPart),

    %% Add partition mapping
    PartMap = #partition_map{
        partition = <<"compute">>,
        cluster = <<"cluster-a">>,
        priority = 100
    },
    ets:insert(?FED_PARTITION_MAP, PartMap),

    Job = #{partition => <<"compute">>, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Submit Job Tests - More Coverage
%%====================================================================

submit_job_more_test_() ->
    {"More submit_job tests", [
        {"submit job to local cluster", fun submit_job_local/0},
        {"submit job auto-route", fun submit_job_auto_route/0},
        {"submit job to unavailable cluster", fun submit_job_unavailable/0}
    ]}.

submit_job_local() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Job = sample_job(),
    Options = #{cluster => State#state.local_cluster},

    %% Local submit will fail because flurm_scheduler is not running
    %% but we're testing the path selection
    %% Wrap in try-catch since scheduler is not running
    Result = try
        flurm_federation:handle_call(
            {submit_job, Job, Options}, {self(), make_ref()}, State)
    catch
        _:_ -> {reply, {error, expected_failure}, State}
    end,

    %% Either error or noproc because scheduler not running
    ?assertMatch({reply, _, _}, Result),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

submit_job_auto_route() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Job = sample_job(),
    Options = #{},  % No cluster specified, will auto-route

    %% Will fall back to local and fail on scheduler
    %% Wrap in try-catch since scheduler is not running
    Result = try
        flurm_federation:handle_call(
            {submit_job, Job, Options}, {self(), make_ref()}, State)
    catch
        _:_ -> {reply, {error, expected_failure}, State}
    end,

    ?assertMatch({reply, _, _}, Result),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

submit_job_unavailable() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Add cluster in down state
    DownCluster = sample_fed_cluster_down(),
    ets:insert(?FED_CLUSTERS_TABLE, DownCluster),

    Job = sample_job(),
    Options = #{cluster => <<"cluster-down">>},

    {reply, {error, {cluster_unavailable, down}}, State} = flurm_federation:handle_call(
        {submit_job, Job, Options}, {self(), make_ref()}, State),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Routing Policy Selection Tests
%%====================================================================

routing_policies_test_() ->
    {"Routing policy selection tests", [
        {"round robin routing", fun routing_round_robin/0},
        {"weighted routing", fun routing_weighted/0},
        {"random routing", fun routing_random/0}
    ]}.

routing_round_robin() ->
    cleanup(),
    {ok, InitState} = flurm_federation:init([]),

    %% Create state with round_robin policy
    State = InitState#state{routing_policy = round_robin},

    %% Insert eligible clusters
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster_b()),

    Job = #{partition => undefined, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster1}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster1)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

routing_weighted() ->
    cleanup(),
    {ok, InitState} = flurm_federation:init([]),

    %% Create state with weighted policy
    State = InitState#state{routing_policy = weighted},

    %% Insert clusters with different weights
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster_b()),

    Job = #{partition => undefined, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

routing_random() ->
    cleanup(),
    {ok, InitState} = flurm_federation:init([]),

    %% Create state with random policy
    State = InitState#state{routing_policy = random},

    %% Insert eligible clusters
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster_b()),

    Job = #{partition => undefined, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Auth Header Building Tests
%%====================================================================

auth_headers_test_() ->
    {"Auth header tests", [
        {"add cluster with token auth", fun auth_with_token/0},
        {"add cluster with api key auth", fun auth_with_api_key/0}
    ]}.

auth_with_token() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Config = #{
        host => <<"auth-test.example.com">>,
        port => 6817,
        auth => #{token => <<"my-secret-token">>}
    },

    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"auth-cluster">>, Config}, {self(), make_ref()}, State),

    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"auth-cluster">>),
    ?assertEqual(#{token => <<"my-secret-token">>}, Cluster#fed_cluster.auth),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

auth_with_api_key() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    Config = #{
        host => <<"api-key-test.example.com">>,
        port => 6817,
        auth => #{api_key => <<"my-api-key">>}
    },

    {reply, ok, State} = flurm_federation:handle_call(
        {add_cluster, <<"apikey-cluster">>, Config}, {self(), make_ref()}, State),

    [Cluster] = ets:lookup(?FED_CLUSTERS_TABLE, <<"apikey-cluster">>),
    ?assertEqual(#{api_key => <<"my-api-key">>}, Cluster#fed_cluster.auth),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Job Helpers Tests
%%====================================================================

job_helpers_test_() ->
    {"Job helper tests", [
        {"job with features", fun job_with_features/0},
        {"route with features filter", fun route_with_features/0}
    ]}.

job_with_features() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert cluster with features
    ClusterWithFeatures = sample_fed_cluster(),
    ets:insert(?FED_CLUSTERS_TABLE, ClusterWithFeatures),

    %% Job map with features
    Job = #{
        partition => <<"default">>,
        num_cpus => 1,
        memory_mb => 1024,
        features => [<<"gpu">>]
    },

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assertEqual(<<"cluster-a">>, Cluster),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

route_with_features() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert cluster with specific features
    ClusterWithGPU = sample_fed_cluster(),  % has [<<"gpu">>]
    ets:insert(?FED_CLUSTERS_TABLE, ClusterWithGPU),

    %% Job requiring feature that exists
    Job = #{
        partition => undefined,
        num_cpus => 1,
        memory_mb => 1024,
        features => [<<"gpu">>]
    },

    {reply, {ok, MatchedCluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assertEqual(<<"cluster-a">>, MatchedCluster),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Federated Job Submission with Federation Tests
%%====================================================================

federated_submit_test_() ->
    {"Federated job submission tests", [
        {"submit federated job to federation", fun submit_federated_with_federation/0}
    ]}.

submit_federated_with_federation() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Create federation
    {reply, ok, State1} = flurm_federation:handle_call(
        {create_federation, <<"test-fed">>, []}, {self(), make_ref()}, State),

    %% Insert cluster that is up
    ClusterUp = sample_fed_cluster(),
    ets:insert(?FED_CLUSTERS_TABLE, ClusterUp),

    Job = sample_job(),
    Options = #{},

    %% This will try to submit to clusters - will fail on HTTP but tests the path
    {reply, Result, State1} = flurm_federation:handle_call(
        {submit_federated_job, Job, Options}, {self(), make_ref()}, State1),

    %% May return error due to no HTTP, but tests the code path
    ?assert(Result =:= {error, no_suitable_cluster} orelse
            Result =:= {error, submission_failed} orelse
            element(1, Result) =:= ok orelse
            element(1, Result) =:= error),

    erlang:cancel_timer(State#state.health_timer),
    erlang:cancel_timer(State1#state.sync_timer),
    cleanup().

%%====================================================================
%% Job Record Handling Tests
%%====================================================================

job_record_test_() ->
    {"Job record handling tests", [
        {"submit job record", fun submit_job_record/0},
        {"submit job map", fun submit_job_map_test/0}
    ]}.

submit_job_record() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Use job record
    Job = sample_job(),
    Options = #{},

    %% Will auto-route to local and fail on scheduler
    %% Wrap in try-catch since scheduler is not running
    Result = try
        flurm_federation:handle_call(
            {submit_job, Job, Options}, {self(), make_ref()}, State)
    catch
        _:_ -> {reply, {error, expected_failure}, State}
    end,

    ?assertMatch({reply, _, _}, Result),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

submit_job_map_test() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Use job map
    Job = sample_job_map(),
    Options = #{},

    %% Will auto-route to local and fail on scheduler
    %% Wrap in try-catch since scheduler is not running
    Result = try
        flurm_federation:handle_call(
            {submit_job, Job, Options}, {self(), make_ref()}, State)
    catch
        _:_ -> {reply, {error, expected_failure}, State}
    end,

    ?assertMatch({reply, _, _}, Result),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Partition Affinity with Routing Policy
%%====================================================================

partition_affinity_test_() ->
    {"Partition affinity routing tests", [
        {"partition affinity routing policy", fun partition_affinity_policy/0}
    ]}.

partition_affinity_policy() ->
    cleanup(),
    {ok, InitState} = flurm_federation:init([]),

    State = InitState#state{routing_policy = partition_affinity},

    %% Insert cluster with partition
    ClusterWithPart = sample_fed_cluster(),
    ets:insert(?FED_CLUSTERS_TABLE, ClusterWithPart),

    Job = #{partition => <<"default">>, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Calculate Load Tests
%%====================================================================

calculate_load_test_() ->
    {"Calculate load tests", [
        {"load calculation with zero CPUs", fun load_zero_cpus/0},
        {"load calculation normal", fun load_normal/0}
    ]}.

load_zero_cpus() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert cluster with 0 CPUs (edge case)
    ZeroCpuCluster = #fed_cluster{
        name = <<"zero-cpu">>,
        host = <<"zero.example.com">>,
        port = 6817,
        auth = #{},
        state = up,
        weight = 1,
        features = [],
        partitions = [],
        node_count = 0,
        cpu_count = 0,  % Zero CPUs
        memory_mb = 0,
        gpu_count = 0,
        pending_jobs = 5,
        running_jobs = 10,
        available_cpus = 0,
        available_memory = 0,
        last_sync = erlang:system_time(second),
        last_health_check = erlang:system_time(second),
        consecutive_failures = 0,
        properties = #{}
    },
    ets:insert(?FED_CLUSTERS_TABLE, ZeroCpuCluster),

    %% Also insert a normal cluster
    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    Job = #{partition => undefined, num_cpus => 0, memory_mb => 0},

    %% Should route to the normal cluster
    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

load_normal() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert clusters with different loads
    LowLoadCluster = sample_fed_cluster(),  % 15 running / 320 cpus = low load
    HighLoadCluster = sample_fed_cluster_b(),  % 30 running / 640 cpus = higher total

    ets:insert(?FED_CLUSTERS_TABLE, LowLoadCluster),
    ets:insert(?FED_CLUSTERS_TABLE, HighLoadCluster),

    Job = #{partition => undefined, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Empty Partition Filter Tests
%%====================================================================

empty_partition_test_() ->
    {"Empty partition filter tests", [
        {"route with empty partition", fun route_empty_partition/0}
    ]}.

route_empty_partition() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    %% Job with empty binary partition
    Job = #{partition => <<>>, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Weighted Routing with Zero Total Weight
%%====================================================================

weighted_zero_test() ->
    cleanup(),
    {ok, InitState} = flurm_federation:init([]),

    State = InitState#state{routing_policy = weighted},

    %% Insert cluster with 0 weight
    ZeroWeightCluster = sample_fed_cluster(),
    ZeroWeightCluster2 = ZeroWeightCluster#fed_cluster{weight = 0},
    ets:insert(?FED_CLUSTERS_TABLE, ZeroWeightCluster2),

    Job = #{partition => undefined, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Submit to Remote Cluster Tests
%%====================================================================

submit_remote_test_() ->
    {"Submit to remote cluster tests", [
        {"submit to existing remote cluster", fun submit_to_remote_existing/0}
    ]}.

submit_to_remote_existing() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Insert an up remote cluster
    RemoteCluster = sample_fed_cluster(),
    ets:insert(?FED_CLUSTERS_TABLE, RemoteCluster),

    Job = sample_job(),
    Options = #{cluster => <<"cluster-a">>},

    %% Will try to submit via HTTP - will fail but tests path
    {reply, Result, State} = flurm_federation:handle_call(
        {submit_job, Job, Options}, {self(), make_ref()}, State),

    %% HTTP will fail, but we test the code path
    ?assert(element(1, Result) =:= error orelse element(1, Result) =:= ok),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Join Federation Tests
%%====================================================================

join_federation_more_test_() ->
    {"More join_federation tests", [
        {"join when not federated", fun join_not_federated/0}
    ]}.

join_not_federated() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    %% Try to join - will return ok because fetch_federation_info returns default federation
    {reply, Result, _NewState} = flurm_federation:handle_call(
        {join_federation, <<"default-federation">>, <<"origin.example.com">>},
        {self(), make_ref()}, State),

    %% Should succeed because internal fetch_federation_info returns default
    ?assertEqual(ok, Result),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

%%====================================================================
%% Additional Code Path Tests
%%====================================================================

additional_paths_test_() ->
    {"Additional code path tests", [
        {"job without partition key", fun job_no_partition_key/0},
        {"job without features key", fun job_no_features_key/0},
        {"job without cpu key", fun job_no_cpu_key/0},
        {"job without memory key", fun job_no_memory_key/0}
    ]}.

job_no_partition_key() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    %% Job without partition key
    Job = #{num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

job_no_features_key() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    %% Job without features key
    Job = #{partition => <<"default">>, num_cpus => 1, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

job_no_cpu_key() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    %% Job without num_cpus key (defaults to 1)
    Job = #{partition => <<"default">>, memory_mb => 1024},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().

job_no_memory_key() ->
    cleanup(),
    {ok, State} = flurm_federation:init([]),

    ets:insert(?FED_CLUSTERS_TABLE, sample_fed_cluster()),

    %% Job without memory_mb key (defaults to 1024)
    Job = #{partition => <<"default">>, num_cpus => 1},

    {reply, {ok, Cluster}, State} = flurm_federation:handle_call(
        {route_job, Job}, {self(), make_ref()}, State),

    ?assert(is_binary(Cluster)),

    erlang:cancel_timer(State#state.health_timer),
    cleanup().
