%%%-------------------------------------------------------------------
%%% @doc Unit Tests for Labeled Metrics and TRES Collection
%%%
%%% Tests for:
%%% - labeled_gauge/3, labeled_counter/3, get_labeled_metric/2
%%% - format_labeled_metrics/0
%%% - collect_tres_metrics/0
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics_labeled_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Define ETS table names
-define(METRICS_TABLE, flurm_metrics).
-define(HISTOGRAM_TABLE, flurm_histograms).
-define(LABELED_TABLE, flurm_labeled_metrics).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Clean up any existing tables
    catch ets:delete(?METRICS_TABLE),
    catch ets:delete(?HISTOGRAM_TABLE),
    catch ets:delete(?LABELED_TABLE),
    %% Stop any running metrics server
    catch gen_server:stop(flurm_metrics),
    ok.

cleanup(_) ->
    catch gen_server:stop(flurm_metrics),
    catch ets:delete(?METRICS_TABLE),
    catch ets:delete(?HISTOGRAM_TABLE),
    catch ets:delete(?LABELED_TABLE),
    ok.

%% Start the metrics server for tests
start_metrics() ->
    setup(),
    {ok, Pid} = flurm_metrics:start_link(),
    Pid.

%%====================================================================
%% Labeled Metrics API Tests
%%====================================================================

labeled_metrics_test_() ->
    {foreach,
     fun start_metrics/0,
     fun cleanup/1,
     [
      {"labeled_gauge sets value correctly", fun test_labeled_gauge/0},
      {"labeled_gauge overwrites previous value", fun test_labeled_gauge_overwrite/0},
      {"labeled_counter increments correctly", fun test_labeled_counter/0},
      {"labeled_counter creates new counter if not exists", fun test_labeled_counter_new/0},
      {"get_labeled_metric returns value", fun test_get_labeled_metric/0},
      {"get_labeled_metric returns not_found for missing", fun test_get_labeled_metric_not_found/0},
      {"different labels are independent", fun test_different_labels/0},
      {"multiple label keys work", fun test_multiple_label_keys/0}
     ]}.

test_labeled_gauge() ->
    Labels = #{type => <<"cpu">>},
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels, 256),
    timer:sleep(10),  % Give gen_server time to process
    ?assertEqual({ok, 256}, flurm_metrics:get_labeled_metric(flurm_tres_total, Labels)).

test_labeled_gauge_overwrite() ->
    Labels = #{type => <<"mem">>},
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels, 1000),
    timer:sleep(10),
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels, 2000),
    timer:sleep(10),
    ?assertEqual({ok, 2000}, flurm_metrics:get_labeled_metric(flurm_tres_total, Labels)).

test_labeled_counter() ->
    Labels = #{type => <<"cpu">>},
    flurm_metrics:labeled_counter(flurm_tres_usage, Labels, 10),
    timer:sleep(10),
    flurm_metrics:labeled_counter(flurm_tres_usage, Labels, 5),
    timer:sleep(10),
    ?assertEqual({ok, 15}, flurm_metrics:get_labeled_metric(flurm_tres_usage, Labels)).

test_labeled_counter_new() ->
    Labels = #{type => <<"new_type">>},
    flurm_metrics:labeled_counter(flurm_new_counter, Labels, 42),
    timer:sleep(10),
    ?assertEqual({ok, 42}, flurm_metrics:get_labeled_metric(flurm_new_counter, Labels)).

test_get_labeled_metric() ->
    Labels = #{type => <<"gpu">>},
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels, 8),
    timer:sleep(10),
    {ok, Value} = flurm_metrics:get_labeled_metric(flurm_tres_total, Labels),
    ?assertEqual(8, Value).

test_get_labeled_metric_not_found() ->
    Labels = #{type => <<"nonexistent">>},
    ?assertEqual({error, not_found}, flurm_metrics:get_labeled_metric(flurm_missing, Labels)).

test_different_labels() ->
    Labels1 = #{type => <<"cpu">>},
    Labels2 = #{type => <<"gpu">>},
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels1, 100),
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels2, 8),
    timer:sleep(10),
    ?assertEqual({ok, 100}, flurm_metrics:get_labeled_metric(flurm_tres_total, Labels1)),
    ?assertEqual({ok, 8}, flurm_metrics:get_labeled_metric(flurm_tres_total, Labels2)).

test_multiple_label_keys() ->
    Labels = #{type => <<"gres/gpu">>, name => <<"a100">>},
    flurm_metrics:labeled_gauge(flurm_tres_configured, Labels, 1),
    timer:sleep(10),
    ?assertEqual({ok, 1}, flurm_metrics:get_labeled_metric(flurm_tres_configured, Labels)).

%%====================================================================
%% Prometheus Formatting Tests
%%====================================================================

format_labeled_test_() ->
    {foreach,
     fun start_metrics/0,
     fun cleanup/1,
     [
      {"format_prometheus includes labeled metrics", fun test_format_includes_labeled/0},
      {"format_prometheus uses correct label syntax", fun test_format_label_syntax/0},
      {"format_prometheus groups by metric name", fun test_format_groups_by_name/0}
     ]}.

test_format_includes_labeled() ->
    flurm_metrics:labeled_gauge(flurm_tres_total, #{type => <<"cpu">>}, 256),
    timer:sleep(10),
    Output = lists:flatten(flurm_metrics:format_prometheus()),
    ?assert(string:find(Output, "flurm_tres_total") =/= nomatch).

test_format_label_syntax() ->
    flurm_metrics:labeled_gauge(flurm_tres_total, #{type => <<"cpu">>}, 128),
    timer:sleep(10),
    Output = lists:flatten(flurm_metrics:format_prometheus()),
    %% Should contain {type="cpu"} syntax
    ?assert(string:find(Output, "type=\"cpu\"") =/= nomatch).

test_format_groups_by_name() ->
    flurm_metrics:labeled_gauge(flurm_tres_total, #{type => <<"cpu">>}, 100),
    flurm_metrics:labeled_gauge(flurm_tres_total, #{type => <<"gpu">>}, 8),
    timer:sleep(10),
    Output = lists:flatten(flurm_metrics:format_prometheus()),
    %% Should have only one HELP line for flurm_tres_total
    Matches = string:split(Output, "# HELP flurm_tres_total", all),
    ?assertEqual(2, length(Matches)).  % Original + 1 split = 2 parts

%%====================================================================
%% TRES Collection Tests
%%====================================================================

tres_collection_test_() ->
    {foreach,
     fun setup_tres_mocks/0,
     fun cleanup_tres_mocks/1,
     [
      {"collect_tres_metrics handles missing node registry", fun test_tres_no_registry/0},
      {"collect_tres_metrics aggregates node data", fun test_tres_aggregation/0}
     ]}.

setup_tres_mocks() ->
    setup(),
    %% Start metrics server
    {ok, _Pid} = flurm_metrics:start_link(),
    ok.

cleanup_tres_mocks(_) ->
    catch meck:unload(flurm_node_registry),
    catch meck:unload(flurm_account_manager),
    cleanup(ok).

test_tres_no_registry() ->
    %% Mock node registry to return error
    meck:new(flurm_node_registry, [non_strict]),
    meck:expect(flurm_node_registry, list_nodes, fun() -> {error, not_running} end),

    %% Mock account manager
    meck:new(flurm_account_manager, [non_strict]),
    meck:expect(flurm_account_manager, list_tres, fun() -> [] end),

    %% Should not crash
    ?assertEqual(ok, catch flurm_metrics:collect_tres_metrics()),

    meck:unload(flurm_node_registry),
    meck:unload(flurm_account_manager).

test_tres_aggregation() ->
    %% Mock node registry with test data
    meck:new(flurm_node_registry, [non_strict]),
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"node1">>, self()}, {<<"node2">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun
        (<<"node1">>) ->
            {ok, #node_entry{
                name = <<"node1">>,
                pid = self(),
                hostname = <<"node1.local">>,
                state = up,
                partitions = [<<"default">>],
                cpus_total = 16,
                cpus_avail = 8,
                memory_total = 32768,
                memory_avail = 16384,
                gpus_total = 4,
                gpus_avail = 2
            }};
        (<<"node2">>) ->
            {ok, #node_entry{
                name = <<"node2">>,
                pid = self(),
                hostname = <<"node2.local">>,
                state = up,
                partitions = [<<"default">>],
                cpus_total = 32,
                cpus_avail = 32,
                memory_total = 65536,
                memory_avail = 65536,
                gpus_total = 8,
                gpus_avail = 8
            }}
    end),

    %% Mock account manager
    meck:new(flurm_account_manager, [non_strict]),
    meck:expect(flurm_account_manager, list_tres, fun() ->
        [
            #tres{id = 1, type = <<"cpu">>, name = <<>>},
            #tres{id = 2, type = <<"mem">>, name = <<>>},
            #tres{id = 5, type = <<"gres/gpu">>, name = <<"a100">>}
        ]
    end),

    %% Trigger collection
    ok = flurm_metrics:collect_tres_metrics(),
    timer:sleep(20),

    %% Verify aggregated values
    %% CPU: 16 + 32 = 48 total, (16-8) + (32-32) = 8 allocated
    ?assertEqual({ok, 48}, flurm_metrics:get_labeled_metric(flurm_tres_total, #{type => <<"cpu">>})),
    ?assertEqual({ok, 8}, flurm_metrics:get_labeled_metric(flurm_tres_allocated, #{type => <<"cpu">>})),
    ?assertEqual({ok, 40}, flurm_metrics:get_labeled_metric(flurm_tres_idle, #{type => <<"cpu">>})),

    %% GPU: 4 + 8 = 12 total, (4-2) + (8-8) = 2 allocated
    ?assertEqual({ok, 12}, flurm_metrics:get_labeled_metric(flurm_tres_total, #{type => <<"gpu">>})),
    ?assertEqual({ok, 2}, flurm_metrics:get_labeled_metric(flurm_tres_allocated, #{type => <<"gpu">>})),

    %% Memory: 32768 + 65536 = 98304 total
    ?assertEqual({ok, 98304}, flurm_metrics:get_labeled_metric(flurm_tres_total, #{type => <<"mem">>})),

    %% Configured TRES should include custom gres
    ?assertEqual({ok, 1}, flurm_metrics:get_labeled_metric(flurm_tres_configured, #{type => <<"gres/gpu">>, name => <<"a100">>})),

    meck:unload(flurm_node_registry),
    meck:unload(flurm_account_manager).

%%====================================================================
%% Reset Tests
%%====================================================================

reset_test_() ->
    {foreach,
     fun start_metrics/0,
     fun cleanup/1,
     [
      {"reset clears labeled metrics", fun test_reset_clears_labeled/0}
     ]}.

test_reset_clears_labeled() ->
    Labels = #{type => <<"cpu">>},
    flurm_metrics:labeled_gauge(flurm_tres_total, Labels, 256),
    timer:sleep(10),
    ?assertEqual({ok, 256}, flurm_metrics:get_labeled_metric(flurm_tres_total, Labels)),

    flurm_metrics:reset(),
    timer:sleep(10),
    ?assertEqual({error, not_found}, flurm_metrics:get_labeled_metric(flurm_tres_total, Labels)).
