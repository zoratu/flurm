%%%-------------------------------------------------------------------
%%% @doc FLURM TRES (Trackable RESource) Utilities
%%%
%%% Centralized module for TRES calculation and manipulation.
%%% TRES types: cpu, mem, node, gpu, billing, license, etc.
%%%
%%% Consolidates TRES logic previously spread across:
%%% - flurm_dbd_server:calculate_tres_usage/1
%%% - flurm_dbd_ra:calculate_tres_from_job/1, aggregate_tres/2
%%% - flurm_qos:combine_tres/2
%%% - flurm_account_manager:combine_tres_maps/2
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_tres).

%% API
-export([
    add/2,
    subtract/2,
    zero/0,
    from_job/1,
    from_job/4,
    format/1,
    parse/1,
    multiply/2,
    is_empty/1,
    exceeds/2,
    keys/0
]).

%%====================================================================
%% Types
%%====================================================================

-type tres_map() :: #{atom() => non_neg_integer()}.
-export_type([tres_map/0]).

%%====================================================================
%% Constants
%%====================================================================

%% Standard TRES keys
-define(TRES_KEYS, [cpu_seconds, mem_seconds, node_seconds, gpu_seconds,
                    job_count, job_time, billing]).

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Return list of standard TRES keys.
-spec keys() -> [atom()].
keys() ->
    ?TRES_KEYS.

%% @doc Add two TRES maps together, combining values for matching keys.
%% Replaces aggregate_tres/2, combine_tres/2, combine_tres_maps/2.
-spec add(tres_map(), tres_map()) -> tres_map().
add(Map1, Map2) when is_map(Map1), is_map(Map2) ->
    maps:fold(fun(K, V, Acc) ->
        maps:update_with(K, fun(Existing) -> Existing + V end, V, Acc)
    end, Map1, Map2).

%% @doc Subtract Map2 from Map1. Values are floored at 0.
-spec subtract(tres_map(), tres_map()) -> tres_map().
subtract(Map1, Map2) when is_map(Map1), is_map(Map2) ->
    maps:fold(fun(K, V, Acc) ->
        maps:update_with(K, fun(Existing) -> max(0, Existing - V) end, 0, Acc)
    end, Map1, Map2).

%% @doc Return an empty TRES map with zero values.
-spec zero() -> tres_map().
zero() ->
    #{
        cpu_seconds => 0,
        mem_seconds => 0,
        node_seconds => 0,
        gpu_seconds => 0,
        job_count => 0,
        job_time => 0,
        billing => 0
    }.

%% @doc Calculate TRES usage from a map containing job details.
%% Expects map with keys: elapsed, num_cpus, req_mem, num_nodes, tres_alloc.
%% Replaces calculate_tres_usage/1 and calculate_tres_from_job/1.
-spec from_job(map()) -> tres_map().
from_job(JobInfo) when is_map(JobInfo) ->
    Elapsed = maps:get(elapsed, JobInfo, 0),
    NumCpus = maps:get(num_cpus, JobInfo, 0),
    ReqMem = maps:get(req_mem, JobInfo, 0),
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    TresAlloc = maps:get(tres_alloc, JobInfo, #{}),
    GpuCount = maps:get(gpu, TresAlloc, 0),
    #{
        cpu_seconds => Elapsed * NumCpus,
        mem_seconds => Elapsed * ReqMem,
        node_seconds => Elapsed * NumNodes,
        gpu_seconds => Elapsed * GpuCount,
        job_count => 1,
        job_time => Elapsed,
        billing => calculate_billing(NumCpus, ReqMem, GpuCount, Elapsed)
    }.

%% @doc Calculate TRES from explicit parameters (simpler interface).
-spec from_job(Elapsed :: non_neg_integer(),
               NumCpus :: non_neg_integer(),
               ReqMem :: non_neg_integer(),
               NumNodes :: non_neg_integer()) -> tres_map().
from_job(Elapsed, NumCpus, ReqMem, NumNodes) ->
    #{
        cpu_seconds => Elapsed * NumCpus,
        mem_seconds => Elapsed * ReqMem,
        node_seconds => Elapsed * NumNodes,
        gpu_seconds => 0,
        job_count => 1,
        job_time => Elapsed,
        billing => calculate_billing(NumCpus, ReqMem, 0, Elapsed)
    }.

%% @doc Format TRES map as a SLURM-compatible string.
%% Output format: "cpu=100,mem=1024M,node=2,billing=150"
-spec format(tres_map()) -> binary().
format(TresMap) when is_map(TresMap) ->
    Parts = lists:filtermap(
        fun({Key, Value}) when Value > 0 ->
            KeyStr = format_key(Key),
            case KeyStr of
                skip -> false;
                _ -> {true, io_lib:format("~s=~B", [KeyStr, Value])}
            end;
           (_) -> false
        end,
        maps:to_list(TresMap)
    ),
    iolist_to_binary(lists:join(",", Parts)).

%% @doc Parse a TRES string into a map.
%% Input format: "cpu=100,mem=1024,node=2"
-spec parse(binary() | string()) -> tres_map().
parse(<<>>) ->
    zero();
parse(TresString) when is_binary(TresString) ->
    parse(binary_to_list(TresString));
parse(TresString) when is_list(TresString) ->
    Parts = string:tokens(TresString, ","),
    lists:foldl(fun(Part, Acc) ->
        case string:tokens(Part, "=") of
            [KeyStr, ValueStr] ->
                Key = parse_key(string:trim(KeyStr)),
                Value = list_to_integer(string:trim(ValueStr)),
                maps:put(Key, Value, Acc);
            _ ->
                Acc
        end
    end, zero(), Parts).

%% @doc Multiply all TRES values by a factor.
-spec multiply(tres_map(), number()) -> tres_map().
multiply(TresMap, Factor) when is_map(TresMap), is_number(Factor) ->
    maps:map(fun(_K, V) -> round(V * Factor) end, TresMap).

%% @doc Check if TRES map is empty (all zeros).
-spec is_empty(tres_map()) -> boolean().
is_empty(TresMap) when is_map(TresMap) ->
    maps:fold(fun(_K, V, Acc) -> Acc andalso V =:= 0 end, true, TresMap).

%% @doc Check if any value in Map1 exceeds the corresponding limit in Limits.
%% Returns list of {Key, Current, Limit} for exceeded limits.
-spec exceeds(tres_map(), tres_map()) -> [{atom(), non_neg_integer(), non_neg_integer()}].
exceeds(Current, Limits) when is_map(Current), is_map(Limits) ->
    maps:fold(fun(Key, Limit, Acc) when Limit > 0 ->
        CurrentVal = maps:get(Key, Current, 0),
        case CurrentVal > Limit of
            true -> [{Key, CurrentVal, Limit} | Acc];
            false -> Acc
        end;
       (_, _, Acc) -> Acc
    end, [], Limits).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Calculate billing TRES using configurable weights.
%% Default formula: CPU + (Mem_GB * 0.25) + (GPU * 2.0)
calculate_billing(NumCpus, MemMb, GpuCount, Elapsed) ->
    CpuWeight = get_billing_weight(cpu, 1.0),
    MemWeight = get_billing_weight(mem, 0.25),
    GpuWeight = get_billing_weight(gpu, 2.0),

    MemGb = MemMb / 1024,
    BillingUnits = NumCpus * CpuWeight + MemGb * MemWeight + GpuCount * GpuWeight,
    round(BillingUnits * Elapsed).

%% @private Get billing weight from configuration.
get_billing_weight(Type, Default) ->
    case application:get_env(flurm_core, tres_billing_weights) of
        {ok, Weights} when is_map(Weights) ->
            maps:get(Type, Weights, Default);
        _ ->
            Default
    end.

%% @private Format a TRES key for output string.
format_key(cpu_seconds) -> "cpu";
format_key(mem_seconds) -> "mem";
format_key(node_seconds) -> "node";
format_key(gpu_seconds) -> "gres/gpu";
format_key(billing) -> "billing";
format_key(job_count) -> skip;  % Don't include in formatted output
format_key(job_time) -> skip;   % Don't include in formatted output
format_key(Key) when is_atom(Key) -> atom_to_list(Key).

%% @private Parse a TRES key from string.
parse_key("cpu") -> cpu_seconds;
parse_key("mem") -> mem_seconds;
parse_key("node") -> node_seconds;
parse_key("gres/gpu") -> gpu_seconds;
parse_key("gpu") -> gpu_seconds;
parse_key("billing") -> billing;
parse_key("job_count") -> job_count;
parse_key("job_time") -> job_time;
parse_key(Other) -> list_to_atom(Other).
