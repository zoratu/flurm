%%%-------------------------------------------------------------------
%%% @doc PropEr Property-Based Tests for flurm_core modules
%%%
%%% Exercises real functions using property-based testing with PropEr.
%%% Covers: flurm_tres, flurm_backfill, flurm_protocol_codec.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cover_prop_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% PropEr Options
%%====================================================================

-define(OPTS, [{numtests, 100}, {to_file, user}]).

%%====================================================================
%% Generators
%%====================================================================

%% Generate a TRES map with standard keys and non-negative integer values.
tres_map() ->
    ?LET({Cpu, Mem, Node, Gpu, JobCount, JobTime, Billing},
         {non_neg_integer(), non_neg_integer(), non_neg_integer(),
          non_neg_integer(), non_neg_integer(), non_neg_integer(),
          non_neg_integer()},
         #{cpu_seconds => Cpu,
           mem_seconds => Mem,
           node_seconds => Node,
           gpu_seconds => Gpu,
           job_count => JobCount,
           job_time => JobTime,
           billing => Billing}).

%% Generate a small non-negative TRES map (bounded to avoid overflow).
small_tres_map() ->
    ?LET({Cpu, Mem, Node, Gpu, JobCount, JobTime, Billing},
         {range(0, 1000000), range(0, 1000000), range(0, 1000000),
          range(0, 1000000), range(0, 1000000), range(0, 1000000),
          range(0, 1000000)},
         #{cpu_seconds => Cpu,
           mem_seconds => Mem,
           node_seconds => Node,
           gpu_seconds => Gpu,
           job_count => JobCount,
           job_time => JobTime,
           billing => Billing}).

%% Generate a TRES map suitable for format/parse roundtrip testing.
%% Only includes keys that survive format/parse (no job_count, job_time).
roundtrip_tres_map() ->
    ?LET({Cpu, Mem, Node, Gpu, Billing},
         {pos_integer(), pos_integer(), pos_integer(),
          pos_integer(), pos_integer()},
         #{cpu_seconds => Cpu,
           mem_seconds => Mem,
           node_seconds => Node,
           gpu_seconds => Gpu,
           billing => Billing}).

%%====================================================================
%% Property: flurm_tres:add/2 is commutative
%%====================================================================

prop_tres_add_commutative() ->
    ?FORALL({A, B}, {tres_map(), tres_map()},
        begin
            flurm_tres:add(A, B) =:= flurm_tres:add(B, A)
        end).

tres_add_commutative_test() ->
    ?assert(proper:quickcheck(prop_tres_add_commutative(), ?OPTS)).

%%====================================================================
%% Property: flurm_tres:add/2 is associative
%%====================================================================

prop_tres_add_associative() ->
    ?FORALL({A, B, C}, {small_tres_map(), small_tres_map(), small_tres_map()},
        begin
            LHS = flurm_tres:add(flurm_tres:add(A, B), C),
            RHS = flurm_tres:add(A, flurm_tres:add(B, C)),
            LHS =:= RHS
        end).

tres_add_associative_test() ->
    ?assert(proper:quickcheck(prop_tres_add_associative(), ?OPTS)).

%%====================================================================
%% Property: flurm_tres:subtract/2 floors at zero
%%====================================================================

prop_tres_subtract_floors_at_zero() ->
    ?FORALL({A, B}, {tres_map(), tres_map()},
        begin
            Result = flurm_tres:subtract(A, B),
            maps:fold(fun(_Key, Value, Acc) ->
                Acc andalso (Value >= 0)
            end, true, Result)
        end).

tres_subtract_floors_at_zero_test() ->
    ?assert(proper:quickcheck(prop_tres_subtract_floors_at_zero(), ?OPTS)).

%%====================================================================
%% Property: flurm_tres:format/1 -> flurm_tres:parse/1 roundtrip
%%
%% format/1 skips job_count and job_time keys, and skips zero-valued
%% entries, so we test on maps with only the surviving keys and
%% strictly positive values.
%%====================================================================

prop_tres_format_parse_roundtrip() ->
    ?FORALL(Map, roundtrip_tres_map(),
        begin
            Formatted = flurm_tres:format(Map),
            Parsed = flurm_tres:parse(Formatted),
            %% Extract only the keys that format emits
            RoundtripKeys = [cpu_seconds, mem_seconds, node_seconds,
                             gpu_seconds, billing],
            lists:all(fun(K) ->
                maps:get(K, Parsed, 0) =:= maps:get(K, Map, 0)
            end, RoundtripKeys)
        end).

tres_format_parse_roundtrip_test() ->
    ?assert(proper:quickcheck(prop_tres_format_parse_roundtrip(), ?OPTS)).

%%====================================================================
%% Property: flurm_tres:multiply/2 by 0 gives zero map
%%====================================================================

prop_tres_multiply_by_zero() ->
    ?FORALL(Map, tres_map(),
        begin
            Result = flurm_tres:multiply(Map, 0),
            maps:fold(fun(_K, V, Acc) ->
                Acc andalso (V =:= 0)
            end, true, Result)
        end).

tres_multiply_by_zero_test() ->
    ?assert(proper:quickcheck(prop_tres_multiply_by_zero(), ?OPTS)).

%%====================================================================
%% Property: flurm_tres:multiply/2 by 1 is identity
%%====================================================================

prop_tres_multiply_by_one() ->
    ?FORALL(Map, tres_map(),
        begin
            Result = flurm_tres:multiply(Map, 1),
            Result =:= Map
        end).

tres_multiply_by_one_test() ->
    ?assert(proper:quickcheck(prop_tres_multiply_by_one(), ?OPTS)).

%%====================================================================
%% Property: flurm_backfill:timestamp_to_seconds/1 returns integer
%%====================================================================

prop_backfill_timestamp_to_seconds_integer() ->
    ?FORALL(Ts, oneof([
            %% erlang:now()-style tuple
            ?LET({Mega, Sec, Micro},
                 {non_neg_integer(), non_neg_integer(), non_neg_integer()},
                 {Mega, Sec, Micro}),
            %% plain integer
            non_neg_integer()
        ]),
        begin
            Result = flurm_backfill:timestamp_to_seconds(Ts),
            is_integer(Result)
        end).

backfill_timestamp_to_seconds_test() ->
    ?assert(proper:quickcheck(prop_backfill_timestamp_to_seconds_integer(), ?OPTS)).

%%====================================================================
%% Property: flurm_protocol_codec:message_type_name/1 returns atom
%%           or {unknown, Type} for any uint16
%%====================================================================

prop_message_type_name_returns_atom_or_tuple() ->
    ?FORALL(Type, range(0, 65535),
        begin
            Result = flurm_protocol_codec:message_type_name(Type),
            is_atom(Result) orelse
                (is_tuple(Result) andalso
                 element(1, Result) =:= unknown andalso
                 element(2, Result) =:= Type)
        end).

message_type_name_test() ->
    ?assert(proper:quickcheck(prop_message_type_name_returns_atom_or_tuple(), ?OPTS)).

%%====================================================================
%% Property: is_request/1 and is_response/1 are mutually consistent
%%
%% For any uint16, it should NOT be the case that both is_request
%% and is_response return true simultaneously.
%%====================================================================

prop_request_response_not_both() ->
    ?FORALL(Type, range(0, 65535),
        begin
            IsReq = flurm_protocol_codec:is_request(Type),
            IsResp = flurm_protocol_codec:is_response(Type),
            %% Both must be booleans
            is_boolean(IsReq) andalso is_boolean(IsResp) andalso
            %% They must not both be true
            not (IsReq andalso IsResp)
        end).

request_response_not_both_test() ->
    ?assert(proper:quickcheck(prop_request_response_not_both(), ?OPTS)).
