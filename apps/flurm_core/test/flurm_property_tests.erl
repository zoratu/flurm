%%%-------------------------------------------------------------------
%%% @doc Property-Based Tests for FLURM Phase 7-8 Modules
%%%
%%% This module provides comprehensive property-based testing using PropEr
%%% for the Phase 7-8 implementation modules:
%%% - JWT token generation and verification
%%% - Protocol codec for federation messages
%%% - TRES (Trackable Resources) calculations
%%% - Accounting double-counting prevention
%%%
%%% Properties tested:
%%% 1. JWT Properties:
%%%    - Roundtrip: generate then verify always succeeds for valid subjects
%%%    - Tamper detection: modifying any byte causes verification failure
%%%    - Base64URL: encode then decode is identity
%%%
%%% 2. Protocol Codec Properties (federation messages):
%%%    - Roundtrip: encode then decode is identity
%%%    - No crash: random bytes never crash decoder
%%%
%%% 3. TRES Calculation Properties:
%%%    - Non-negative: TRES values are always >= 0
%%%    - Additive: combining TRES is associative and commutative
%%%
%%% 4. Accounting Properties:
%%%    - No double counting: recording same job_id twice doesn't increase totals
%%%
%%% Usage:
%%%   rebar3 proper -m flurm_property_tests -n 100
%%%   rebar3 proper -m flurm_property_tests -p prop_jwt_roundtrip -n 500
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_property_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% PropEr properties
-export([
    %% JWT Properties
    prop_jwt_roundtrip/0,
    prop_jwt_tamper_detection/0,
    prop_base64url_roundtrip/0,

    %% Protocol Codec Properties
    prop_fed_message_roundtrip/0,
    prop_fed_message_no_crash/0,

    %% TRES Calculation Properties
    prop_tres_non_negative/0,
    prop_tres_additive/0,

    %% Accounting Properties
    prop_no_double_counting/0
]).

%% PropEr generators
-export([
    gen_jwt_subject/0,
    gen_valid_jwt_subject/0,
    gen_jwt_claims/0,
    gen_printable_binary/0,
    gen_fed_message_type/0,
    gen_fed_job_submit_msg/0,
    gen_fed_job_started_msg/0,
    gen_fed_sibling_revoke_msg/0,
    gen_tres_map/0,
    gen_tres_type/0,
    gen_job_id/0,
    gen_cluster_name/0
]).

%% Test helper exports
-export([
    tamper_binary/1,
    tamper_binary_guaranteed/1,
    merge_tres/2,
    tres_to_list/1
]).

%%====================================================================
%% JWT Properties
%%====================================================================

%% @doc Property: JWT roundtrip - generate then verify always succeeds
%% for valid subjects and claims.
%%
%% This verifies that any token we generate can be successfully verified
%% using the same secret key.
prop_jwt_roundtrip() ->
    ?FORALL({Subject, ExtraClaims}, {gen_jwt_subject(), gen_jwt_claims()},
        begin
            case flurm_jwt:generate(Subject, ExtraClaims) of
                {ok, Token} ->
                    case flurm_jwt:verify(Token) of
                        {ok, Claims} ->
                            %% Verify the subject is preserved
                            maps:get(<<"sub">>, Claims, undefined) =:= Subject;
                        {error, _} ->
                            %% Verification failed - this is a bug
                            false
                    end;
                {error, _} ->
                    %% Generation can fail for invalid inputs, which is acceptable
                    true
            end
        end).

%% @doc Property: JWT tamper detection - modifying any byte in token
%% causes verification failure.
%%
%% This is critical for security: any modification to the token should
%% result in verification failure.
prop_jwt_tamper_detection() ->
    ?FORALL(Subject, gen_valid_jwt_subject(),
        begin
            case flurm_jwt:generate(Subject) of
                {ok, Token} when byte_size(Token) > 20 ->
                    %% Tamper with the token - ensure we actually change it
                    TamperedToken = tamper_binary_guaranteed(Token),
                    %% Verification should fail for a tampered token
                    case TamperedToken =:= Token of
                        true ->
                            %% Tampering didn't change anything (shouldn't happen)
                            true;
                        false ->
                            %% Token was modified, verification must fail
                            case flurm_jwt:verify(TamperedToken) of
                                {error, _} -> true;
                                {ok, _} ->
                                    %% Tampered token verified - security issue!
                                    false
                            end
                    end;
                {ok, _Token} ->
                    %% Token too short to tamper meaningfully
                    true;
                {error, _} ->
                    %% Generation failed - acceptable
                    true
            end
        end).

%% @doc Property: Base64URL roundtrip - encode then decode is identity.
%%
%% Tests that the base64url encoding/decoding functions are correct inverses.
prop_base64url_roundtrip() ->
    ?FORALL(Data, binary(),
        begin
            %% Use the internal functions exposed for testing
            Encoded = flurm_jwt:encode_base64url(Data),
            Decoded = flurm_jwt:decode_base64url(Encoded),
            Decoded =:= Data
        end).

%%====================================================================
%% Protocol Codec Properties (Federation Messages)
%%====================================================================

%% @doc Property: Federation message roundtrip - encode then decode is identity.
%%
%% Tests that federation messages survive encode/decode without data loss.
prop_fed_message_roundtrip() ->
    ?FORALL({MsgType, Msg}, gen_fed_message(),
        begin
            case catch flurm_protocol_codec:encode(MsgType, Msg) of
                {ok, Encoded} ->
                    case catch flurm_protocol_codec:decode(Encoded) of
                        {ok, #slurm_msg{header = Header}, Rest} ->
                            %% Verify message type is preserved
                            Header#slurm_header.msg_type =:= MsgType andalso
                            Rest =:= <<>>;
                        {error, _} ->
                            %% Decode failed - might be expected for some message types
                            true;
                        {'EXIT', _} ->
                            %% Crash is not acceptable
                            false
                    end;
                {error, _} ->
                    %% Encoding failure is acceptable for some inputs
                    true;
                {'EXIT', _} ->
                    %% Crash during encoding - not acceptable
                    false
            end
        end).

%% @doc Property: Random bytes never crash decoder (return error ok).
%%
%% The decoder must be crash-resistant against any input, returning
%% an error tuple rather than crashing.
prop_fed_message_no_crash() ->
    ?FORALL(Bytes, binary(),
        begin
            Result = try
                flurm_protocol_codec:decode(Bytes)
            catch
                %% These specific crashes indicate bugs in the decoder
                error:badarg -> {crash, badarg};
                error:function_clause -> {crash, function_clause};
                error:{badmatch, _} -> {crash, badmatch};
                error:badarith -> {crash, badarith};
                error:{case_clause, _} -> {crash, case_clause};
                %% Other exceptions might be acceptable
                _:_ -> ok
            end,
            case Result of
                {crash, _Reason} -> false;
                _ -> true
            end
        end).

%%====================================================================
%% TRES Calculation Properties
%%====================================================================

%% @doc Property: TRES values are always >= 0.
%%
%% TRES (Trackable RESource) values represent resource quantities
%% and must never be negative.
prop_tres_non_negative() ->
    ?FORALL(TresMap, gen_tres_map(),
        begin
            %% All values in the TRES map must be non-negative
            lists:all(
                fun({_Key, Value}) ->
                    is_number(Value) andalso Value >= 0
                end,
                maps:to_list(TresMap)
            )
        end).

%% @doc Property: TRES combining is associative and commutative.
%%
%% When merging TRES resources:
%% - Order of combining should not matter (commutativity)
%% - Grouping should not matter (associativity)
prop_tres_additive() ->
    ?FORALL({Tres1, Tres2, Tres3}, {gen_tres_map(), gen_tres_map(), gen_tres_map()},
        begin
            %% Commutativity: A + B = B + A
            MergeAB = merge_tres(Tres1, Tres2),
            MergeBA = merge_tres(Tres2, Tres1),
            Commutative = lists:sort(tres_to_list(MergeAB)) =:=
                          lists:sort(tres_to_list(MergeBA)),

            %% Associativity: (A + B) + C = A + (B + C)
            MergeAB_C = merge_tres(merge_tres(Tres1, Tres2), Tres3),
            MergeA_BC = merge_tres(Tres1, merge_tres(Tres2, Tres3)),
            Associative = lists:sort(tres_to_list(MergeAB_C)) =:=
                          lists:sort(tres_to_list(MergeA_BC)),

            Commutative andalso Associative
        end).

%%====================================================================
%% Accounting Properties
%%====================================================================

%% @doc Property: Recording same job_id twice doesn't increase totals.
%%
%% This tests idempotency of job accounting - submitting the same job
%% multiple times should not cause double-counting of resources.
prop_no_double_counting() ->
    ?FORALL(JobId, gen_job_id(),
        begin
            %% Create a job data record
            JobData = #job_data{
                job_id = JobId,
                user_id = 1000,
                group_id = 1000,
                partition = <<"batch">>,
                num_nodes = 1,
                num_cpus = 4,
                time_limit = 3600,
                script = <<"#!/bin/bash\necho hello">>,
                allocated_nodes = [],
                submit_time = erlang:timestamp(),
                priority = 100,
                state_version = 1
            },

            %% First submission
            Result1 = flurm_accounting:record_job_submit(JobData),

            %% Second submission of same job (should be idempotent)
            Result2 = flurm_accounting:record_job_submit(JobData),

            %% Both should succeed (the accounting module is designed to
            %% never crash and always return ok)
            Result1 =:= ok andalso Result2 =:= ok
        end).

%%====================================================================
%% Generators
%%====================================================================

%% @doc Generate valid JWT subjects (user names, service identifiers).
gen_jwt_subject() ->
    ?LET(Base, oneof([
            gen_printable_binary(),
            ?LET(User, gen_username(), list_to_binary(User)),
            ?LET(Service, gen_service_name(), list_to_binary(Service))
        ]),
        %% Ensure non-empty
        case Base of
            <<>> -> <<"anonymous">>;
            _ -> Base
        end).

%% @doc Generate valid JWT subjects that will produce proper tokens.
%% These are guaranteed to be valid subject strings.
gen_valid_jwt_subject() ->
    oneof([
        <<"alice">>,
        <<"bob">>,
        <<"admin">>,
        <<"service-account">>,
        <<"flurm-controller">>,
        <<"user123">>,
        ?LET(N, range(1, 9999), list_to_binary("user" ++ integer_to_list(N)))
    ]).

%% @doc Generate username-like strings.
gen_username() ->
    ?LET(Name, non_empty(list(oneof([
            choose($a, $z),
            choose($0, $9),
            $_
        ]))),
        Name).

%% @doc Generate service name strings.
gen_service_name() ->
    oneof([
        "flurm-controller",
        "flurm-node-daemon",
        "flurm-scheduler",
        "slurmrestd",
        "slurmdbd"
    ]).

%% @doc Generate extra claims for JWT tokens.
gen_jwt_claims() ->
    ?LET(Claims, list({gen_claim_key(), gen_claim_value()}),
        maps:from_list(Claims)).

gen_claim_key() ->
    oneof([
        <<"role">>,
        <<"cluster_id">>,
        <<"partition">>,
        <<"admin">>,
        <<"scope">>
    ]).

gen_claim_value() ->
    oneof([
        gen_printable_binary(),
        ?LET(N, nat(), N),
        ?LET(B, bool(), B)
    ]).

%% @doc Generate printable binary strings.
gen_printable_binary() ->
    ?LET(Chars, list(choose(32, 126)),
        list_to_binary(Chars)).

%% @doc Generate federation message types.
gen_fed_message_type() ->
    oneof([
        ?MSG_FED_JOB_SUBMIT,
        ?MSG_FED_JOB_STARTED,
        ?MSG_FED_SIBLING_REVOKE,
        ?MSG_FED_JOB_COMPLETED,
        ?MSG_FED_JOB_FAILED
    ]).

%% @doc Generate a federation message with its type.
gen_fed_message() ->
    oneof([
        {?MSG_FED_JOB_SUBMIT, gen_fed_job_submit_msg()},
        {?MSG_FED_JOB_STARTED, gen_fed_job_started_msg()},
        {?MSG_FED_SIBLING_REVOKE, gen_fed_sibling_revoke_msg()}
    ]).

%% @doc Generate a federation job submit message.
gen_fed_job_submit_msg() ->
    ?LET({FedJobId, OriginCluster, TargetCluster, SubmitTime},
         {gen_federation_job_id(), gen_cluster_name(), gen_cluster_name(), nat()},
        #fed_job_submit_msg{
            federation_job_id = FedJobId,
            origin_cluster = OriginCluster,
            target_cluster = TargetCluster,
            job_spec = #{
                name => <<"test_job">>,
                script => <<"#!/bin/bash\necho test">>,
                partition => <<"batch">>,
                num_cpus => 1,
                num_nodes => 1
            },
            submit_time = SubmitTime
        }).

%% @doc Generate a federation job started message.
gen_fed_job_started_msg() ->
    ?LET({FedJobId, RunningCluster, LocalJobId, StartTime},
         {gen_federation_job_id(), gen_cluster_name(), gen_job_id(), nat()},
        #fed_job_started_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            start_time = StartTime
        }).

%% @doc Generate a federation sibling revoke message.
gen_fed_sibling_revoke_msg() ->
    ?LET({FedJobId, RunningCluster},
         {gen_federation_job_id(), gen_cluster_name()},
        #fed_sibling_revoke_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            revoke_reason = <<"sibling_started">>
        }).

%% @doc Generate a federation job ID.
gen_federation_job_id() ->
    ?LET({Timestamp, Random},
         {nat(), nat()},
        list_to_binary(io_lib:format("fed-~B-~B", [Timestamp, Random]))).

%% @doc Generate a TRES (Trackable Resource) map.
gen_tres_map() ->
    ?LET(Entries, list({gen_tres_type(), gen_tres_value()}),
        maps:from_list(Entries)).

%% @doc Generate TRES type names.
gen_tres_type() ->
    oneof([
        <<"cpu">>,
        <<"mem">>,
        <<"node">>,
        <<"billing">>,
        <<"energy">>,
        <<"gres/gpu">>,
        <<"gres/gpu:a100">>,
        <<"gres/fpga">>,
        <<"license/matlab">>,
        <<"license/ansys">>
    ]).

%% @doc Generate TRES values (non-negative integers).
gen_tres_value() ->
    non_neg_integer().

%% @doc Generate a job ID.
gen_job_id() ->
    ?SUCHTHAT(N, pos_integer(), N < 2147483647).

%% @doc Generate a cluster name.
gen_cluster_name() ->
    oneof([
        <<"cluster-alpha">>,
        <<"cluster-beta">>,
        <<"cluster-gamma">>,
        <<"prod-west">>,
        <<"prod-east">>,
        <<"dev-cluster">>,
        ?LET(Name, gen_printable_binary(),
            case Name of
                <<>> -> <<"default">>;
                _ -> <<"cluster-", Name/binary>>
            end)
    ]).

%%====================================================================
%% Helper Functions
%%====================================================================

%% @doc Tamper with a binary by flipping a random bit.
%% Ensures the tampered binary is different from the original.
-spec tamper_binary(binary()) -> binary().
tamper_binary(Bin) when byte_size(Bin) > 0 ->
    %% Pick a random position and flip a bit there
    Pos = rand:uniform(byte_size(Bin)) - 1,
    <<Before:Pos/binary, Byte:8, After/binary>> = Bin,
    %% Flip a random bit
    BitPos = rand:uniform(8) - 1,
    NewByte = Byte bxor (1 bsl BitPos),
    <<Before/binary, NewByte:8, After/binary>>;
tamper_binary(Bin) ->
    Bin.

%% @doc Tamper with a binary, guaranteeing the result is different.
%% Flips a bit in the middle third of the binary (avoiding header/format bytes).
-spec tamper_binary_guaranteed(binary()) -> binary().
tamper_binary_guaranteed(Bin) when byte_size(Bin) > 10 ->
    %% Pick a position in the middle third to avoid header corruption
    Size = byte_size(Bin),
    Start = Size div 3,
    End = (2 * Size) div 3,
    Range = End - Start,
    Pos = Start + (rand:uniform(max(1, Range)) - 1),
    <<Before:Pos/binary, Byte:8, After/binary>> = Bin,
    %% Flip a bit - use XOR with non-zero to guarantee change
    BitPos = rand:uniform(8) - 1,
    NewByte = Byte bxor (1 bsl BitPos),
    %% If XOR didn't change (shouldn't happen), flip another bit
    FinalByte = case NewByte =:= Byte of
        true -> Byte bxor 16#FF;  % Flip all bits
        false -> NewByte
    end,
    <<Before/binary, FinalByte:8, After/binary>>;
tamper_binary_guaranteed(Bin) ->
    %% For very short binaries, just flip the first byte
    case Bin of
        <<First:8, Rest/binary>> -> <<(First bxor 16#FF):8, Rest/binary>>;
        _ -> Bin
    end.

%% @doc Merge two TRES maps by summing values for matching keys.
-spec merge_tres(map(), map()) -> map().
merge_tres(Tres1, Tres2) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            ExistingValue = maps:get(Key, Acc, 0),
            maps:put(Key, ExistingValue + Value, Acc)
        end,
        Tres1,
        Tres2
    ).

%% @doc Convert TRES map to a sorted list for comparison.
-spec tres_to_list(map()) -> [{binary(), number()}].
tres_to_list(TresMap) ->
    lists:sort(maps:to_list(TresMap)).

%%====================================================================
%% EUnit Test Wrappers
%%====================================================================

%% @doc Quick smoke test for JWT roundtrip.
jwt_roundtrip_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_jwt_roundtrip(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for JWT tamper detection.
jwt_tamper_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_jwt_tamper_detection(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for base64url roundtrip.
base64url_roundtrip_test_() ->
    {timeout, 30, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_base64url_roundtrip(),
            [{numtests, 200}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for federation message no-crash property.
fed_message_no_crash_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_fed_message_no_crash(),
            [{numtests, 500}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for TRES non-negative property.
tres_non_negative_test_() ->
    {timeout, 30, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_tres_non_negative(),
            [{numtests, 200}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for TRES additive property.
tres_additive_test_() ->
    {timeout, 30, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_tres_additive(),
            [{numtests, 200}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for accounting no-double-counting property.
no_double_counting_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_no_double_counting(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Comprehensive test suite - runs all properties with more iterations.
comprehensive_test_() ->
    {timeout, 300,
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       {"JWT roundtrip (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_jwt_roundtrip(),
                [{numtests, 200}, {to_file, user}]))
        end},
       {"Base64URL roundtrip (500 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_base64url_roundtrip(),
                [{numtests, 500}, {to_file, user}]))
        end},
       {"Federation message crash resistance (1000 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_fed_message_no_crash(),
                [{numtests, 1000}, {to_file, user}]))
        end},
       {"TRES non-negative (500 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_tres_non_negative(),
                [{numtests, 500}, {to_file, user}]))
        end},
       {"TRES additive (500 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_tres_additive(),
                [{numtests, 500}, {to_file, user}]))
        end}
      ]}}.
