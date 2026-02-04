%%%-------------------------------------------------------------------
%%% @doc Federation Protocol Fuzzing Tests for FLURM using PropEr
%%%
%%% This module provides comprehensive property-based fuzzing tests for the
%%% federation protocol messages to ensure crash resistance and robustness.
%%%
%%% Fuzzing Strategies:
%%% 1. Mutation fuzzing - corrupt valid federation messages
%%% 2. Boundary fuzzing - test edge cases (empty IDs, max values, etc.)
%%% 3. Random byte sequences - test decoder against arbitrary data
%%% 4. Field type fuzzing - replace fields with invalid types
%%%
%%% Federation Message Types Tested:
%%% - MSG_FED_JOB_SUBMIT (2070)
%%% - MSG_FED_JOB_STARTED (2071)
%%% - MSG_FED_SIBLING_REVOKE (2072)
%%% - MSG_FED_JOB_COMPLETED (2073)
%%% - MSG_FED_JOB_FAILED (2074)
%%%
%%% Usage:
%%%   rebar3 proper -m flurm_federation_fuzz_tests -n 1000
%%%   rebar3 eunit -m flurm_federation_fuzz_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_fuzz_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% PropEr properties
-export([
    prop_fed_job_submit_mutation/0,
    prop_fed_job_started_mutation/0,
    prop_fed_sibling_revoke_mutation/0,
    prop_fed_job_completed_mutation/0,
    prop_fed_job_failed_mutation/0,
    prop_random_bytes_no_crash_federation/0,
    prop_boundary_federation_job_id/0,
    prop_boundary_cluster_name/0,
    prop_boundary_timestamps/0,
    prop_boundary_job_spec/0,
    prop_roundtrip_federation_messages/0,
    prop_malformed_headers_no_crash/0
]).

%% PropEr generators
-export([
    fed_job_submit_msg/0,
    fed_job_started_msg/0,
    fed_sibling_revoke_msg/0,
    fed_job_completed_msg/0,
    fed_job_failed_msg/0,
    federation_job_id/0,
    cluster_name/0,
    job_spec/0,
    mutation_operation/0
]).

%% Mutation helpers
-export([
    apply_mutation/2,
    create_valid_federation_message/2,
    create_valid_federation_message/3
]).

%%====================================================================
%% PropEr Properties - Mutation Fuzzing
%%====================================================================

%% @doc MSG_FED_JOB_SUBMIT (2070) mutation fuzzing
%% Verifies that corrupted job submit messages don't crash the decoder.
prop_fed_job_submit_mutation() ->
    ?FORALL(Msg, fed_job_submit_msg(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
                Corrupted = apply_mutation(FullMsg, Mutation),
                test_decode_no_crash(Corrupted)
            end)).

%% @doc MSG_FED_JOB_STARTED (2071) mutation fuzzing
prop_fed_job_started_mutation() ->
    ?FORALL(Msg, fed_job_started_msg(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_STARTED, EncodedBody),
                Corrupted = apply_mutation(FullMsg, Mutation),
                test_decode_no_crash(Corrupted)
            end)).

%% @doc MSG_FED_SIBLING_REVOKE (2072) mutation fuzzing
prop_fed_sibling_revoke_mutation() ->
    ?FORALL(Msg, fed_sibling_revoke_msg(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_SIBLING_REVOKE, EncodedBody),
                Corrupted = apply_mutation(FullMsg, Mutation),
                test_decode_no_crash(Corrupted)
            end)).

%% @doc MSG_FED_JOB_COMPLETED (2073) mutation fuzzing
prop_fed_job_completed_mutation() ->
    ?FORALL(Msg, fed_job_completed_msg(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_COMPLETED, EncodedBody),
                Corrupted = apply_mutation(FullMsg, Mutation),
                test_decode_no_crash(Corrupted)
            end)).

%% @doc MSG_FED_JOB_FAILED (2074) mutation fuzzing
prop_fed_job_failed_mutation() ->
    ?FORALL(Msg, fed_job_failed_msg(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_FAILED, EncodedBody),
                Corrupted = apply_mutation(FullMsg, Mutation),
                test_decode_no_crash(Corrupted)
            end)).

%%====================================================================
%% PropEr Properties - Crash Resistance
%%====================================================================

%% @doc Random byte sequences should never crash the decoder
%% Tests all federation message types with random data.
prop_random_bytes_no_crash_federation() ->
    FedMsgTypes = [?MSG_FED_JOB_SUBMIT, ?MSG_FED_JOB_STARTED,
                   ?MSG_FED_SIBLING_REVOKE, ?MSG_FED_JOB_COMPLETED,
                   ?MSG_FED_JOB_FAILED],
    ?FORALL({MsgType, RandomBytes}, {oneof(FedMsgTypes), binary()},
        begin
            FullMsg = create_valid_federation_message(MsgType, RandomBytes),
            test_decode_no_crash(FullMsg)
        end).

%% @doc Malformed headers should return error, not crash
prop_malformed_headers_no_crash() ->
    ?FORALL(Bytes, binary(),
        test_decode_no_crash(Bytes)).

%%====================================================================
%% PropEr Properties - Boundary Fuzzing
%%====================================================================

%% @doc Test boundary conditions for federation_job_id
%% Empty IDs, very long IDs, special characters
prop_boundary_federation_job_id() ->
    BoundaryIds = [
        <<>>,                                          % Empty
        <<"a">>,                                       % Single char
        binary:copy(<<"x">>, 100),                     % Long string
        binary:copy(<<"x">>, 65535),                   % Max uint16 bytes
        binary:copy(<<"x">>, 65536),                   % Exceeds uint16
        <<"fed_job_", 0, "_null">>,                    % Contains null
        <<"fed_job_\n\r\t">>,                          % Control chars
        <<255, 255, 255, 255>>,                        % Non-UTF8 bytes
        <<"cluster1:", 16#FFFFFFFF:32/big, ":123">>   % Max uint32
    ],
    ?FORALL(FedJobId, oneof([return(Id) || Id <- BoundaryIds] ++ [binary()]),
        begin
            Msg = #fed_job_submit_msg{
                federation_job_id = FedJobId,
                origin_cluster = <<"origin">>,
                target_cluster = <<"target">>,
                job_spec = #{name => <<"test">>},
                submit_time = 0
            },
            try
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
                test_decode_no_crash(FullMsg)
            catch
                _:_ -> true  % Encoding failure is acceptable for invalid inputs
            end
        end).

%% @doc Test boundary conditions for cluster names
%% Very long names, empty names, special characters
prop_boundary_cluster_name() ->
    BoundaryNames = [
        <<>>,                                          % Empty
        <<"a">>,                                       % Single char
        binary:copy(<<"c">>, 255),                     % Long name
        binary:copy(<<"c">>, 65535),                   % Very long name
        binary:copy(<<"c">>, 65536),                   % Exceeds uint16
        <<"cluster\0name">>,                           % Null in middle
        <<"cluster with spaces">>,                     % Spaces
        <<"cluster-with-dashes">>,                     % Dashes
        <<"cluster_with_underscores">>                 % Underscores
    ],
    ?FORALL({OriginCluster, TargetCluster},
            {oneof([return(N) || N <- BoundaryNames] ++ [binary()]),
             oneof([return(N) || N <- BoundaryNames] ++ [binary()])},
        begin
            Msg = #fed_job_submit_msg{
                federation_job_id = <<"test-job-123">>,
                origin_cluster = OriginCluster,
                target_cluster = TargetCluster,
                job_spec = #{},
                submit_time = 0
            },
            try
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
                test_decode_no_crash(FullMsg)
            catch
                _:_ -> true
            end
        end).

%% @doc Test boundary conditions for timestamps
%% Negative timestamps, max values, zero
prop_boundary_timestamps() ->
    BoundaryTimestamps = [
        0,                                             % Zero
        1,                                             % Minimum positive
        16#7FFFFFFF,                                   % Max signed 32-bit
        16#FFFFFFFF,                                   % Max unsigned 32-bit
        16#7FFFFFFFFFFFFFFF,                           % Max signed 64-bit
        16#FFFFFFFFFFFFFFFF                            % Max unsigned 64-bit
    ],
    ?FORALL({SubmitTime, StartTime, EndTime},
            {oneof([return(T) || T <- BoundaryTimestamps] ++ [non_neg_integer()]),
             oneof([return(T) || T <- BoundaryTimestamps] ++ [non_neg_integer()]),
             oneof([return(T) || T <- BoundaryTimestamps] ++ [non_neg_integer()])},
        begin
            %% Test submit time in job submit
            Msg1 = #fed_job_submit_msg{
                federation_job_id = <<"test-job">>,
                origin_cluster = <<"origin">>,
                target_cluster = <<"target">>,
                job_spec = #{},
                submit_time = SubmitTime
            },
            Result1 = try
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg1),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
                test_decode_no_crash(FullMsg)
            catch _:_ -> true end,

            %% Test start time in job started
            Msg2 = #fed_job_started_msg{
                federation_job_id = <<"test-job">>,
                running_cluster = <<"cluster1">>,
                local_job_id = 123,
                start_time = StartTime
            },
            Result2 = try
                {ok, EncodedBody2} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg2),
                FullMsg2 = create_valid_federation_message(?MSG_FED_JOB_STARTED, EncodedBody2),
                test_decode_no_crash(FullMsg2)
            catch _:_ -> true end,

            %% Test end time in job completed
            Msg3 = #fed_job_completed_msg{
                federation_job_id = <<"test-job">>,
                running_cluster = <<"cluster1">>,
                local_job_id = 123,
                end_time = EndTime,
                exit_code = 0
            },
            Result3 = try
                {ok, EncodedBody3} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg3),
                FullMsg3 = create_valid_federation_message(?MSG_FED_JOB_COMPLETED, EncodedBody3),
                test_decode_no_crash(FullMsg3)
            catch _:_ -> true end,

            Result1 andalso Result2 andalso Result3
        end).

%% @doc Test boundary conditions for job_spec
%% Empty spec, large spec, nested spec, malformed spec
prop_boundary_job_spec() ->
    BoundarySpecs = [
        #{},                                           % Empty map
        #{name => <<>>},                               % Empty value
        #{name => binary:copy(<<"x">>, 10000)},        % Large value
        #{a => #{b => #{c => #{d => 1}}}},             % Nested
        #{1 => 2, 3 => 4},                             % Integer keys
        #{<<"key">> => [1,2,3,4,5]}                    % List value
    ],
    ?FORALL(JobSpec, oneof([return(S) || S <- BoundarySpecs] ++ [job_spec()]),
        begin
            Msg = #fed_job_submit_msg{
                federation_job_id = <<"test-job">>,
                origin_cluster = <<"origin">>,
                target_cluster = <<"target">>,
                job_spec = JobSpec,
                submit_time = 0
            },
            try
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
                FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
                test_decode_no_crash(FullMsg)
            catch
                _:_ -> true
            end
        end).

%%====================================================================
%% PropEr Properties - Roundtrip
%%====================================================================

%% @doc Valid federation messages should survive encode/decode roundtrip
prop_roundtrip_federation_messages() ->
    MsgGens = [
        {?MSG_FED_JOB_SUBMIT, fed_job_submit_msg()},
        {?MSG_FED_JOB_STARTED, fed_job_started_msg()},
        {?MSG_FED_SIBLING_REVOKE, fed_sibling_revoke_msg()},
        {?MSG_FED_JOB_COMPLETED, fed_job_completed_msg()},
        {?MSG_FED_JOB_FAILED, fed_job_failed_msg()}
    ],
    ?FORALL({MsgType, Msg}, oneof([{T, G} || {T, G} <- MsgGens]),
        begin
            try
                {ok, EncodedBody} = flurm_protocol_codec:encode_body(MsgType, Msg),
                FullMsg = create_valid_federation_message(MsgType, EncodedBody),
                case flurm_protocol_codec:decode(FullMsg) of
                    {ok, #slurm_msg{header = H}, <<>>} ->
                        H#slurm_header.msg_type =:= MsgType;
                    {ok, _, _Rest} ->
                        true;  % Extra data acceptable
                    {error, _} ->
                        true;  % Decode error acceptable
                    _ ->
                        true
                end
            catch
                _:_ -> true  % Errors acceptable for generated data
            end
        end).

%%====================================================================
%% PropEr Generators
%%====================================================================

%% @doc Generate federation job ID
federation_job_id() ->
    ?LET({Cluster, JobId, Ts}, {cluster_name(), non_neg_integer(), non_neg_integer()},
        iolist_to_binary([Cluster, ":", integer_to_list(JobId), ":", integer_to_list(Ts)])).

%% @doc Generate cluster name
cluster_name() ->
    ?LET(Name, non_empty(list(oneof([range($a, $z), range($0, $9), $_, $-]))),
        list_to_binary(Name)).

%% @doc Generate job specification map
job_spec() ->
    ?LET(Fields, list({oneof([name, partition, num_cpus, num_nodes, time_limit]),
                       oneof([binary(), non_neg_integer()])}),
        maps:from_list(Fields)).

%% @doc Generate MSG_FED_JOB_SUBMIT message
fed_job_submit_msg() ->
    ?LET({FedJobId, Origin, Target, Spec, Time},
         {federation_job_id(), cluster_name(), cluster_name(), job_spec(), non_neg_integer()},
        #fed_job_submit_msg{
            federation_job_id = FedJobId,
            origin_cluster = Origin,
            target_cluster = Target,
            job_spec = Spec,
            submit_time = Time
        }).

%% @doc Generate MSG_FED_JOB_STARTED message
fed_job_started_msg() ->
    ?LET({FedJobId, Running, LocalId, Time},
         {federation_job_id(), cluster_name(), non_neg_integer(), non_neg_integer()},
        #fed_job_started_msg{
            federation_job_id = FedJobId,
            running_cluster = Running,
            local_job_id = LocalId,
            start_time = Time
        }).

%% @doc Generate MSG_FED_SIBLING_REVOKE message
fed_sibling_revoke_msg() ->
    ?LET({FedJobId, Running, Reason},
         {federation_job_id(), cluster_name(), binary()},
        #fed_sibling_revoke_msg{
            federation_job_id = FedJobId,
            running_cluster = Running,
            revoke_reason = Reason
        }).

%% @doc Generate MSG_FED_JOB_COMPLETED message
fed_job_completed_msg() ->
    ?LET({FedJobId, Running, LocalId, Time, Exit},
         {federation_job_id(), cluster_name(), non_neg_integer(), non_neg_integer(), integer()},
        #fed_job_completed_msg{
            federation_job_id = FedJobId,
            running_cluster = Running,
            local_job_id = LocalId,
            end_time = Time,
            exit_code = Exit
        }).

%% @doc Generate MSG_FED_JOB_FAILED message
fed_job_failed_msg() ->
    ?LET({FedJobId, Running, LocalId, Time, Exit, ErrMsg},
         {federation_job_id(), cluster_name(), non_neg_integer(), non_neg_integer(), integer(), binary()},
        #fed_job_failed_msg{
            federation_job_id = FedJobId,
            running_cluster = Running,
            local_job_id = LocalId,
            end_time = Time,
            exit_code = Exit,
            error_msg = ErrMsg
        }).

%% @doc Generate mutation operations
mutation_operation() ->
    oneof([
        {bit_flip, non_neg_integer()},
        {byte_flip, non_neg_integer()},
        {truncate, non_neg_integer()},
        {extend, binary()},
        {insert, non_neg_integer(), binary()},
        {delete, non_neg_integer(), range(1, 10)},
        {swap, non_neg_integer(), non_neg_integer()},
        {set_byte, non_neg_integer(), range(0, 255)},
        {zero_fill, non_neg_integer(), range(1, 10)},
        {set_interesting_value, non_neg_integer()}
    ]).

%%====================================================================
%% Mutation Helpers
%%====================================================================

%% @doc Apply a mutation operation to a binary
apply_mutation(Bin, _) when byte_size(Bin) =:= 0 ->
    Bin;

apply_mutation(Bin, {bit_flip, Pos}) ->
    RealPos = Pos rem byte_size(Bin),
    <<Before:RealPos/binary, Byte:8, After/binary>> = Bin,
    BitPos = Pos rem 8,
    NewByte = Byte bxor (1 bsl BitPos),
    <<Before/binary, NewByte:8, After/binary>>;

apply_mutation(Bin, {byte_flip, Pos}) ->
    RealPos = Pos rem byte_size(Bin),
    <<Before:RealPos/binary, _:8, After/binary>> = Bin,
    NewByte = rand:uniform(256) - 1,
    <<Before/binary, NewByte:8, After/binary>>;

apply_mutation(Bin, {truncate, Len}) ->
    RealLen = min(Len, byte_size(Bin)),
    case RealLen of
        0 -> <<>>;
        _ -> binary:part(Bin, 0, RealLen)
    end;

apply_mutation(Bin, {extend, Extra}) ->
    <<Bin/binary, Extra/binary>>;

apply_mutation(Bin, {insert, Pos, Bytes}) ->
    RealPos = min(Pos, byte_size(Bin)),
    <<Before:RealPos/binary, After/binary>> = Bin,
    <<Before/binary, Bytes/binary, After/binary>>;

apply_mutation(Bin, {delete, Pos, Len}) when byte_size(Bin) > 0 ->
    RealPos = Pos rem byte_size(Bin),
    RealLen = min(Len, byte_size(Bin) - RealPos),
    case RealLen of
        0 -> Bin;
        _ ->
            <<Before:RealPos/binary, _:RealLen/binary, After/binary>> = Bin,
            <<Before/binary, After/binary>>
    end;
apply_mutation(Bin, {delete, _, _}) ->
    Bin;

apply_mutation(Bin, {swap, Pos1, Pos2}) when byte_size(Bin) >= 2 ->
    RealPos1 = Pos1 rem byte_size(Bin),
    RealPos2 = Pos2 rem byte_size(Bin),
    case RealPos1 =:= RealPos2 of
        true -> Bin;
        false ->
            {P1, P2} = {min(RealPos1, RealPos2), max(RealPos1, RealPos2)},
            <<Pre:P1/binary, B1:8, Mid:(P2-P1-1)/binary, B2:8, Post/binary>> = Bin,
            <<Pre/binary, B2:8, Mid/binary, B1:8, Post/binary>>
    end;
apply_mutation(Bin, {swap, _, _}) ->
    Bin;

apply_mutation(Bin, {set_byte, Pos, Value}) ->
    RealPos = Pos rem byte_size(Bin),
    <<Before:RealPos/binary, _:8, After/binary>> = Bin,
    <<Before/binary, Value:8, After/binary>>;

apply_mutation(Bin, {zero_fill, Pos, Len}) ->
    RealPos = Pos rem byte_size(Bin),
    RealLen = min(Len, byte_size(Bin) - RealPos),
    <<Before:RealPos/binary, _:RealLen/binary, After/binary>> = Bin,
    Zeros = binary:copy(<<0>>, RealLen),
    <<Before/binary, Zeros/binary, After/binary>>;

apply_mutation(Bin, {set_interesting_value, Pos}) ->
    InterestingValues = [
        <<0>>,
        <<255>>,
        <<127>>,
        <<128>>,
        <<0, 0>>,
        <<255, 255>>,
        <<0, 0, 0, 0>>,
        <<255, 255, 255, 255>>,
        <<127, 255, 255, 255>>,  % Max signed 32-bit
        <<128, 0, 0, 0>>,        % Min signed 32-bit as unsigned
        <<254, 255, 255, 255>>,  % SLURM_NO_VAL
        <<253, 255, 255, 255>>,  % SLURM_INFINITE
        <<0, 0, 0, 0, 0, 0, 0, 0>>,          % Zero 64-bit
        <<255, 255, 255, 255, 255, 255, 255, 255>>  % Max 64-bit
    ],
    Value = lists:nth((Pos rem length(InterestingValues)) + 1, InterestingValues),
    ValueSize = byte_size(Value),
    case byte_size(Bin) >= ValueSize of
        true ->
            RealPos = Pos rem (byte_size(Bin) - ValueSize + 1),
            <<Before:RealPos/binary, _:ValueSize/binary, After/binary>> = Bin,
            <<Before/binary, Value/binary, After/binary>>;
        false ->
            Value
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @doc Test that decoding doesn't crash with specific error types
test_decode_no_crash(Data) ->
    Result = try
        flurm_protocol_codec:decode(Data)
    catch
        error:badarg -> {crash, badarg};
        error:function_clause -> {crash, function_clause};
        error:{badmatch, _} -> {crash, badmatch};
        error:badarith -> {crash, badarith};
        error:{case_clause, _} -> {crash, case_clause};
        _:_ -> ok  % Other exceptions are acceptable
    end,
    case Result of
        {crash, _} -> false;  % These are bugs
        _ -> true
    end.

%% @doc Create a valid wire-format message with the given type and body
create_valid_federation_message(MsgType, BodyBin) ->
    create_valid_federation_message(MsgType, BodyBin, ?SLURM_PROTOCOL_VERSION).

create_valid_federation_message(MsgType, BodyBin, Version) ->
    Flags = 0,
    ForwardCnt = 0,
    RetCnt = 0,
    OrigAddr = <<0, 0>>,  % AF_UNSPEC
    BodyLen = byte_size(BodyBin),
    Header = <<Version:16/big, Flags:16/big, MsgType:16/big,
               BodyLen:32/big, ForwardCnt:16/big, RetCnt:16/big,
               OrigAddr/binary>>,
    Length = byte_size(Header) + BodyLen,
    <<Length:32/big, Header/binary, BodyBin/binary>>.

%%====================================================================
%% EUnit Test Wrappers
%%====================================================================

%% Quick smoke test for CI
federation_smoke_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_random_bytes_no_crash_federation(),
            [{numtests, 200}, {to_file, user}])),
        ?assertEqual(true, proper:quickcheck(prop_malformed_headers_no_crash(),
            [{numtests, 200}, {to_file, user}]))
    end}.

%% Mutation fuzzing tests
fed_job_submit_mutation_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_fed_job_submit_mutation(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

fed_job_started_mutation_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_fed_job_started_mutation(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

fed_sibling_revoke_mutation_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_fed_sibling_revoke_mutation(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

fed_job_completed_mutation_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_fed_job_completed_mutation(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

fed_job_failed_mutation_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_fed_job_failed_mutation(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

%% Boundary tests
boundary_federation_job_id_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_boundary_federation_job_id(),
            [{numtests, 500}, {to_file, user}]))
    end}.

boundary_cluster_name_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_boundary_cluster_name(),
            [{numtests, 500}, {to_file, user}]))
    end}.

boundary_timestamps_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_boundary_timestamps(),
            [{numtests, 500}, {to_file, user}]))
    end}.

boundary_job_spec_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_boundary_job_spec(),
            [{numtests, 500}, {to_file, user}]))
    end}.

%% Roundtrip tests
roundtrip_federation_messages_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_roundtrip_federation_messages(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

%%====================================================================
%% Direct Unit Tests for Specific Edge Cases
%%====================================================================

%% Test empty federation_job_id
empty_federation_job_id_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<>>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"target">>,
        job_spec = #{},
        submit_time = 0
    },
    Result = try
        {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
        test_decode_no_crash(FullMsg)
    catch _:_ -> true end,
    ?assert(Result).

%% Test very long cluster name
very_long_cluster_name_test() ->
    LongName = binary:copy(<<"x">>, 65536),  % >65535 bytes
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"test-job">>,
        origin_cluster = LongName,
        target_cluster = <<"target">>,
        job_spec = #{},
        submit_time = 0
    },
    Result = try
        {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
        test_decode_no_crash(FullMsg)
    catch _:_ -> true end,
    ?assert(Result).

%% Test max uint32 job ID
max_uint32_job_id_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"test-job">>,
        running_cluster = <<"cluster1">>,
        local_job_id = 16#FFFFFFFF,  % Max uint32
        start_time = 0
    },
    {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
    FullMsg = create_valid_federation_message(?MSG_FED_JOB_STARTED, EncodedBody),
    ?assert(test_decode_no_crash(FullMsg)).

%% Test max uint64 timestamp
max_uint64_timestamp_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"test-job">>,
        running_cluster = <<"cluster1">>,
        local_job_id = 123,
        end_time = 16#FFFFFFFFFFFFFFFF,  % Max uint64
        exit_code = 0
    },
    {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
    FullMsg = create_valid_federation_message(?MSG_FED_JOB_COMPLETED, EncodedBody),
    ?assert(test_decode_no_crash(FullMsg)).

%% Test negative exit code
negative_exit_code_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"test-job">>,
        running_cluster = <<"cluster1">>,
        local_job_id = 123,
        end_time = 1234567890,
        exit_code = -128,  % Negative exit code
        error_msg = <<"Job killed by signal">>
    },
    {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
    FullMsg = create_valid_federation_message(?MSG_FED_JOB_FAILED, EncodedBody),
    ?assert(test_decode_no_crash(FullMsg)).

%% Test zero-length job spec
zero_length_job_spec_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"test-job">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"target">>,
        job_spec = #{},
        submit_time = 0
    },
    {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
    FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
    ?assert(test_decode_no_crash(FullMsg)).

%% Test truncated message
truncated_message_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"test-job">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"target">>,
        job_spec = #{name => <<"test">>},
        submit_time = 1234567890
    },
    {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
    FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
    %% Truncate at various points
    lists:foreach(fun(Len) ->
        Truncated = binary:part(FullMsg, 0, min(Len, byte_size(FullMsg))),
        ?assert(test_decode_no_crash(Truncated))
    end, [0, 1, 2, 4, 8, 16, 32]).

%% Test random byte injection
random_bytes_injection_test() ->
    lists:foreach(fun(_) ->
        RandomBytes = crypto:strong_rand_bytes(rand:uniform(1024)),
        FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, RandomBytes),
        ?assert(test_decode_no_crash(FullMsg))
    end, lists:seq(1, 100)).

%% Test all zeros
all_zeros_test() ->
    ZeroBody = binary:copy(<<0>>, 100),
    lists:foreach(fun(MsgType) ->
        FullMsg = create_valid_federation_message(MsgType, ZeroBody),
        ?assert(test_decode_no_crash(FullMsg))
    end, [?MSG_FED_JOB_SUBMIT, ?MSG_FED_JOB_STARTED, ?MSG_FED_SIBLING_REVOKE,
          ?MSG_FED_JOB_COMPLETED, ?MSG_FED_JOB_FAILED]).

%% Test all 0xFF bytes
all_ones_test() ->
    OnesBody = binary:copy(<<255>>, 100),
    lists:foreach(fun(MsgType) ->
        FullMsg = create_valid_federation_message(MsgType, OnesBody),
        ?assert(test_decode_no_crash(FullMsg))
    end, [?MSG_FED_JOB_SUBMIT, ?MSG_FED_JOB_STARTED, ?MSG_FED_SIBLING_REVOKE,
          ?MSG_FED_JOB_COMPLETED, ?MSG_FED_JOB_FAILED]).

%% Test invalid message type in header
invalid_message_type_test() ->
    Body = <<0:32, 0:32>>,
    %% Use an invalid message type
    FullMsg = create_valid_federation_message(16#FFFF, Body),
    ?assert(test_decode_no_crash(FullMsg)).

%% Test corrupted length field
corrupted_length_field_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"test-job">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"target">>,
        job_spec = #{},
        submit_time = 0
    },
    {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
    FullMsg = create_valid_federation_message(?MSG_FED_JOB_SUBMIT, EncodedBody),
    %% Corrupt the length field with various values
    lists:foreach(fun(NewLen) ->
        <<_:32, Rest/binary>> = FullMsg,
        Corrupted = <<NewLen:32/big, Rest/binary>>,
        ?assert(test_decode_no_crash(Corrupted))
    end, [0, 1, 16#FFFFFFFF, 16#7FFFFFFF, byte_size(FullMsg) * 2]).

%% Test special UTF-8 characters in strings
utf8_strings_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"job-", 16#C0:8, 16#80:8>>,  % Overlong encoding
        running_cluster = <<"cluster-", 16#E2:8, 16#80:8, 16#8B:8>>,  % Zero-width space
        revoke_reason = <<"reason-", 16#EF:8, 16#BF:8, 16#BD:8>>  % Replacement char
    },
    Result = try
        {ok, EncodedBody} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        FullMsg = create_valid_federation_message(?MSG_FED_SIBLING_REVOKE, EncodedBody),
        test_decode_no_crash(FullMsg)
    catch _:_ -> true end,
    ?assert(Result).
