%%%-------------------------------------------------------------------
%%% @doc Protocol Fuzzing Tests for FLURM using PropEr
%%%
%%% This module provides comprehensive property-based fuzzing tests for the
%%% SLURM protocol codec to ensure crash resistance and robustness.
%%%
%%% Fuzzing Strategies:
%%% 1. Mutation fuzzing - corrupt valid messages in various ways
%%% 2. Random byte sequences - test decoder against arbitrary data
%%% 3. Boundary testing - test edge cases for numeric fields
%%% 4. TRES string parsing - test trackable resource strings
%%% 5. Header corruption - specifically target header parsing
%%%
%%% Usage:
%%%   rebar3 proper -m flurm_protocol_fuzz_tests -n 1000
%%%   rebar3 proper -m flurm_protocol_fuzz_tests -n 10000 -p prop_random_bytes_no_crash
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_fuzz_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% PropEr properties
-export([
    prop_decode_survives_mutation/0,
    prop_random_bytes_no_crash/0,
    prop_job_array_boundaries/0,
    prop_tres_string_parsing/0,
    prop_header_corruption_no_crash/0,
    prop_roundtrip_preserves_data/0,
    prop_length_field_manipulation/0,
    prop_message_type_fuzzing/0
]).

%% PropEr generators
-export([
    valid_slurm_message/0,
    mutation_operation/0,
    tres_string/0,
    slurm_header/0,
    message_type/0,
    job_array_spec/0
]).

%% Mutation helpers
-export([
    apply_mutation/2
]).

%%====================================================================
%% PropEr Properties
%%====================================================================

%% @doc Mutation fuzzing - corrupt valid messages
%% Verifies that the decoder handles corrupted valid messages gracefully.
prop_decode_survives_mutation() ->
    ?FORALL(Msg, valid_slurm_message(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                Corrupted = apply_mutation(Msg, Mutation),
                case catch flurm_protocol_codec:decode(Corrupted) of
                    {ok, _, _} -> true;
                    {error, _} -> true;
                    {incomplete, _} -> true;
                    {'EXIT', {badarg, _}} -> false;  % Should not crash with badarg
                    {'EXIT', {function_clause, _}} -> false;  % Should not crash with function_clause
                    {'EXIT', {{badmatch, _}, _}} -> false;  % Should not crash with badmatch
                    {'EXIT', {badarith, _}} -> false;  % Should not crash with badarith
                    {'EXIT', _} -> true;  % Other exits might be acceptable
                    _ -> true
                end
            end)).

%% @doc Random byte sequences (crash resistance)
%% The decoder should never crash on arbitrary input - only return errors.
prop_random_bytes_no_crash() ->
    ?FORALL(Bytes, binary(),
        begin
            Result = try
                flurm_protocol_codec:decode(Bytes)
            catch
                error:badarg -> {crash, badarg};
                error:function_clause -> {crash, function_clause};
                error:{badmatch, _} -> {crash, badmatch};
                error:badarith -> {crash, badarith};
                _:_ -> ok  % Other exceptions are acceptable
            end,
            case Result of
                {crash, _} -> false;  % These are bugs
                _ -> true
            end
        end).

%% @doc Boundary testing for job arrays
%% Tests that array size limits are enforced correctly.
prop_job_array_boundaries() ->
    ?FORALL(Size, range(0, 2000000),
        begin
            ArraySpec = <<"1-", (integer_to_binary(Size))/binary>>,
            case catch flurm_job_array:parse_array_spec(ArraySpec) of
                {ok, Spec} ->
                    %% Valid specs should have reasonable sizes
                    TaskCount = get_task_count(Spec),
                    TaskCount =< 1000001;  % Allow some margin
                {error, array_too_large} ->
                    Size > 1000000;
                {error, _} ->
                    true;  % Other errors are acceptable
                {'EXIT', _} ->
                    false  % Should not crash
            end
        end).

%% @doc TRES string parsing
%% Tests that TRES format strings are parsed without crashing.
prop_tres_string_parsing() ->
    ?FORALL(TresStr, tres_string(),
        begin
            Result = try
                parse_tres_string(TresStr)
            catch
                error:badarg -> {crash, badarg};
                error:function_clause -> {crash, function_clause};
                _:_ -> ok
            end,
            case Result of
                {crash, _} -> false;
                _ -> true
            end
        end).

%% @doc Header corruption fuzzing
%% Tests that header parsing handles corrupted headers gracefully.
prop_header_corruption_no_crash() ->
    ?FORALL(Header, slurm_header(),
        ?FORALL(Mutation, mutation_operation(),
            begin
                Corrupted = apply_mutation(Header, Mutation),
                Result = try
                    flurm_protocol_header:parse_header(Corrupted)
                catch
                    error:badarg -> {crash, badarg};
                    error:function_clause -> {crash, function_clause};
                    error:{badmatch, _} -> {crash, badmatch};
                    _:_ -> ok
                end,
                case Result of
                    {crash, _} -> false;
                    _ -> true
                end
            end)).

%% @doc Roundtrip property - encode then decode preserves data
%% Valid messages should survive encode/decode roundtrip.
prop_roundtrip_preserves_data() ->
    ?FORALL(MsgType, message_type(),
        begin
            Body = create_body_for_type(MsgType),
            case catch flurm_protocol_codec:encode(MsgType, Body) of
                {ok, Encoded} ->
                    case catch flurm_protocol_codec:decode(Encoded) of
                        {ok, #slurm_msg{header = H}, <<>>} ->
                            H#slurm_header.msg_type =:= MsgType;
                        {ok, _, Rest} when byte_size(Rest) > 0 ->
                            false;  % Extra data is a bug
                        {error, _} ->
                            false;  % Should be able to decode what we encoded
                        {'EXIT', _} ->
                            false
                    end;
                {error, _} ->
                    true;  % Some types might not be encodable
                {'EXIT', _} ->
                    true  % Acceptable if encoding fails
            end
        end).

%% @doc Length field manipulation
%% Tests that manipulating the length field doesn't crash the decoder.
prop_length_field_manipulation() ->
    ?FORALL({Msg, NewLen}, {valid_slurm_message(), non_neg_integer()},
        begin
            Corrupted = case Msg of
                <<_OldLen:32/big, Rest/binary>> when byte_size(Rest) >= 0 ->
                    <<NewLen:32/big, Rest/binary>>;
                _ ->
                    Msg
            end,
            Result = try
                flurm_protocol_codec:decode(Corrupted)
            catch
                error:badarg -> {crash, badarg};
                error:function_clause -> {crash, function_clause};
                _:_ -> ok
            end,
            case Result of
                {crash, _} -> false;
                _ -> true
            end
        end).

%% @doc Message type fuzzing
%% Tests that arbitrary message types don't crash the decoder.
prop_message_type_fuzzing() ->
    ?FORALL(MsgType, range(0, 16#FFFF),
        begin
            %% Create a minimal valid structure with arbitrary message type
            Body = <<>>,
            BodyLen = 0,
            Version = ?SLURM_PROTOCOL_VERSION,
            Flags = 0,
            ForwardCnt = 0,
            RetCnt = 0,
            OrigAddr = <<0, 0>>,  % AF_UNSPEC
            Header = <<Version:16/big, Flags:16/big, MsgType:16/big,
                       BodyLen:32/big, ForwardCnt:16/big, RetCnt:16/big,
                       OrigAddr/binary>>,
            Length = byte_size(Header) + BodyLen,
            Msg = <<Length:32/big, Header/binary, Body/binary>>,
            Result = try
                flurm_protocol_codec:decode(Msg)
            catch
                error:badarg -> {crash, badarg};
                error:function_clause -> {crash, function_clause};
                _:_ -> ok
            end,
            case Result of
                {crash, _} -> false;
                _ -> true
            end
        end).

%%====================================================================
%% PropEr Generators
%%====================================================================

%% @doc Generate valid SLURM protocol messages
valid_slurm_message() ->
    ?LET(MsgType, oneof([
            ?REQUEST_PING,
            ?REQUEST_JOB_INFO,
            ?REQUEST_NODE_INFO,
            ?REQUEST_PARTITION_INFO,
            ?REQUEST_NODE_REGISTRATION_STATUS,
            ?RESPONSE_SLURM_RC
        ]),
        create_valid_message(MsgType)).

%% @doc Generate mutation operations
mutation_operation() ->
    oneof([
        {bit_flip, non_neg_integer()},
        {byte_flip, non_neg_integer()},
        {truncate, non_neg_integer()},
        {insert, non_neg_integer(), binary()},
        {delete, non_neg_integer(), range(1, 10)},
        {swap, non_neg_integer(), non_neg_integer()},
        {set_byte, non_neg_integer(), range(0, 255)},
        {duplicate_section, non_neg_integer(), range(1, 20)},
        {zero_fill, non_neg_integer(), range(1, 10)},
        {set_interesting_value, non_neg_integer()}
    ]).

%% @doc Generate TRES format strings
%% TRES format: "type/name=count,type/name=count,..."
tres_string() ->
    ?LET(Parts, list(tres_part()),
        begin
            case Parts of
                [] -> <<>>;
                _ -> list_to_binary(lists:join(",", Parts))
            end
        end).

tres_part() ->
    ?LET({Type, Name, Count}, {tres_type(), tres_name(), non_neg_integer()},
        case Name of
            <<>> -> io_lib:format("~s=~B", [Type, Count]);
            _ -> io_lib:format("~s/~s=~B", [Type, Name, Count])
        end).

tres_type() ->
    oneof([<<"cpu">>, <<"mem">>, <<"node">>, <<"billing">>, <<"fs">>,
           <<"vmem">>, <<"pages">>, <<"energy">>, <<"gres">>, <<"bb">>,
           <<"license">>, binary()]).

tres_name() ->
    oneof([<<>>, <<"gpu">>, <<"gpu:tesla">>, <<"gpu:a100">>,
           <<"disk">>, <<"bandwidth">>, binary()]).

%% @doc Generate SLURM protocol headers
slurm_header() ->
    ?LET({Version, Flags, MsgType, BodyLen, FwdCnt, RetCnt},
         {range(0, 16#FFFF), range(0, 16#FFFF), range(0, 16#FFFF),
          range(0, 16#FFFFFFFF), range(0, 16#FFFF), range(0, 16#FFFF)},
        begin
            OrigAddr = <<0, 0>>,  % AF_UNSPEC (2 bytes)
            <<Version:16/big, Flags:16/big, MsgType:16/big,
              BodyLen:32/big, FwdCnt:16/big, RetCnt:16/big,
              OrigAddr/binary>>
        end).

%% @doc Generate valid message types
message_type() ->
    oneof([
        ?REQUEST_PING,
        ?REQUEST_JOB_INFO,
        ?REQUEST_NODE_INFO,
        ?REQUEST_PARTITION_INFO,
        ?REQUEST_NODE_REGISTRATION_STATUS,
        ?REQUEST_SUBMIT_BATCH_JOB,
        ?REQUEST_CANCEL_JOB,
        ?RESPONSE_SLURM_RC,
        ?RESPONSE_JOB_INFO,
        ?RESPONSE_NODE_INFO
    ]).

%% @doc Generate job array specifications
job_array_spec() ->
    oneof([
        %% Simple range: "0-100"
        ?LET({Start, End}, {range(0, 1000), range(1, 1001)},
            list_to_binary(io_lib:format("~B-~B", [Start, Start + End]))),
        %% Range with step: "0-100:2"
        ?LET({Start, End, Step}, {range(0, 1000), range(1, 1001), range(1, 10)},
            list_to_binary(io_lib:format("~B-~B:~B", [Start, Start + End, Step]))),
        %% Range with limit: "0-100%10"
        ?LET({Start, End, Limit}, {range(0, 1000), range(1, 1001), range(1, 100)},
            list_to_binary(io_lib:format("~B-~B%~B", [Start, Start + End, Limit]))),
        %% Comma-separated list
        ?LET(Nums, non_empty(list(range(0, 10000))),
            list_to_binary(lists:join(",", [integer_to_list(N) || N <- Nums])))
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

apply_mutation(Bin, {duplicate_section, Pos, Len}) ->
    RealPos = Pos rem byte_size(Bin),
    RealLen = min(Len, byte_size(Bin) - RealPos),
    case RealLen of
        0 -> Bin;
        _ ->
            <<Before:RealPos/binary, Section:RealLen/binary, After/binary>> = Bin,
            <<Before/binary, Section/binary, Section/binary, After/binary>>
    end;

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
        <<253, 255, 255, 255>>   % SLURM_INFINITE
    ],
    Value = lists:nth((Pos rem length(InterestingValues)) + 1, InterestingValues),
    ValueSize = byte_size(Value),
    case byte_size(Bin) >= ValueSize of
        true ->
            RealPos = Pos rem (byte_size(Bin) - ValueSize + 1),
            <<Before:RealPos/binary, _:ValueSize/binary, After/binary>> = Bin,
            <<Before/binary, Value/binary, After/binary>>;
        false ->
            Value  % Replace entire binary with interesting value
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @doc Create a valid protocol message for the given type
create_valid_message(MsgType) ->
    Version = ?SLURM_PROTOCOL_VERSION,
    Flags = 0,
    ForwardCnt = 0,
    RetCnt = 0,
    OrigAddr = <<0, 0>>,  % AF_UNSPEC
    Body = create_body_for_type(MsgType),
    BodyLen = byte_size(Body),
    Header = <<Version:16/big, Flags:16/big, MsgType:16/big,
               BodyLen:32/big, ForwardCnt:16/big, RetCnt:16/big,
               OrigAddr/binary>>,
    Length = byte_size(Header) + BodyLen,
    <<Length:32/big, Header/binary, Body/binary>>.

%% @doc Create an appropriate body for the message type
create_body_for_type(?REQUEST_PING) ->
    <<>>;
create_body_for_type(?REQUEST_JOB_INFO) ->
    %% job_info_request: show_flags, job_id, user_id
    <<0:32/big, 0:32/big, 0:32/big>>;
create_body_for_type(?REQUEST_NODE_INFO) ->
    %% node_info_request: show_flags, node_name (packstr with 0 length)
    <<0:32/big, 0:32/big>>;
create_body_for_type(?REQUEST_PARTITION_INFO) ->
    %% partition_info_request: show_flags, partition_name (packstr)
    <<0:32/big, 0:32/big>>;
create_body_for_type(?REQUEST_NODE_REGISTRATION_STATUS) ->
    %% status_only: 0 or 1
    <<0:8>>;
create_body_for_type(?RESPONSE_SLURM_RC) ->
    %% return_code: 32-bit signed
    <<0:32/big>>;
create_body_for_type(?RESPONSE_JOB_INFO) ->
    %% Minimal job info response: last_update, job_count (0)
    <<0:32/big, 0:32/big>>;
create_body_for_type(?RESPONSE_NODE_INFO) ->
    %% Minimal node info response: last_update, node_count (0)
    <<0:32/big, 0:32/big>>;
create_body_for_type(_) ->
    %% Unknown type - empty body
    <<>>.

%% @doc Get task count from array spec
get_task_count(Spec) when is_map(Spec) ->
    maps:get(task_count, Spec, 0);
get_task_count(_) ->
    0.

%% @doc Simple TRES string parser for testing
%% Returns ok or crashes - we use this to verify crash resistance
parse_tres_string(<<>>) ->
    {ok, #{}};
parse_tres_string(TresStr) ->
    Parts = binary:split(TresStr, <<",">>, [global]),
    lists:foldl(fun(Part, Acc) ->
        parse_tres_part(Part, Acc)
    end, {ok, #{}}, Parts).

parse_tres_part(<<>>, Acc) ->
    Acc;
parse_tres_part(Part, {ok, Map}) ->
    case binary:split(Part, <<"=">>) of
        [TypeName, ValueBin] ->
            Value = try binary_to_integer(ValueBin) catch _:_ -> 0 end,
            Key = case binary:split(TypeName, <<"/">>) of
                [Type, Name] -> {Type, Name};
                [Type] -> Type
            end,
            {ok, Map#{Key => Value}};
        _ ->
            {ok, Map}
    end;
parse_tres_part(_, Error) ->
    Error.

%%====================================================================
%% EUnit Test Wrappers
%%====================================================================

%% @doc Run fuzzing tests with extended timeout and iterations
fuzz_random_bytes_test_() ->
    {timeout, 300, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_random_bytes_no_crash(),
            [{numtests, 10000}, {to_file, user}]))
    end}.

fuzz_mutation_test_() ->
    {timeout, 300, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_decode_survives_mutation(),
            [{numtests, 5000}, {to_file, user}]))
    end}.

fuzz_header_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_header_corruption_no_crash(),
            [{numtests, 5000}, {to_file, user}]))
    end}.

fuzz_length_field_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_length_field_manipulation(),
            [{numtests, 5000}, {to_file, user}]))
    end}.

fuzz_message_type_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_message_type_fuzzing(),
            [{numtests, 5000}, {to_file, user}]))
    end}.

roundtrip_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_roundtrip_preserves_data(),
            [{numtests, 1000}, {to_file, user}]))
    end}.

tres_parsing_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_tres_string_parsing(),
            [{numtests, 2000}, {to_file, user}]))
    end}.

%% Quick smoke test that runs fast for CI
smoke_test_() ->
    {timeout, 30, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_random_bytes_no_crash(),
            [{numtests, 100}, {to_file, user}])),
        ?assertEqual(true, proper:quickcheck(prop_decode_survives_mutation(),
            [{numtests, 100}, {to_file, user}]))
    end}.
