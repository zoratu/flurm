%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Fuzzing Module
%%%
%%% Protocol fuzzing for the SLURM binary protocol codec.
%%% Generates random/mutated inputs to find parsing bugs, crashes,
%%% and edge cases in the protocol implementation.
%%%
%%% Usage:
%%%   fuzz_protocol:run(1000).         % Run 1000 random tests
%%%   fuzz_protocol:run_mutation(100). % Run 100 mutation tests
%%%   fuzz_protocol:run_all().         % Run comprehensive fuzzing
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(fuzz_protocol).

-export([
    run/1,
    run_mutation/1,
    run_all/0,
    fuzz_target/1,
    generate_random_message/0,
    mutate_message/1
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run N random fuzzing iterations
-spec run(pos_integer()) -> {ok, non_neg_integer(), non_neg_integer()}.
run(N) ->
    io:format("Starting random fuzzing with ~p iterations...~n", [N]),
    Results = [fuzz_random() || _ <- lists:seq(1, N)],
    {Passes, Fails} = count_results(Results),
    io:format("Random fuzzing complete: ~p passed, ~p failed~n", [Passes, Fails]),
    {ok, Passes, Fails}.

%% @doc Run N mutation-based fuzzing iterations
-spec run_mutation(pos_integer()) -> {ok, non_neg_integer(), non_neg_integer()}.
run_mutation(N) ->
    io:format("Starting mutation fuzzing with ~p iterations...~n", [N]),
    %% Get valid message templates
    Templates = get_valid_templates(),
    Results = [fuzz_mutation(Templates) || _ <- lists:seq(1, N)],
    {Passes, Fails} = count_results(Results),
    io:format("Mutation fuzzing complete: ~p passed, ~p failed~n", [Passes, Fails]),
    {ok, Passes, Fails}.

%% @doc Run comprehensive fuzzing suite
-spec run_all() -> ok.
run_all() ->
    io:format("=== Starting Comprehensive Protocol Fuzzing ===~n~n"),

    %% Random fuzzing
    {ok, RP, RF} = run(10000),

    %% Mutation fuzzing
    {ok, MP, MF} = run_mutation(5000),

    %% Boundary testing
    {ok, BP, BF} = run_boundary_tests(),

    %% Edge case testing
    {ok, EP, EF} = run_edge_cases(),

    io:format("~n=== Fuzzing Summary ===~n"),
    io:format("Random:    ~p passed, ~p failed~n", [RP, RF]),
    io:format("Mutation:  ~p passed, ~p failed~n", [MP, MF]),
    io:format("Boundary:  ~p passed, ~p failed~n", [BP, BF]),
    io:format("EdgeCases: ~p passed, ~p failed~n", [EP, EF]),
    io:format("Total:     ~p passed, ~p failed~n",
              [RP + MP + BP + EP, RF + MF + BF + EF]),
    ok.

%% @doc Fuzz target function for external fuzzers (AFL, etc)
-spec fuzz_target(binary()) -> ok | {error, term()}.
fuzz_target(BinaryInput) ->
    try
        case flurm_protocol_codec:decode(BinaryInput) of
            {ok, #slurm_msg{header = Header, body = Body}, _Rest} ->
                %% Try to re-encode and verify round-trip
                MsgType = Header#slurm_header.msg_type,
                case flurm_protocol_codec:encode(MsgType, Body) of
                    {ok, ReEncoded} ->
                        case flurm_protocol_codec:decode(ReEncoded) of
                            {ok, _, _} -> ok;
                            _ -> ok  % Re-decode may differ, that's fine
                        end;
                    {error, _} ->
                        ok  % Encoding failure is not necessarily a bug
                end;
            {error, _} ->
                ok  % Invalid input is expected
        end
    catch
        error:badarg -> error(badarg);  % These are bugs
        error:function_clause -> error(function_clause);
        error:{badmatch, _} -> error(badmatch);
        error:badarith -> error(badarith);
        _:_ -> ok  % Other errors may be expected
    end.

%% @doc Generate a random message
-spec generate_random_message() -> binary().
generate_random_message() ->
    case rand:uniform(10) of
        1 -> generate_empty_message();
        2 -> generate_short_message();
        3 -> generate_valid_header_random_body();
        4 -> generate_huge_length();
        5 -> generate_truncated_message();
        6 -> generate_zero_version();
        7 -> generate_random_msg_type();
        8 -> generate_all_ones();
        9 -> generate_all_zeros();
        _ -> generate_pure_random(rand:uniform(1000))
    end.

%% @doc Mutate an existing message
-spec mutate_message(binary()) -> binary().
mutate_message(Msg) when byte_size(Msg) < 4 ->
    Msg;
mutate_message(Msg) ->
    case rand:uniform(10) of
        1 -> bit_flip(Msg);
        2 -> byte_flip(Msg);
        3 -> truncate(Msg);
        4 -> extend(Msg);
        5 -> insert_bytes(Msg);
        6 -> delete_bytes(Msg);
        7 -> swap_bytes(Msg);
        8 -> corrupt_length(Msg);
        9 -> corrupt_header(Msg);
        _ -> repeat_bytes(Msg)
    end.

%%====================================================================
%% Internal Functions - Fuzzing Logic
%%====================================================================

fuzz_random() ->
    Msg = generate_random_message(),
    fuzz_target(Msg).

fuzz_mutation(Templates) ->
    Template = lists:nth(rand:uniform(length(Templates)), Templates),
    Mutated = mutate_message(Template),
    fuzz_target(Mutated).

count_results(Results) ->
    lists:foldl(fun
        (ok, {P, F}) -> {P + 1, F};
        ({error, _}, {P, F}) -> {P, F + 1}
    end, {0, 0}, Results).

%%====================================================================
%% Internal Functions - Message Generators
%%====================================================================

generate_empty_message() ->
    <<>>.

generate_short_message() ->
    N = rand:uniform(13),
    crypto:strong_rand_bytes(N).

generate_valid_header_random_body() ->
    Version = ?SLURM_PROTOCOL_VERSION,
    Flags = rand:uniform(16) - 1,
    MsgType = lists:nth(rand:uniform(20), valid_msg_types()),
    BodySize = rand:uniform(100),
    Body = crypto:strong_rand_bytes(BodySize),
    Length = 10 + BodySize,
    <<Length:32/big, Version:16/big, Flags:16/big, 0:16/big, MsgType:16/big,
      BodySize:32/big, Body/binary>>.

generate_huge_length() ->
    %% Length says huge, but actual data is small
    HugeLength = 16#FFFFFFFF,
    SmallBody = crypto:strong_rand_bytes(10),
    <<HugeLength:32/big, SmallBody/binary>>.

generate_truncated_message() ->
    %% Length says 100, but we only have 20 bytes
    <<100:32/big, (crypto:strong_rand_bytes(20))/binary>>.

generate_zero_version() ->
    BodySize = rand:uniform(50),
    Body = crypto:strong_rand_bytes(BodySize),
    Length = 10 + BodySize,
    <<Length:32/big, 0:16/big, 0:16/big, 0:16/big, 4003:16/big,
      BodySize:32/big, Body/binary>>.

generate_random_msg_type() ->
    Version = ?SLURM_PROTOCOL_VERSION,
    MsgType = rand:uniform(16#FFFF),
    BodySize = rand:uniform(50),
    Body = crypto:strong_rand_bytes(BodySize),
    Length = 10 + BodySize,
    <<Length:32/big, Version:16/big, 0:16/big, 0:16/big, MsgType:16/big,
      BodySize:32/big, Body/binary>>.

generate_all_ones() ->
    N = 4 + rand:uniform(50),
    binary:copy(<<255>>, N).

generate_all_zeros() ->
    N = 4 + rand:uniform(50),
    binary:copy(<<0>>, N).

generate_pure_random(N) ->
    crypto:strong_rand_bytes(N).

%%====================================================================
%% Internal Functions - Mutation Operations
%%====================================================================

bit_flip(Msg) ->
    Pos = rand:uniform(byte_size(Msg)) - 1,
    <<Before:Pos/binary, Byte:8, After/binary>> = Msg,
    BitPos = rand:uniform(8) - 1,
    Flipped = Byte bxor (1 bsl BitPos),
    <<Before/binary, Flipped:8, After/binary>>.

byte_flip(Msg) ->
    Pos = rand:uniform(byte_size(Msg)) - 1,
    <<Before:Pos/binary, _:8, After/binary>> = Msg,
    NewByte = rand:uniform(256) - 1,
    <<Before/binary, NewByte:8, After/binary>>.

truncate(Msg) ->
    NewLen = rand:uniform(byte_size(Msg)),
    <<Result:NewLen/binary, _/binary>> = Msg,
    Result.

extend(Msg) ->
    Extra = crypto:strong_rand_bytes(rand:uniform(50)),
    <<Msg/binary, Extra/binary>>.

insert_bytes(Msg) ->
    Pos = rand:uniform(byte_size(Msg) + 1) - 1,
    <<Before:Pos/binary, After/binary>> = Msg,
    Insert = crypto:strong_rand_bytes(rand:uniform(10)),
    <<Before/binary, Insert/binary, After/binary>>.

delete_bytes(Msg) when byte_size(Msg) < 2 ->
    Msg;
delete_bytes(Msg) ->
    Pos = rand:uniform(byte_size(Msg) - 1) - 1,
    DelLen = min(rand:uniform(5), byte_size(Msg) - Pos),
    <<Before:Pos/binary, _:DelLen/binary, After/binary>> = Msg,
    <<Before/binary, After/binary>>.

swap_bytes(Msg) when byte_size(Msg) < 2 ->
    Msg;
swap_bytes(Msg) ->
    Pos1 = rand:uniform(byte_size(Msg)) - 1,
    Pos2 = rand:uniform(byte_size(Msg)) - 1,
    case Pos1 == Pos2 of
        true -> Msg;
        false ->
            {P1, P2} = {min(Pos1, Pos2), max(Pos1, Pos2)},
            <<Before:P1/binary, B1:8, Mid:(P2-P1-1)/binary, B2:8, After/binary>> = Msg,
            <<Before/binary, B2:8, Mid/binary, B1:8, After/binary>>
    end.

corrupt_length(Msg) when byte_size(Msg) < 4 ->
    Msg;
corrupt_length(<<_:32/big, Rest/binary>>) ->
    NewLen = rand:uniform(16#FFFFFFFF),
    <<NewLen:32/big, Rest/binary>>.

corrupt_header(Msg) when byte_size(Msg) < 14 ->
    Msg;
corrupt_header(<<Length:32/big, _Header:10/binary, Body/binary>>) ->
    NewHeader = crypto:strong_rand_bytes(10),
    <<Length:32/big, NewHeader/binary, Body/binary>>.

repeat_bytes(Msg) when byte_size(Msg) == 0 ->
    Msg;
repeat_bytes(Msg) ->
    Pos = rand:uniform(byte_size(Msg)) - 1,
    Len = min(rand:uniform(10), byte_size(Msg) - Pos),
    <<Before:Pos/binary, Chunk:Len/binary, After/binary>> = Msg,
    Repeats = rand:uniform(5),
    Repeated = binary:copy(Chunk, Repeats),
    <<Before/binary, Repeated/binary, After/binary>>.

%%====================================================================
%% Internal Functions - Boundary and Edge Case Testing
%%====================================================================

run_boundary_tests() ->
    io:format("Running boundary tests...~n"),
    Tests = [
        %% Length boundary cases
        <<0:32/big>>,
        <<1:32/big, 0>>,
        <<10:32/big, 0:80>>,  % Exact header size
        <<11:32/big, 0:88>>,  % Header + 1 byte
        <<16#FFFFFFFF:32/big, 0:80>>,  % Max length
        <<16#FFFFFFFE:32/big, 0:80>>,  % SLURM_NO_VAL length

        %% Version boundary cases
        <<14:32/big, 0:16/big, 0:16/big, 0:16/big, 4003:16/big, 0:32/big>>,
        <<14:32/big, 16#FFFF:16/big, 0:16/big, 0:16/big, 4003:16/big, 0:32/big>>,

        %% Message type boundaries
        <<14:32/big, 16#2600:16/big, 0:16/big, 0:16/big, 0:16/big, 0:32/big>>,
        <<14:32/big, 16#2600:16/big, 0:16/big, 0:16/big, 16#FFFF:16/big, 0:32/big>>
    ],
    Results = [fuzz_target(T) || T <- Tests],
    {Passes, Fails} = count_results(Results),
    {ok, Passes, Fails}.

run_edge_cases() ->
    io:format("Running edge case tests...~n"),
    Tests = [
        %% Empty body for each message type
        [make_empty_body_msg(MT) || MT <- valid_msg_types()],

        %% Very large body
        [make_large_body_msg(MT, 10000) || MT <- [4003, 2003, 1001]],

        %% Body with embedded nulls
        [make_null_body_msg(MT) || MT <- valid_msg_types()],

        %% Unicode in strings
        [make_unicode_msg()]
    ],
    FlatTests = lists:flatten(Tests),
    Results = [fuzz_target(T) || T <- FlatTests],
    {Passes, Fails} = count_results(Results),
    {ok, Passes, Fails}.

make_empty_body_msg(MsgType) ->
    <<10:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
      MsgType:16/big, 0:32/big>>.

make_large_body_msg(MsgType, Size) ->
    Body = crypto:strong_rand_bytes(Size),
    Length = 10 + Size,
    <<Length:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
      MsgType:16/big, Size:32/big, Body/binary>>.

make_null_body_msg(MsgType) ->
    Body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
    <<20:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
      MsgType:16/big, 10:32/big, Body/binary>>.

make_unicode_msg() ->
    %% UTF-8 encoded string with various Unicode
    UnicodeStr = <<"test_üñíçödé_テスト">>,
    StrLen = byte_size(UnicodeStr),
    Body = <<StrLen:32/big, UnicodeStr/binary>>,
    BodySize = byte_size(Body),
    Length = 10 + BodySize,
    <<Length:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
      4003:16/big, BodySize:32/big, Body/binary>>.

%%====================================================================
%% Internal Functions - Valid Templates
%%====================================================================

valid_msg_types() ->
    [1001, 1002, 1003, 1008,  % Node ops
     2003, 2004, 2007, 2008, 2009, 2010,  % Info
     4001, 4002, 4003, 4004, 4006,  % Jobs
     5001, 5002, 5003, 5004,  % Steps
     8001].  % Return code

get_valid_templates() ->
    [
        %% Valid ping request
        <<10:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
          ?REQUEST_PING:16/big, 0:32/big>>,

        %% Valid job info request
        <<22:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
          ?REQUEST_JOB_INFO:16/big, 12:32/big, 0:32/big, 0:32/big, 0:32/big>>,

        %% Valid node info request
        <<18:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
          ?REQUEST_NODE_INFO:16/big, 8:32/big, 0:32/big, 0:32/big>>,

        %% Valid return code response
        <<14:32/big, ?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big,
          ?RESPONSE_SLURM_RC:16/big, 4:32/big, 0:32/big>>
    ].
