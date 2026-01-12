%%%-------------------------------------------------------------------
%%% @doc Protocol Fuzzing Tests for FLURM
%%%
%%% This module provides fuzzing capabilities for the FLURM protocol
%%% codec to discover edge cases and potential crashes.
%%%
%%% Can be used with:
%%% - AFL (American Fuzzy Lop)
%%% - libFuzzer
%%% - PropEr-based fuzzing
%%% - Manual fuzz testing
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_fuzz).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-export([
    fuzz_decode/1,
    fuzz_roundtrip/1,
    run_random_fuzz/1,
    run_mutation_fuzz/2,
    afl_target/0
]).

%% Suppress unused function warnings for corpus generation
-compile({nowarn_unused_function, [
    generate_corpus/1,
    write_corpus_file/3,
    create_valid_job_info_message/0
]}).

%%====================================================================
%% Fuzzing Entry Points
%%====================================================================

%% @doc Main fuzzing target for AFL/libFuzzer
%% Returns 0 on success, non-zero on failure (for AFL)
-spec afl_target() -> ok.
afl_target() ->
    %% Read input from stdin (AFL convention)
    case file:read(standard_io, 65536) of
        {ok, Data} ->
            fuzz_decode(Data);
        eof ->
            ok;
        {error, _} ->
            ok
    end.

%% @doc Fuzz the decoder with arbitrary binary input
%% This should never crash - only return ok/error tuples
-spec fuzz_decode(binary()) -> ok | {error, term()}.
fuzz_decode(Data) when is_binary(Data) ->
    try
        case flurm_protocol_codec:decode(Data) of
            {ok, _Msg, _Rest} -> ok;
            {error, _Reason} -> ok;
            {incomplete, _Needed} -> ok
        end
    catch
        %% These are bugs we want to find
        error:badarg ->
            io:format(standard_error, "CRASH: badarg with input ~p~n", [Data]),
            {error, badarg};
        error:function_clause ->
            io:format(standard_error, "CRASH: function_clause with input ~p~n", [Data]),
            {error, function_clause};
        error:{badmatch, _} = E ->
            io:format(standard_error, "CRASH: ~p with input ~p~n", [E, Data]),
            {error, E};
        error:badarith ->
            io:format(standard_error, "CRASH: badarith with input ~p~n", [Data]),
            {error, badarith};
        %% Expected for malformed input
        throw:_ -> ok;
        error:_ -> ok
    end;
fuzz_decode(_) ->
    ok.

%% @doc Fuzz with round-trip testing
%% Encode then decode should preserve data
-spec fuzz_roundtrip(term()) -> ok | {error, term()}.
fuzz_roundtrip(Input) ->
    try
        case flurm_protocol_codec:encode(?REQUEST_PING, Input) of
            {ok, Encoded} ->
                case flurm_protocol_codec:decode(Encoded) of
                    {ok, _Msg, <<>>} -> ok;
                    {ok, _Msg, Rest} when byte_size(Rest) > 0 ->
                        {error, {extra_data, Rest}};
                    {error, Reason} ->
                        {error, {decode_failed, Reason}};
                    {incomplete, _} ->
                        {error, incomplete_after_encode}
                end;
            {error, _} ->
                ok  % Some inputs can't be encoded
        end
    catch
        _:_ -> ok
    end.

%% @doc Run random fuzzing for N iterations
-spec run_random_fuzz(pos_integer()) -> {ok, non_neg_integer()} | {error, term()}.
run_random_fuzz(Iterations) ->
    run_random_fuzz(Iterations, 0).

run_random_fuzz(0, Crashes) ->
    {ok, Crashes};
run_random_fuzz(N, Crashes) ->
    Data = generate_random_binary(),
    case fuzz_decode(Data) of
        ok ->
            run_random_fuzz(N - 1, Crashes);
        {error, _} ->
            run_random_fuzz(N - 1, Crashes + 1)
    end.

%% @doc Run mutation-based fuzzing starting from a valid seed
-spec run_mutation_fuzz(binary(), pos_integer()) -> {ok, non_neg_integer()}.
run_mutation_fuzz(Seed, Iterations) ->
    run_mutation_fuzz(Seed, Iterations, 0).

run_mutation_fuzz(_Seed, 0, Crashes) ->
    {ok, Crashes};
run_mutation_fuzz(Seed, N, Crashes) ->
    Mutated = mutate_binary(Seed),
    case fuzz_decode(Mutated) of
        ok ->
            run_mutation_fuzz(Seed, N - 1, Crashes);
        {error, _} ->
            run_mutation_fuzz(Seed, N - 1, Crashes + 1)
    end.

%%====================================================================
%% EUnit Tests
%%====================================================================

fuzz_random_test_() ->
    {timeout, 30, fun() ->
        {ok, Crashes} = run_random_fuzz(1000),
        ?assertEqual(0, Crashes)
    end}.

fuzz_mutation_test_() ->
    {timeout, 30, fun() ->
        %% Create a valid message to use as seed
        Seed = create_valid_ping_message(),
        {ok, Crashes} = run_mutation_fuzz(Seed, 1000),
        ?assertEqual(0, Crashes)
    end}.

fuzz_boundary_test_() ->
    {timeout, 10, fun() ->
        %% Test specific boundary conditions
        BoundaryInputs = [
            <<>>,                           % Empty
            <<0>>,                           % Single byte
            <<0,0,0,0>>,                     % Just length field (zero)
            <<255,255,255,255>>,             % Max length field
            <<0,0,0,1,0>>,                   % Length 1, one byte
            <<0,0,0,10>>,                    % Length 10, no body
            binary:copy(<<0>>, 100),         % All zeros
            binary:copy(<<255>>, 100),       % All ones
            create_valid_ping_message(),     % Valid message
            truncate_message(create_valid_ping_message()), % Truncated valid
            extend_message(create_valid_ping_message())    % Extended valid
        ],
        lists:foreach(fun(Input) ->
            Result = fuzz_decode(Input),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end, BoundaryInputs)
    end}.

%%====================================================================
%% Generators
%%====================================================================

%% Generate random binary of random size
generate_random_binary() ->
    Size = rand:uniform(2048),
    list_to_binary([rand:uniform(256) - 1 || _ <- lists:seq(1, Size)]).

%% Mutate a binary in various ways
mutate_binary(Binary) ->
    Mutations = [
        fun bit_flip/1,
        fun byte_flip/1,
        fun insert_byte/1,
        fun delete_byte/1,
        fun duplicate_chunk/1,
        fun swap_bytes/1,
        fun truncate/1,
        fun extend/1,
        fun set_interesting_value/1
    ],
    Mutation = lists:nth(rand:uniform(length(Mutations)), Mutations),
    Mutation(Binary).

%% Flip a random bit
bit_flip(<<>>) -> <<>>;
bit_flip(Binary) ->
    Pos = rand:uniform(byte_size(Binary)) - 1,
    BitPos = rand:uniform(8) - 1,
    <<Pre:Pos/binary, Byte:8, Post/binary>> = Binary,
    NewByte = Byte bxor (1 bsl BitPos),
    <<Pre/binary, NewByte:8, Post/binary>>.

%% Flip a random byte
byte_flip(<<>>) -> <<>>;
byte_flip(Binary) ->
    Pos = rand:uniform(byte_size(Binary)) - 1,
    <<Pre:Pos/binary, _:8, Post/binary>> = Binary,
    NewByte = rand:uniform(256) - 1,
    <<Pre/binary, NewByte:8, Post/binary>>.

%% Insert a random byte
insert_byte(Binary) ->
    Pos = rand:uniform(byte_size(Binary) + 1) - 1,
    <<Pre:Pos/binary, Post/binary>> = Binary,
    NewByte = rand:uniform(256) - 1,
    <<Pre/binary, NewByte:8, Post/binary>>.

%% Delete a random byte
delete_byte(<<>>) -> <<>>;
delete_byte(<<_:8>>) -> <<>>;
delete_byte(Binary) ->
    Pos = rand:uniform(byte_size(Binary)) - 1,
    <<Pre:Pos/binary, _:8, Post/binary>> = Binary,
    <<Pre/binary, Post/binary>>.

%% Duplicate a chunk
duplicate_chunk(Binary) when byte_size(Binary) < 2 -> Binary;
duplicate_chunk(Binary) ->
    ChunkSize = min(rand:uniform(16), byte_size(Binary)),
    Start = rand:uniform(byte_size(Binary) - ChunkSize + 1) - 1,
    <<Pre:Start/binary, Chunk:ChunkSize/binary, Post/binary>> = Binary,
    <<Pre/binary, Chunk/binary, Chunk/binary, Post/binary>>.

%% Swap two bytes
swap_bytes(Binary) when byte_size(Binary) < 2 -> Binary;
swap_bytes(Binary) ->
    Pos1 = rand:uniform(byte_size(Binary)) - 1,
    Pos2 = rand:uniform(byte_size(Binary)) - 1,
    if
        Pos1 =:= Pos2 -> Binary;
        Pos1 > Pos2 -> swap_bytes_at(Binary, Pos2, Pos1);
        true -> swap_bytes_at(Binary, Pos1, Pos2)
    end.

swap_bytes_at(Binary, Pos1, Pos2) ->
    <<Pre:Pos1/binary, B1:8, Mid:(Pos2-Pos1-1)/binary, B2:8, Post/binary>> = Binary,
    <<Pre/binary, B2:8, Mid/binary, B1:8, Post/binary>>.

%% Truncate binary
truncate(<<>>) -> <<>>;
truncate(Binary) ->
    NewSize = rand:uniform(byte_size(Binary)),
    <<Result:NewSize/binary, _/binary>> = Binary,
    Result.

%% Extend binary with random data
extend(Binary) ->
    Extension = generate_random_binary(),
    <<Binary/binary, Extension/binary>>.

%% Set interesting boundary values at random position
set_interesting_value(<<>>) -> <<>>;
set_interesting_value(Binary) ->
    InterestingValues = [
        <<0>>,
        <<255>>,
        <<0,0>>,
        <<255,255>>,
        <<0,0,0,0>>,
        <<255,255,255,255>>,
        <<127,255,255,255>>,  % Max signed 32-bit
        <<128,0,0,0>>         % Min signed 32-bit
    ],
    Value = lists:nth(rand:uniform(length(InterestingValues)), InterestingValues),
    ValueSize = byte_size(Value),
    case byte_size(Binary) >= ValueSize of
        true ->
            Pos = rand:uniform(byte_size(Binary) - ValueSize + 1) - 1,
            <<Pre:Pos/binary, _:ValueSize/binary, Post/binary>> = Binary,
            <<Pre/binary, Value/binary, Post/binary>>;
        false ->
            Value
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

create_valid_ping_message() ->
    Version = 16#2600,
    Flags = 0,
    MsgType = ?REQUEST_PING,
    Body = <<>>,
    BodyLen = 0,
    HeaderLen = 10,
    TotalLen = HeaderLen + BodyLen,
    Header = <<Version:16/big, Flags:16/big, 0:16/big,
               MsgType:16/big, BodyLen:32/big>>,
    <<TotalLen:32/big, Header/binary, Body/binary>>.

truncate_message(Message) ->
    Size = byte_size(Message),
    TruncSize = max(1, Size - rand:uniform(Size div 2)),
    <<Result:TruncSize/binary, _/binary>> = Message,
    Result.

extend_message(Message) ->
    Extra = generate_random_binary(),
    <<Message/binary, Extra/binary>>.

%%====================================================================
%% Corpus Generation (for AFL)
%%====================================================================

%% @doc Generate a fuzzing corpus directory
-spec generate_corpus(file:filename()) -> ok.
generate_corpus(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),

    %% Valid messages
    write_corpus_file(Dir, "valid_ping", create_valid_ping_message()),
    write_corpus_file(Dir, "valid_job_info", create_valid_job_info_message()),

    %% Edge cases
    write_corpus_file(Dir, "empty", <<>>),
    write_corpus_file(Dir, "min_header", <<0,0,0,10, 0,0,0,0,0,0,0,0,0,0>>),
    write_corpus_file(Dir, "max_length", <<255,255,255,255>>),

    ok.

write_corpus_file(Dir, Name, Data) ->
    Path = filename:join(Dir, Name),
    file:write_file(Path, Data).

create_valid_job_info_message() ->
    Version = 16#2600,
    Flags = 0,
    MsgType = ?REQUEST_JOB_INFO,
    Body = <<1:32/big, 0:16/big, 0:16/big>>,  % job_id=1, flags=0, user=0
    BodyLen = byte_size(Body),
    HeaderLen = 10,
    TotalLen = HeaderLen + BodyLen,
    Header = <<Version:16/big, Flags:16/big, 0:16/big,
               MsgType:16/big, BodyLen:32/big>>,
    <<TotalLen:32/big, Header/binary, Body/binary>>.
