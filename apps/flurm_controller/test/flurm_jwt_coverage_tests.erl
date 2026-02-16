%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_jwt module
%%% Tests for JWT token generation and validation
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_jwt_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% API Function Tests
%%====================================================================

%% Test generate/1 with simple subject
generate_simple_test() ->
    {ok, Token} = flurm_jwt:generate(<<"test_user">>),
    ?assert(is_binary(Token)),
    %% JWT has 3 parts separated by dots
    Parts = binary:split(Token, <<".">>, [global]),
    ?assertEqual(3, length(Parts)).

generate_binary_subject_test() ->
    {ok, Token} = flurm_jwt:generate(<<"admin">>),
    ?assert(is_binary(Token)).

generate_empty_subject_test() ->
    {ok, Token} = flurm_jwt:generate(<<>>),
    ?assert(is_binary(Token)).

%% Test generate/2 with extra claims
generate_with_claims_test() ->
    ExtraClaims = #{
        <<"role">> => <<"admin">>,
        <<"permissions">> => [<<"read">>, <<"write">>]
    },
    {ok, Token} = flurm_jwt:generate(<<"user1">>, ExtraClaims),
    ?assert(is_binary(Token)).

generate_with_empty_claims_test() ->
    {ok, Token} = flurm_jwt:generate(<<"user1">>, #{}),
    ?assert(is_binary(Token)).

generate_with_nested_claims_test() ->
    ExtraClaims = #{
        <<"metadata">> => #{
            <<"department">> => <<"engineering">>,
            <<"level">> => 3
        }
    },
    {ok, Token} = flurm_jwt:generate(<<"user1">>, ExtraClaims),
    ?assert(is_binary(Token)).

%%====================================================================
%% verify Tests
%%====================================================================

verify_valid_token_test() ->
    {ok, Token} = flurm_jwt:generate(<<"test_user">>),
    {ok, Claims} = flurm_jwt:verify(Token),
    ?assert(is_map(Claims)),
    ?assertEqual(<<"test_user">>, maps:get(<<"sub">>, Claims)).

verify_token_with_claims_test() ->
    ExtraClaims = #{<<"role">> => <<"admin">>},
    {ok, Token} = flurm_jwt:generate(<<"admin_user">>, ExtraClaims),
    {ok, Claims} = flurm_jwt:verify(Token),
    ?assertEqual(<<"admin_user">>, maps:get(<<"sub">>, Claims)),
    ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims)).

verify_invalid_token_test() ->
    Result = flurm_jwt:verify(<<"invalid.token.here">>),
    ?assertMatch({error, _}, Result).

verify_malformed_token_test() ->
    Result = flurm_jwt:verify(<<"notavalidtoken">>),
    ?assertMatch({error, _}, Result).

verify_empty_token_test() ->
    Result = flurm_jwt:verify(<<>>),
    ?assertMatch({error, _}, Result).

verify_tampered_signature_test() ->
    {ok, Token} = flurm_jwt:generate(<<"user">>),
    %% Tamper with the signature (last part)
    [Header, Payload, _Sig] = binary:split(Token, <<".">>, [global]),
    TamperedToken = <<Header/binary, ".", Payload/binary, ".tampered">>,
    Result = flurm_jwt:verify(TamperedToken),
    ?assertMatch({error, _}, Result).

verify_with_custom_secret_test() ->
    Secret = <<"my-custom-secret">>,
    %% Generate with default secret
    {ok, Token} = flurm_jwt:generate(<<"user">>),
    %% Verify with different secret should fail
    Result = flurm_jwt:verify(Token, Secret),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% decode Tests
%%====================================================================

decode_valid_token_test() ->
    {ok, Token} = flurm_jwt:generate(<<"decode_test">>),
    {ok, Claims} = flurm_jwt:decode(Token),
    ?assert(is_map(Claims)),
    ?assertEqual(<<"decode_test">>, maps:get(<<"sub">>, Claims)).

decode_invalid_token_test() ->
    Result = flurm_jwt:decode(<<"invalid.token">>),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% get_claims Tests
%%====================================================================

get_claims_valid_token_test() ->
    {ok, Token} = flurm_jwt:generate(<<"claims_test">>),
    {ok, Claims} = flurm_jwt:get_claims(Token),
    ?assert(is_map(Claims)),
    ?assertEqual(<<"claims_test">>, maps:get(<<"sub">>, Claims)),
    ?assert(maps:is_key(<<"iss">>, Claims)),
    ?assert(maps:is_key(<<"iat">>, Claims)),
    ?assert(maps:is_key(<<"exp">>, Claims)),
    ?assert(maps:is_key(<<"cluster_id">>, Claims)).

get_claims_with_custom_claims_test() ->
    ExtraClaims = #{<<"custom">> => <<"value">>},
    {ok, Token} = flurm_jwt:generate(<<"user">>, ExtraClaims),
    {ok, Claims} = flurm_jwt:get_claims(Token),
    ?assertEqual(<<"value">>, maps:get(<<"custom">>, Claims)).

get_claims_invalid_token_test() ->
    Result = flurm_jwt:get_claims(<<"not.a.token">>),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% refresh Tests
%%====================================================================

refresh_valid_token_test() ->
    {ok, Token} = flurm_jwt:generate(<<"refresh_user">>),
    %% Wait a moment so timestamp changes
    timer:sleep(1100),
    {ok, NewToken} = flurm_jwt:refresh(Token),
    ?assert(is_binary(NewToken)),
    %% Token may or may not be different depending on whether iat changed
    %% The important thing is that it's valid
    {ok, Claims} = flurm_jwt:verify(NewToken),
    ?assertEqual(<<"refresh_user">>, maps:get(<<"sub">>, Claims)).

refresh_with_claims_preserved_test() ->
    ExtraClaims = #{<<"role">> => <<"admin">>},
    {ok, Token} = flurm_jwt:generate(<<"admin_user">>, ExtraClaims),
    {ok, NewToken} = flurm_jwt:refresh(Token),
    {ok, Claims} = flurm_jwt:get_claims(NewToken),
    %% Extra claims should be preserved
    ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims)).

refresh_invalid_token_test() ->
    Result = flurm_jwt:refresh(<<"invalid.token.here">>),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Internal Function Tests (-ifdef(TEST) exports)
%%====================================================================

%% encode_base64url Tests

encode_base64url_simple_test() ->
    Result = flurm_jwt:encode_base64url(<<"hello">>),
    ?assert(is_binary(Result)),
    %% Should not contain +, /, or =
    ?assertEqual(nomatch, binary:match(Result, <<"+">>)),
    ?assertEqual(nomatch, binary:match(Result, <<"/">>)),
    ?assertEqual(nomatch, binary:match(Result, <<"=">>)).

encode_base64url_binary_test() ->
    Result = flurm_jwt:encode_base64url(<<0, 1, 255>>),
    ?assert(is_binary(Result)).

encode_base64url_empty_test() ->
    Result = flurm_jwt:encode_base64url(<<>>),
    ?assertEqual(<<>>, Result).

%% decode_base64url Tests

decode_base64url_simple_test() ->
    Encoded = flurm_jwt:encode_base64url(<<"test data">>),
    Decoded = flurm_jwt:decode_base64url(Encoded),
    ?assertEqual(<<"test data">>, Decoded).

decode_base64url_roundtrip_test() ->
    Original = <<"roundtrip test with special chars: +/=">>,
    Encoded = flurm_jwt:encode_base64url(Original),
    Decoded = flurm_jwt:decode_base64url(Encoded),
    ?assertEqual(Original, Decoded).

decode_base64url_binary_test() ->
    Original = <<0, 127, 128, 255>>,
    Encoded = flurm_jwt:encode_base64url(Original),
    Decoded = flurm_jwt:decode_base64url(Encoded),
    ?assertEqual(Original, Decoded).

%% build_header Tests

build_header_test() ->
    Header = flurm_jwt:build_header(),
    ?assert(is_map(Header)),
    ?assertEqual(<<"HS256">>, maps:get(<<"alg">>, Header)),
    ?assertEqual(<<"JWT">>, maps:get(<<"typ">>, Header)).

%% build_payload Tests

build_payload_simple_test() ->
    Payload = flurm_jwt:build_payload(<<"test_subject">>, #{}),
    ?assert(is_map(Payload)),
    ?assertEqual(<<"test_subject">>, maps:get(<<"sub">>, Payload)),
    ?assert(maps:is_key(<<"iss">>, Payload)),
    ?assert(maps:is_key(<<"iat">>, Payload)),
    ?assert(maps:is_key(<<"exp">>, Payload)),
    ?assert(maps:is_key(<<"cluster_id">>, Payload)).

build_payload_with_claims_test() ->
    ExtraClaims = #{<<"custom">> => <<"value">>},
    Payload = flurm_jwt:build_payload(<<"user">>, ExtraClaims),
    ?assertEqual(<<"value">>, maps:get(<<"custom">>, Payload)).

build_payload_timestamps_test() ->
    Payload = flurm_jwt:build_payload(<<"user">>, #{}),
    Iat = maps:get(<<"iat">>, Payload),
    Exp = maps:get(<<"exp">>, Payload),
    ?assert(is_integer(Iat)),
    ?assert(is_integer(Exp)),
    ?assert(Exp > Iat).  % Expiry should be after issue time

%% sign Tests

sign_test() ->
    Message = <<"test message">>,
    Secret = <<"secret">>,
    Signature = flurm_jwt:sign(Message, Secret),
    ?assert(is_binary(Signature)),
    ?assertEqual(32, byte_size(Signature)).  % SHA256 produces 32 bytes

sign_consistent_test() ->
    Message = <<"consistent message">>,
    Secret = <<"key">>,
    Sig1 = flurm_jwt:sign(Message, Secret),
    Sig2 = flurm_jwt:sign(Message, Secret),
    ?assertEqual(Sig1, Sig2).  % Same input should produce same output

sign_different_messages_test() ->
    Secret = <<"key">>,
    Sig1 = flurm_jwt:sign(<<"message1">>, Secret),
    Sig2 = flurm_jwt:sign(<<"message2">>, Secret),
    ?assertNotEqual(Sig1, Sig2).

sign_different_secrets_test() ->
    Message = <<"message">>,
    Sig1 = flurm_jwt:sign(Message, <<"secret1">>),
    Sig2 = flurm_jwt:sign(Message, <<"secret2">>),
    ?assertNotEqual(Sig1, Sig2).

%% get_secret Tests

get_secret_test() ->
    Secret = flurm_jwt:get_secret(),
    ?assert(is_binary(Secret)),
    ?assert(byte_size(Secret) > 0).

%% parse_token Tests

parse_token_valid_test() ->
    {ok, Token} = flurm_jwt:generate(<<"user">>),
    Result = flurm_jwt:parse_token(Token),
    ?assertMatch({ok, _, _, _}, Result),
    {ok, Header, Payload, Signature} = Result,
    ?assert(is_map(Header)),
    ?assert(is_map(Payload)),
    ?assert(is_binary(Signature)).

parse_token_invalid_format_test() ->
    Result = flurm_jwt:parse_token(<<"not.valid">>),
    ?assertMatch({error, invalid_token_format}, Result).

parse_token_only_two_parts_test() ->
    Result = flurm_jwt:parse_token(<<"only.two">>),
    ?assertMatch({error, invalid_token_format}, Result).

parse_token_invalid_base64_test() ->
    Result = flurm_jwt:parse_token(<<"!!!.!!!.!!!">>),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Edge Cases
%%====================================================================

%% Very long subject
long_subject_test() ->
    LongSubject = binary:copy(<<"x">>, 10000),
    {ok, Token} = flurm_jwt:generate(LongSubject),
    {ok, Claims} = flurm_jwt:verify(Token),
    ?assertEqual(LongSubject, maps:get(<<"sub">>, Claims)).

%% Unicode in subject
unicode_subject_test() ->
    Subject = <<"user_\u4e2d\u6587">>,
    {ok, Token} = flurm_jwt:generate(Subject),
    {ok, Claims} = flurm_jwt:verify(Token),
    ?assertEqual(Subject, maps:get(<<"sub">>, Claims)).

%% Many extra claims
many_claims_test() ->
    ExtraClaims = maps:from_list([{list_to_binary("claim_" ++ integer_to_list(I)), I} || I <- lists:seq(1, 100)]),
    {ok, Token} = flurm_jwt:generate(<<"user">>, ExtraClaims),
    {ok, Claims} = flurm_jwt:get_claims(Token),
    ?assertEqual(1, maps:get(<<"claim_1">>, Claims)),
    ?assertEqual(100, maps:get(<<"claim_100">>, Claims)).

%% Verify with correct secret after generation
verify_with_correct_secret_test() ->
    Secret = flurm_jwt:get_secret(),
    {ok, Token} = flurm_jwt:generate(<<"user">>),
    {ok, Claims} = flurm_jwt:verify(Token, Secret),
    ?assertEqual(<<"user">>, maps:get(<<"sub">>, Claims)).
