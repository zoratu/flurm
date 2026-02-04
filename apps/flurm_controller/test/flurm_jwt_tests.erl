%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Tests for flurm_jwt module
%%%
%%% Tests JWT token generation, verification, and refresh functionality.
%%% Uses real time for most tests and manually crafted tokens for
%%% expiry-related tests.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_jwt_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Setup/Teardown Helpers
%%%===================================================================

setup_app_env() ->
    %% Ensure the application environment is clean
    application:unset_env(flurm_controller, jwt_secret),
    application:unset_env(flurm_controller, jwt_issuer),
    application:unset_env(flurm_controller, jwt_expiry),
    application:unset_env(flurm_core, cluster_name).

cleanup_app_env(_) ->
    application:unset_env(flurm_controller, jwt_secret),
    application:unset_env(flurm_controller, jwt_issuer),
    application:unset_env(flurm_controller, jwt_expiry),
    application:unset_env(flurm_core, cluster_name).

%% Helper to create a token with a specific expiry time
create_token_with_expiry(Subject, Exp) ->
    create_token_with_expiry(Subject, Exp, #{}).

create_token_with_expiry(Subject, Exp, ExtraClaims) ->
    Secret = get_test_secret(),
    Header = #{<<"alg">> => <<"HS256">>, <<"typ">> => <<"JWT">>},
    Iat = erlang:system_time(second),
    Payload = maps:merge(#{
        <<"sub">> => Subject,
        <<"iss">> => <<"flurm">>,
        <<"iat">> => Iat,
        <<"exp">> => Exp,
        <<"cluster_id">> => <<"default">>
    }, ExtraClaims),

    HeaderB64 = flurm_jwt:encode_base64url(jsx:encode(Header)),
    PayloadB64 = flurm_jwt:encode_base64url(jsx:encode(Payload)),
    Message = <<HeaderB64/binary, ".", PayloadB64/binary>>,
    Signature = flurm_jwt:sign(Message, Secret),
    SignatureB64 = flurm_jwt:encode_base64url(Signature),
    <<Message/binary, ".", SignatureB64/binary>>.

get_test_secret() ->
    <<"flurm-development-secret-change-in-production">>.

%%%===================================================================
%%% generate/1 Tests
%%%===================================================================

generate_1_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"generate/1 returns {ok, Token} for valid subject", fun() ->
              Result = flurm_jwt:generate(<<"test_user">>),
              ?assertMatch({ok, _}, Result),
              {ok, Token} = Result,
              ?assert(is_binary(Token))
          end},

          {"generate/1 creates token with three parts", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"test_user">>),
              Parts = binary:split(Token, <<".">>, [global]),
              ?assertEqual(3, length(Parts))
          end},

          {"generate/1 includes correct subject in claims", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"my_subject">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"my_subject">>, maps:get(<<"sub">>, Claims))
          end},

          {"generate/1 includes default issuer", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"flurm">>, maps:get(<<"iss">>, Claims))
          end},

          {"generate/1 includes iat claim", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              Iat = maps:get(<<"iat">>, Claims),
              Now = erlang:system_time(second),
              %% iat should be within 5 seconds of now
              ?assert(abs(Now - Iat) < 5)
          end},

          {"generate/1 includes exp claim with default expiry", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              Iat = maps:get(<<"iat">>, Claims),
              Exp = maps:get(<<"exp">>, Claims),
              %% Default expiry is 3600 seconds
              ?assertEqual(3600, Exp - Iat)
          end},

          {"generate/1 includes cluster_id claim", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assert(maps:is_key(<<"cluster_id">>, Claims))
          end}
         ]
     end}.

generate_1_with_custom_config_test_() ->
    {setup,
     fun() ->
         setup_app_env(),
         application:set_env(flurm_controller, jwt_secret, <<"custom_secret">>),
         application:set_env(flurm_controller, jwt_issuer, <<"custom_issuer">>),
         application:set_env(flurm_controller, jwt_expiry, 7200),
         application:set_env(flurm_core, cluster_name, <<"test_cluster">>)
     end,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"generate/1 uses custom issuer from config", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"custom_issuer">>, maps:get(<<"iss">>, Claims))
          end},

          {"generate/1 uses custom expiry from config", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              Iat = maps:get(<<"iat">>, Claims),
              Exp = maps:get(<<"exp">>, Claims),
              ?assertEqual(7200, Exp - Iat)
          end},

          {"generate/1 uses cluster_name from config", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"test_cluster">>, maps:get(<<"cluster_id">>, Claims))
          end}
         ]
     end}.

%%%===================================================================
%%% generate/2 Tests
%%%===================================================================

generate_2_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"generate/2 returns {ok, Token} with extra claims", fun() ->
              ExtraClaims = #{<<"role">> => <<"admin">>},
              Result = flurm_jwt:generate(<<"user">>, ExtraClaims),
              ?assertMatch({ok, _}, Result)
          end},

          {"generate/2 includes extra claims in token", fun() ->
              ExtraClaims = #{<<"role">> => <<"admin">>, <<"permissions">> => [<<"read">>, <<"write">>]},
              {ok, Token} = flurm_jwt:generate(<<"user">>, ExtraClaims),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims)),
              ?assertEqual([<<"read">>, <<"write">>], maps:get(<<"permissions">>, Claims))
          end},

          {"generate/2 preserves standard claims alongside extra claims", fun() ->
              ExtraClaims = #{<<"custom">> => <<"value">>},
              {ok, Token} = flurm_jwt:generate(<<"test_user">>, ExtraClaims),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"test_user">>, maps:get(<<"sub">>, Claims)),
              ?assertEqual(<<"flurm">>, maps:get(<<"iss">>, Claims)),
              ?assertEqual(<<"value">>, maps:get(<<"custom">>, Claims))
          end},

          {"generate/2 with empty extra claims works like generate/1", fun() ->
              {ok, Token1} = flurm_jwt:generate(<<"user">>),
              {ok, Token2} = flurm_jwt:generate(<<"user">>, #{}),
              {ok, Claims1} = flurm_jwt:get_claims(Token1),
              {ok, Claims2} = flurm_jwt:get_claims(Token2),
              ?assertEqual(maps:get(<<"sub">>, Claims1), maps:get(<<"sub">>, Claims2)),
              ?assertEqual(maps:get(<<"iss">>, Claims1), maps:get(<<"iss">>, Claims2))
          end},

          {"generate/2 extra claims can override cluster_id", fun() ->
              ExtraClaims = #{<<"cluster_id">> => <<"override_cluster">>},
              {ok, Token} = flurm_jwt:generate(<<"user">>, ExtraClaims),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"override_cluster">>, maps:get(<<"cluster_id">>, Claims))
          end}
         ]
     end}.

%%%===================================================================
%%% verify/1 Tests
%%%===================================================================

verify_1_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"verify/1 returns {ok, Claims} for valid token", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              Result = flurm_jwt:verify(Token),
              ?assertMatch({ok, _}, Result),
              {ok, Claims} = Result,
              ?assertEqual(<<"user">>, maps:get(<<"sub">>, Claims))
          end},

          {"verify/1 validates all claims correctly", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"admin">>),
              {ok, Claims} = flurm_jwt:verify(Token),
              ?assertEqual(<<"admin">>, maps:get(<<"sub">>, Claims)),
              ?assertEqual(<<"flurm">>, maps:get(<<"iss">>, Claims)),
              ?assert(maps:is_key(<<"iat">>, Claims)),
              ?assert(maps:is_key(<<"exp">>, Claims))
          end}
         ]
     end}.

%%%===================================================================
%%% verify/2 Tests (Custom Secret)
%%%===================================================================

verify_2_custom_secret_test_() ->
    {setup,
     fun() ->
         setup_app_env(),
         application:set_env(flurm_controller, jwt_secret, <<"my_secret">>)
     end,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"verify/2 with correct custom secret returns claims", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              Result = flurm_jwt:verify(Token, <<"my_secret">>),
              ?assertMatch({ok, _}, Result)
          end},

          {"verify/2 with wrong secret returns invalid_signature", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              Result = flurm_jwt:verify(Token, <<"wrong_secret">>),
              ?assertEqual({error, invalid_signature}, Result)
          end},

          {"verify/2 allows cross-cluster verification with different secrets", fun() ->
              %% Generate with one secret
              application:set_env(flurm_controller, jwt_secret, <<"cluster1_secret">>),
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              %% Verify with explicit secret
              Result = flurm_jwt:verify(Token, <<"cluster1_secret">>),
              ?assertMatch({ok, _}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% verify - Invalid Signature Tests
%%%===================================================================

verify_invalid_signature_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"verify returns {error, invalid_signature} for tampered payload", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [Header, _Payload, Signature] = binary:split(Token, <<".">>, [global]),
              %% Create a different payload
              FakePayload = flurm_jwt:encode_base64url(jsx:encode(#{<<"sub">> => <<"hacker">>})),
              TamperedToken = <<Header/binary, ".", FakePayload/binary, ".", Signature/binary>>,
              Result = flurm_jwt:verify(TamperedToken),
              ?assertEqual({error, invalid_signature}, Result)
          end},

          {"verify returns {error, invalid_signature} for tampered header", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [_Header, Payload, Signature] = binary:split(Token, <<".">>, [global]),
              %% Create a different header (still valid JSON though)
              FakeHeader = flurm_jwt:encode_base64url(jsx:encode(#{<<"alg">> => <<"HS256">>, <<"typ">> => <<"JWT">>, <<"extra">> => <<"data">>})),
              TamperedToken = <<FakeHeader/binary, ".", Payload/binary, ".", Signature/binary>>,
              Result = flurm_jwt:verify(TamperedToken),
              ?assertEqual({error, invalid_signature}, Result)
          end},

          {"verify returns {error, invalid_signature} for modified signature", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [Header, Payload, _Signature] = binary:split(Token, <<".">>, [global]),
              %% Create a fake signature
              FakeSignature = flurm_jwt:encode_base64url(<<"fake_signature_bytes_here_1234">>),
              TamperedToken = <<Header/binary, ".", Payload/binary, ".", FakeSignature/binary>>,
              Result = flurm_jwt:verify(TamperedToken),
              ?assertEqual({error, invalid_signature}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% verify - Expired Token Tests
%%%===================================================================

verify_expired_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"verify returns {error, expired} for expired token", fun() ->
              %% Create a token that expired in the past
              PastExp = erlang:system_time(second) - 100,
              Token = create_token_with_expiry(<<"user">>, PastExp),
              Result = flurm_jwt:verify(Token),
              ?assertEqual({error, expired}, Result)
          end},

          {"verify succeeds for token not yet expired", fun() ->
              %% Create a token that expires in the future
              FutureExp = erlang:system_time(second) + 3600,
              Token = create_token_with_expiry(<<"user">>, FutureExp),
              Result = flurm_jwt:verify(Token),
              ?assertMatch({ok, _}, Result)
          end},

          {"verify succeeds for token at exact expiry boundary", fun() ->
              %% Create a token that expires exactly now (should still be valid)
              NowExp = erlang:system_time(second),
              Token = create_token_with_expiry(<<"user">>, NowExp),
              Result = flurm_jwt:verify(Token),
              ?assertMatch({ok, _}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% decode/1 Tests
%%%===================================================================

decode_1_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"decode/1 returns claims without verification", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              Result = flurm_jwt:decode(Token),
              ?assertMatch({ok, _}, Result),
              {ok, Claims} = Result,
              ?assertEqual(<<"user">>, maps:get(<<"sub">>, Claims))
          end},

          {"decode/1 returns claims even for tampered token", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [Header, _Payload, Signature] = binary:split(Token, <<".">>, [global]),
              %% Create a tampered payload
              FakePayload = flurm_jwt:encode_base64url(jsx:encode(#{<<"sub">> => <<"hacker">>, <<"iss">> => <<"evil">>})),
              TamperedToken = <<Header/binary, ".", FakePayload/binary, ".", Signature/binary>>,
              %% decode should return the claims without verifying signature
              Result = flurm_jwt:decode(TamperedToken),
              ?assertMatch({ok, _}, Result),
              {ok, Claims} = Result,
              ?assertEqual(<<"hacker">>, maps:get(<<"sub">>, Claims))
          end}
         ]
     end}.

%%%===================================================================
%%% get_claims/1 Tests
%%%===================================================================

get_claims_1_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"get_claims/1 extracts all claims from token", fun() ->
              ExtraClaims = #{<<"role">> => <<"admin">>, <<"level">> => 5},
              {ok, Token} = flurm_jwt:generate(<<"user">>, ExtraClaims),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"user">>, maps:get(<<"sub">>, Claims)),
              ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims)),
              ?assertEqual(5, maps:get(<<"level">>, Claims))
          end},

          {"get_claims/1 works for expired tokens", fun() ->
              %% Create an expired token
              PastExp = erlang:system_time(second) - 99999,
              Token = create_token_with_expiry(<<"user">>, PastExp),
              %% get_claims should still work
              Result = flurm_jwt:get_claims(Token),
              ?assertMatch({ok, _}, Result),
              {ok, Claims} = Result,
              ?assertEqual(<<"user">>, maps:get(<<"sub">>, Claims))
          end}
         ]
     end}.

%%%===================================================================
%%% refresh/1 Tests
%%%===================================================================

refresh_valid_token_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"refresh/1 generates new token with updated timestamps", fun() ->
              {ok, Token1} = flurm_jwt:generate(<<"user">>),
              {ok, Claims1} = flurm_jwt:get_claims(Token1),

              %% Small delay to ensure timestamps differ
              timer:sleep(1100),

              {ok, Token2} = flurm_jwt:refresh(Token1),
              {ok, Claims2} = flurm_jwt:get_claims(Token2),

              %% Subject should be preserved
              ?assertEqual(maps:get(<<"sub">>, Claims1), maps:get(<<"sub">>, Claims2)),
              %% iat should be updated (or same if within same second)
              ?assert(maps:get(<<"iat">>, Claims2) >= maps:get(<<"iat">>, Claims1)),
              %% exp should be updated
              ?assert(maps:get(<<"exp">>, Claims2) >= maps:get(<<"exp">>, Claims1))
          end},

          {"refresh/1 preserves extra claims", fun() ->
              ExtraClaims = #{<<"role">> => <<"admin">>, <<"department">> => <<"engineering">>},
              {ok, Token1} = flurm_jwt:generate(<<"user">>, ExtraClaims),

              {ok, Token2} = flurm_jwt:refresh(Token1),
              {ok, Claims2} = flurm_jwt:get_claims(Token2),

              ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims2)),
              ?assertEqual(<<"engineering">>, maps:get(<<"department">>, Claims2))
          end}
         ]
     end}.

refresh_expired_token_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"refresh/1 allows refreshing expired token", fun() ->
              %% Create an expired token
              PastExp = erlang:system_time(second) - 100,
              Token = create_token_with_expiry(<<"user">>, PastExp),

              %% Verify would fail
              ?assertEqual({error, expired}, flurm_jwt:verify(Token)),

              %% But refresh should work
              Result = flurm_jwt:refresh(Token),
              ?assertMatch({ok, _}, Result),

              %% New token should be valid
              {ok, NewToken} = Result,
              ?assertMatch({ok, _}, flurm_jwt:verify(NewToken))
          end},

          {"refresh/1 preserves subject from expired token", fun() ->
              PastExp = erlang:system_time(second) - 100,
              Token = create_token_with_expiry(<<"original_user">>, PastExp),

              {ok, NewToken} = flurm_jwt:refresh(Token),
              {ok, Claims} = flurm_jwt:get_claims(NewToken),

              ?assertEqual(<<"original_user">>, maps:get(<<"sub">>, Claims))
          end},

          {"refresh/1 preserves extra claims from expired token", fun() ->
              PastExp = erlang:system_time(second) - 100,
              ExtraClaims = #{<<"role">> => <<"superuser">>},
              Token = create_token_with_expiry(<<"user">>, PastExp, ExtraClaims),

              {ok, NewToken} = flurm_jwt:refresh(Token),
              {ok, Claims} = flurm_jwt:get_claims(NewToken),

              ?assertEqual(<<"superuser">>, maps:get(<<"role">>, Claims))
          end}
         ]
     end}.

refresh_invalid_token_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"refresh/1 returns error for tampered token", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [Header, _Payload, Signature] = binary:split(Token, <<".">>, [global]),
              FakePayload = flurm_jwt:encode_base64url(jsx:encode(#{<<"sub">> => <<"hacker">>})),
              TamperedToken = <<Header/binary, ".", FakePayload/binary, ".", Signature/binary>>,
              Result = flurm_jwt:refresh(TamperedToken),
              ?assertEqual({error, invalid_signature}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% Base64URL Encoding/Decoding Tests
%%%===================================================================

base64url_roundtrip_test_() ->
    [
     {"base64url roundtrip for simple data", fun() ->
         Data = <<"hello world">>,
         Encoded = flurm_jwt:encode_base64url(Data),
         Decoded = flurm_jwt:decode_base64url(Encoded),
         ?assertEqual(Data, Decoded)
     end},

     {"base64url roundtrip for binary with special characters", fun() ->
         Data = <<"data with / and + characters">>,
         Encoded = flurm_jwt:encode_base64url(Data),
         Decoded = flurm_jwt:decode_base64url(Encoded),
         ?assertEqual(Data, Decoded)
     end},

     {"base64url roundtrip for JSON data", fun() ->
         Json = jsx:encode(#{<<"key">> => <<"value">>, <<"num">> => 123}),
         Encoded = flurm_jwt:encode_base64url(Json),
         Decoded = flurm_jwt:decode_base64url(Encoded),
         ?assertEqual(Json, Decoded)
     end},

     {"base64url roundtrip for empty binary", fun() ->
         Data = <<>>,
         Encoded = flurm_jwt:encode_base64url(Data),
         Decoded = flurm_jwt:decode_base64url(Encoded),
         ?assertEqual(Data, Decoded)
     end},

     {"base64url roundtrip for binary requiring padding", fun() ->
         %% Test various lengths that require different padding
         lists:foreach(fun(Len) ->
             Data = list_to_binary(lists:duplicate(Len, $x)),
             Encoded = flurm_jwt:encode_base64url(Data),
             Decoded = flurm_jwt:decode_base64url(Encoded),
             ?assertEqual(Data, Decoded)
         end, [1, 2, 3, 4, 5, 10, 100])
     end},

     {"base64url encoding removes padding", fun() ->
         %% Data that would normally have '=' padding
         Data = <<"ab">>,  % Would be "YWI=" in standard base64
         Encoded = flurm_jwt:encode_base64url(Data),
         ?assertEqual(nomatch, binary:match(Encoded, <<"=">>))
     end},

     {"base64url encoding replaces + with -", fun() ->
         %% Find data that produces + in standard base64
         %% The binary <<251, 239>> produces "++" in standard base64
         Data = <<251, 239>>,
         Encoded = flurm_jwt:encode_base64url(Data),
         ?assertEqual(nomatch, binary:match(Encoded, <<"+">>))
     end},

     {"base64url encoding replaces / with _", fun() ->
         %% Find data that produces / in standard base64
         %% The binary <<255, 255>> produces "//" in standard base64
         Data = <<255, 255>>,
         Encoded = flurm_jwt:encode_base64url(Data),
         ?assertEqual(nomatch, binary:match(Encoded, <<"/">>))
     end}
    ].

%%%===================================================================
%%% Constant-Time Comparison Tests
%%%===================================================================

constant_time_comparison_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"constant time comparison rejects different length signatures", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [Header, Payload, _Signature] = binary:split(Token, <<".">>, [global]),
              %% Create a signature of different length
              ShortSig = flurm_jwt:encode_base64url(<<"short">>),
              TamperedToken = <<Header/binary, ".", Payload/binary, ".", ShortSig/binary>>,
              Result = flurm_jwt:verify(TamperedToken),
              ?assertEqual({error, invalid_signature}, Result)
          end},

          {"constant time comparison handles same-length different signatures", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [Header, Payload, Signature] = binary:split(Token, <<".">>, [global]),
              %% Modify one byte in the signature (keeping same length after base64)
              DecodedSig = flurm_jwt:decode_base64url(Signature),
              <<FirstByte:8, Rest/binary>> = DecodedSig,
              ModifiedSig = <<(FirstByte bxor 1):8, Rest/binary>>,
              ModifiedSigB64 = flurm_jwt:encode_base64url(ModifiedSig),
              TamperedToken = <<Header/binary, ".", Payload/binary, ".", ModifiedSigB64/binary>>,
              Result = flurm_jwt:verify(TamperedToken),
              ?assertEqual({error, invalid_signature}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% Invalid Token Format Tests
%%%===================================================================

invalid_token_format_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"verify returns error for token with only one part", fun() ->
              Result = flurm_jwt:verify(<<"single_part">>),
              ?assertMatch({error, _}, Result)
          end},

          {"verify returns error for token with two parts", fun() ->
              Result = flurm_jwt:verify(<<"part1.part2">>),
              ?assertMatch({error, _}, Result)
          end},

          {"verify returns error for token with four parts", fun() ->
              Result = flurm_jwt:verify(<<"part1.part2.part3.part4">>),
              ?assertMatch({error, _}, Result)
          end},

          {"verify returns error for empty token", fun() ->
              Result = flurm_jwt:verify(<<>>),
              ?assertMatch({error, _}, Result)
          end},

          {"verify returns error for malformed base64 in header", fun() ->
              Result = flurm_jwt:verify(<<"!!!invalid!!!.payload.signature">>),
              ?assertMatch({error, _}, Result)
          end},

          {"verify returns error for malformed JSON in header", fun() ->
              %% Valid base64 but invalid JSON
              BadHeader = flurm_jwt:encode_base64url(<<"not json">>),
              Token = <<BadHeader/binary, ".payload.signature">>,
              Result = flurm_jwt:verify(Token),
              ?assertMatch({error, _}, Result)
          end},

          {"verify returns error for malformed JSON in payload", fun() ->
              Header = flurm_jwt:encode_base64url(jsx:encode(#{<<"alg">> => <<"HS256">>, <<"typ">> => <<"JWT">>})),
              BadPayload = flurm_jwt:encode_base64url(<<"not json">>),
              Signature = flurm_jwt:encode_base64url(<<"sig">>),
              Token = <<Header/binary, ".", BadPayload/binary, ".", Signature/binary>>,
              Result = flurm_jwt:verify(Token),
              ?assertMatch({error, _}, Result)
          end},

          {"get_claims returns error for invalid token", fun() ->
              Result = flurm_jwt:get_claims(<<"not.valid.token">>),
              ?assertMatch({error, _}, Result)
          end},

          {"decode returns error for invalid token", fun() ->
              Result = flurm_jwt:decode(<<"invalid">>),
              ?assertMatch({error, _}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% Algorithm Verification Tests
%%%===================================================================

algorithm_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"verify accepts HS256 algorithm", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:verify(Token),
              ?assertEqual(<<"user">>, maps:get(<<"sub">>, Claims))
          end},

          {"verify rejects unsupported algorithm", fun() ->
              %% Create a token with different algorithm in header
              Header = flurm_jwt:encode_base64url(jsx:encode(#{<<"alg">> => <<"RS256">>, <<"typ">> => <<"JWT">>})),
              Payload = flurm_jwt:encode_base64url(jsx:encode(#{<<"sub">> => <<"user">>})),
              Signature = flurm_jwt:encode_base64url(<<"fake_sig">>),
              Token = <<Header/binary, ".", Payload/binary, ".", Signature/binary>>,
              Result = flurm_jwt:verify(Token),
              ?assertMatch({error, {unsupported_algorithm, <<"RS256">>}}, Result)
          end},

          {"verify rejects none algorithm", fun() ->
              Header = flurm_jwt:encode_base64url(jsx:encode(#{<<"alg">> => <<"none">>, <<"typ">> => <<"JWT">>})),
              Payload = flurm_jwt:encode_base64url(jsx:encode(#{<<"sub">> => <<"user">>})),
              Signature = flurm_jwt:encode_base64url(<<>>),
              Token = <<Header/binary, ".", Payload/binary, ".", Signature/binary>>,
              Result = flurm_jwt:verify(Token),
              ?assertMatch({error, {unsupported_algorithm, <<"none">>}}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% Token Header Structure Tests
%%%===================================================================

header_structure_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"generated token has correct header structure", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              [HeaderB64, _, _] = binary:split(Token, <<".">>, [global]),
              HeaderJson = flurm_jwt:decode_base64url(HeaderB64),
              Header = jsx:decode(HeaderJson, [return_maps]),
              ?assertEqual(<<"HS256">>, maps:get(<<"alg">>, Header)),
              ?assertEqual(<<"JWT">>, maps:get(<<"typ">>, Header))
          end}
         ]
     end}.

%%%===================================================================
%%% Internal Function Tests (via -ifdef(TEST) exports)
%%%===================================================================

build_header_test_() ->
    [
     {"build_header returns correct map", fun() ->
         Header = flurm_jwt:build_header(),
         ?assertEqual(#{<<"alg">> => <<"HS256">>, <<"typ">> => <<"JWT">>}, Header)
     end}
    ].

build_payload_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"build_payload includes all required claims", fun() ->
              Payload = flurm_jwt:build_payload(<<"test_user">>, #{}),
              ?assertEqual(<<"test_user">>, maps:get(<<"sub">>, Payload)),
              ?assert(maps:is_key(<<"iss">>, Payload)),
              ?assert(maps:is_key(<<"iat">>, Payload)),
              ?assert(maps:is_key(<<"exp">>, Payload)),
              ?assert(maps:is_key(<<"cluster_id">>, Payload))
          end},

          {"build_payload merges extra claims", fun() ->
              ExtraClaims = #{<<"role">> => <<"admin">>},
              Payload = flurm_jwt:build_payload(<<"user">>, ExtraClaims),
              ?assertEqual(<<"admin">>, maps:get(<<"role">>, Payload))
          end}
         ]
     end}.

sign_test_() ->
    [
     {"sign produces consistent output", fun() ->
         Message = <<"test message">>,
         Secret = <<"secret">>,
         Sig1 = flurm_jwt:sign(Message, Secret),
         Sig2 = flurm_jwt:sign(Message, Secret),
         ?assertEqual(Sig1, Sig2)
     end},

     {"sign produces different output for different messages", fun() ->
         Secret = <<"secret">>,
         Sig1 = flurm_jwt:sign(<<"message1">>, Secret),
         Sig2 = flurm_jwt:sign(<<"message2">>, Secret),
         ?assertNotEqual(Sig1, Sig2)
     end},

     {"sign produces different output for different secrets", fun() ->
         Message = <<"message">>,
         Sig1 = flurm_jwt:sign(Message, <<"secret1">>),
         Sig2 = flurm_jwt:sign(Message, <<"secret2">>),
         ?assertNotEqual(Sig1, Sig2)
     end}
    ].

get_secret_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"get_secret returns default when not configured", fun() ->
              Secret = flurm_jwt:get_secret(),
              ?assertEqual(<<"flurm-development-secret-change-in-production">>, Secret)
          end},

          {"get_secret returns configured binary secret", fun() ->
              application:set_env(flurm_controller, jwt_secret, <<"my_secret">>),
              Secret = flurm_jwt:get_secret(),
              ?assertEqual(<<"my_secret">>, Secret),
              application:unset_env(flurm_controller, jwt_secret)
          end},

          {"get_secret converts list secret to binary", fun() ->
              application:set_env(flurm_controller, jwt_secret, "list_secret"),
              Secret = flurm_jwt:get_secret(),
              ?assertEqual(<<"list_secret">>, Secret),
              application:unset_env(flurm_controller, jwt_secret)
          end}
         ]
     end}.

parse_token_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"parse_token correctly parses valid token", fun() ->
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              Result = flurm_jwt:parse_token(Token),
              ?assertMatch({ok, _, _, _}, Result),
              {ok, Header, Payload, Signature} = Result,
              ?assertEqual(<<"HS256">>, maps:get(<<"alg">>, Header)),
              ?assertEqual(<<"user">>, maps:get(<<"sub">>, Payload)),
              ?assert(is_binary(Signature))
          end},

          {"parse_token returns error for invalid format", fun() ->
              Result = flurm_jwt:parse_token(<<"invalid">>),
              ?assertEqual({error, invalid_token_format}, Result)
          end},

          {"parse_token returns error for two parts", fun() ->
              Result = flurm_jwt:parse_token(<<"part1.part2">>),
              ?assertEqual({error, invalid_token_format}, Result)
          end}
         ]
     end}.

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

edge_cases_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"generate handles empty subject", fun() ->
              {ok, Token} = flurm_jwt:generate(<<>>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<>>, maps:get(<<"sub">>, Claims))
          end},

          {"generate handles long subject", fun() ->
              LongSubject = list_to_binary(lists:duplicate(1000, $x)),
              {ok, Token} = flurm_jwt:generate(LongSubject),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(LongSubject, maps:get(<<"sub">>, Claims))
          end},

          {"generate handles unicode subject", fun() ->
              UnicodeSubject = <<"user_\x{4e2d}\x{6587}"/utf8>>,
              {ok, Token} = flurm_jwt:generate(UnicodeSubject),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(UnicodeSubject, maps:get(<<"sub">>, Claims))
          end},

          {"generate handles complex extra claims", fun() ->
              ExtraClaims = #{
                  <<"nested">> => #{<<"deep">> => #{<<"value">> => 42}},
                  <<"list">> => [1, 2, 3],
                  <<"bool">> => true,
                  <<"null">> => null
              },
              {ok, Token} = flurm_jwt:generate(<<"user">>, ExtraClaims),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(#{<<"deep">> => #{<<"value">> => 42}}, maps:get(<<"nested">>, Claims)),
              ?assertEqual([1, 2, 3], maps:get(<<"list">>, Claims))
          end}
         ]
     end}.

%%%===================================================================
%%% Cluster ID Configuration Tests
%%%===================================================================

cluster_id_config_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"cluster_id defaults to 'default' when not configured", fun() ->
              application:unset_env(flurm_core, cluster_name),
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"default">>, maps:get(<<"cluster_id">>, Claims))
          end},

          {"cluster_id uses configured binary value", fun() ->
              application:set_env(flurm_core, cluster_name, <<"production">>),
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"production">>, maps:get(<<"cluster_id">>, Claims)),
              application:unset_env(flurm_core, cluster_name)
          end},

          {"cluster_id converts list to binary", fun() ->
              application:set_env(flurm_core, cluster_name, "staging"),
              {ok, Token} = flurm_jwt:generate(<<"user">>),
              {ok, Claims} = flurm_jwt:get_claims(Token),
              ?assertEqual(<<"staging">>, maps:get(<<"cluster_id">>, Claims)),
              application:unset_env(flurm_core, cluster_name)
          end}
         ]
     end}.

%%%===================================================================
%%% Full Integration Tests
%%%===================================================================

integration_test_() ->
    {setup,
     fun setup_app_env/0,
     fun cleanup_app_env/1,
     fun(_) ->
         [
          {"full token lifecycle: generate -> verify -> refresh -> verify", fun() ->
              %% Generate initial token
              {ok, Token1} = flurm_jwt:generate(<<"lifecycle_user">>, #{<<"role">> => <<"tester">>}),

              %% Verify it works
              {ok, Claims1} = flurm_jwt:verify(Token1),
              ?assertEqual(<<"lifecycle_user">>, maps:get(<<"sub">>, Claims1)),
              ?assertEqual(<<"tester">>, maps:get(<<"role">>, Claims1)),

              %% Refresh the token
              {ok, Token2} = flurm_jwt:refresh(Token1),

              %% Verify refreshed token works
              {ok, Claims2} = flurm_jwt:verify(Token2),
              ?assertEqual(<<"lifecycle_user">>, maps:get(<<"sub">>, Claims2)),
              ?assertEqual(<<"tester">>, maps:get(<<"role">>, Claims2))
          end},

          {"tokens from different secrets are incompatible", fun() ->
              application:set_env(flurm_controller, jwt_secret, <<"secret_a">>),
              {ok, TokenA} = flurm_jwt:generate(<<"user">>),

              application:set_env(flurm_controller, jwt_secret, <<"secret_b">>),
              {ok, TokenB} = flurm_jwt:generate(<<"user">>),

              %% TokenA cannot be verified with secret_b
              ?assertEqual({error, invalid_signature}, flurm_jwt:verify(TokenA)),

              %% TokenB is valid with current secret (secret_b)
              ?assertMatch({ok, _}, flurm_jwt:verify(TokenB)),

              %% But TokenA can be verified with explicit secret_a
              ?assertMatch({ok, _}, flurm_jwt:verify(TokenA, <<"secret_a">>))
          end}
         ]
     end}.
