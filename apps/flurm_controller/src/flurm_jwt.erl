%%%-------------------------------------------------------------------
%%% @doc FLURM JWT Token Handler - Cross-Realm Authentication
%%%
%%% This module provides JWT (JSON Web Token) generation and validation
%%% for cross-realm authentication between FLURM clusters and the
%%% SLURM bridge REST API.
%%%
%%% JWT tokens are used to:
%%% - Authenticate REST API requests to the bridge management endpoints
%%% - Enable secure cross-cluster communication during federation
%%% - Provide stateless authentication for the REST API
%%%
%%% Token Structure:
%%% - Header: Algorithm (HS256) and type (JWT)
%%% - Payload: Claims including sub, iss, exp, iat, cluster_id
%%% - Signature: HMAC-SHA256 of header.payload with secret key
%%%
%%% Configuration:
%%% - jwt_secret: The secret key for signing (required for production)
%%% - jwt_issuer: The issuer claim (default: "flurm")
%%% - jwt_expiry: Token expiry in seconds (default: 3600)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_jwt).

-export([
    generate/1,
    generate/2,
    verify/1,
    verify/2,
    decode/1,
    get_claims/1,
    refresh/1
]).

%% For testing
-ifdef(TEST).
-export([
    encode_base64url/1,
    decode_base64url/1,
    build_header/0,
    build_payload/2,
    sign/2,
    get_secret/0,
    parse_token/1
]).
-endif.

-define(DEFAULT_EXPIRY, 3600).  % 1 hour
-define(DEFAULT_ISSUER, <<"flurm">>).
-define(DEFAULT_SECRET, <<"flurm-development-secret-change-in-production">>).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Generate a JWT token for the given subject.
%% The subject is typically a username or service identifier.
-spec generate(Subject :: binary()) -> {ok, Token :: binary()} | {error, term()}.
generate(Subject) ->
    generate(Subject, #{}).

%% @doc Generate a JWT token with additional claims.
%% Extra claims can include roles, permissions, or other metadata.
-spec generate(Subject :: binary(), ExtraClaims :: map()) ->
    {ok, Token :: binary()} | {error, term()}.
generate(Subject, ExtraClaims) ->
    try
        Header = build_header(),
        Payload = build_payload(Subject, ExtraClaims),
        Secret = get_secret(),

        HeaderB64 = encode_base64url(jsx:encode(Header)),
        PayloadB64 = encode_base64url(jsx:encode(Payload)),

        Message = <<HeaderB64/binary, ".", PayloadB64/binary>>,
        Signature = sign(Message, Secret),
        SignatureB64 = encode_base64url(Signature),

        Token = <<Message/binary, ".", SignatureB64/binary>>,
        {ok, Token}
    catch
        Error:Reason ->
            lager:error("JWT generation failed: ~p:~p", [Error, Reason]),
            {error, {generation_failed, Reason}}
    end.

%% @doc Verify a JWT token and return the claims if valid.
%% Uses the configured jwt_secret for verification.
-spec verify(Token :: binary()) ->
    {ok, Claims :: map()} | {error, term()}.
verify(Token) ->
    verify(Token, get_secret()).

%% @doc Verify a JWT token with a specific secret key.
%% Useful for cross-cluster authentication where each cluster has its own key.
-spec verify(Token :: binary(), Secret :: binary()) ->
    {ok, Claims :: map()} | {error, term()}.
verify(Token, Secret) ->
    try
        case parse_token(Token) of
            {ok, Header, Payload, Signature} ->
                %% Verify algorithm
                case maps:get(<<"alg">>, Header, undefined) of
                    <<"HS256">> ->
                        verify_signature_with_secret(Payload, Signature, Token, Secret);
                    Alg ->
                        {error, {unsupported_algorithm, Alg}}
                end;
            {error, ParseReason} ->
                {error, ParseReason}
        end
    catch
        Error:CatchReason ->
            lager:warning("JWT verification failed: ~p:~p", [Error, CatchReason]),
            {error, {verification_failed, CatchReason}}
    end.

%% @doc Decode a JWT token and return the claims without verification.
%% This is useful for inspecting token contents without validating.
%% WARNING: Do not trust claims from decode/1 without calling verify/1.
-spec decode(Token :: binary()) ->
    {ok, Claims :: map()} | {error, term()}.
decode(Token) ->
    get_claims(Token).

%% @doc Get claims from a token without full verification.
%% Useful for debugging or extracting info from expired tokens.
-spec get_claims(Token :: binary()) ->
    {ok, Claims :: map()} | {error, term()}.
get_claims(Token) ->
    case parse_token(Token) of
        {ok, _Header, Payload, _Signature} ->
            {ok, Payload};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Refresh a token by generating a new one with the same claims.
%% The subject and extra claims are preserved, but timestamps are updated.
-spec refresh(Token :: binary()) ->
    {ok, NewToken :: binary()} | {error, term()}.
refresh(Token) ->
    case verify(Token) of
        {ok, Claims} ->
            Subject = maps:get(<<"sub">>, Claims, <<>>),
            %% Remove standard claims that will be regenerated
            ExtraClaims = maps:without([<<"sub">>, <<"iss">>, <<"iat">>, <<"exp">>], Claims),
            generate(Subject, ExtraClaims);
        {error, expired} ->
            %% Allow refresh of expired tokens
            case get_claims(Token) of
                {ok, Claims} ->
                    Subject = maps:get(<<"sub">>, Claims, <<>>),
                    ExtraClaims = maps:without([<<"sub">>, <<"iss">>, <<"iat">>, <<"exp">>], Claims),
                    generate(Subject, ExtraClaims);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

build_header() ->
    #{
        <<"alg">> => <<"HS256">>,
        <<"typ">> => <<"JWT">>
    }.

build_payload(Subject, ExtraClaims) ->
    Now = erlang:system_time(second),
    Expiry = application:get_env(flurm_controller, jwt_expiry, ?DEFAULT_EXPIRY),
    Issuer = application:get_env(flurm_controller, jwt_issuer, ?DEFAULT_ISSUER),
    ClusterId = get_cluster_id(),

    BaseClaims = #{
        <<"sub">> => Subject,
        <<"iss">> => Issuer,
        <<"iat">> => Now,
        <<"exp">> => Now + Expiry,
        <<"cluster_id">> => ClusterId
    },

    maps:merge(BaseClaims, ExtraClaims).

get_cluster_id() ->
    case application:get_env(flurm_core, cluster_name) of
        {ok, Name} when is_binary(Name) -> Name;
        {ok, Name} when is_list(Name) -> list_to_binary(Name);
        undefined -> <<"default">>
    end.

get_secret() ->
    case application:get_env(flurm_controller, jwt_secret) of
        {ok, Secret} when is_binary(Secret) ->
            Secret;
        {ok, Secret} when is_list(Secret) ->
            list_to_binary(Secret);
        undefined ->
            lager:warning("Using default JWT secret - configure jwt_secret for production!"),
            ?DEFAULT_SECRET
    end.

sign(Message, Secret) ->
    crypto:mac(hmac, sha256, Secret, Message).

verify_signature_with_secret(Payload, Signature, Token, Secret) ->
    %% Reconstruct the message from the original token parts
    [HeaderB64, PayloadB64, _] = binary:split(Token, <<".">>, [global]),
    Message = <<HeaderB64/binary, ".", PayloadB64/binary>>,

    ExpectedSig = sign(Message, Secret),

    case constant_time_compare(Signature, ExpectedSig) of
        true ->
            verify_claims(Payload);
        false ->
            {error, invalid_signature}
    end.

verify_claims(Claims) ->
    Now = erlang:system_time(second),
    Exp = maps:get(<<"exp">>, Claims, 0),

    case Exp >= Now of
        true ->
            {ok, Claims};
        false ->
            {error, expired}
    end.

parse_token(Token) ->
    try
        case binary:split(Token, <<".">>, [global]) of
            [HeaderB64, PayloadB64, SignatureB64] ->
                Header = jsx:decode(decode_base64url(HeaderB64), [return_maps]),
                Payload = jsx:decode(decode_base64url(PayloadB64), [return_maps]),
                Signature = decode_base64url(SignatureB64),
                {ok, Header, Payload, Signature};
            _ ->
                {error, invalid_token_format}
        end
    catch
        _:_ ->
            {error, invalid_token_format}
    end.

%% @doc Constant-time comparison to prevent timing attacks.
constant_time_compare(A, B) when byte_size(A) =/= byte_size(B) ->
    false;
constant_time_compare(A, B) ->
    constant_time_compare(A, B, 0).

constant_time_compare(<<>>, <<>>, Acc) ->
    Acc =:= 0;
constant_time_compare(<<A, RestA/binary>>, <<B, RestB/binary>>, Acc) ->
    constant_time_compare(RestA, RestB, Acc bor (A bxor B)).

%% @doc Base64URL encoding (URL-safe base64 without padding).
encode_base64url(Data) ->
    B64 = base64:encode(Data),
    %% Replace + with -, / with _, remove =
    B64_1 = binary:replace(B64, <<"+">>, <<"-">>, [global]),
    B64_2 = binary:replace(B64_1, <<"/">>, <<"_">>, [global]),
    binary:replace(B64_2, <<"=">>, <<>>, [global]).

%% @doc Base64URL decoding (URL-safe base64 without padding).
decode_base64url(Data) ->
    %% Replace - with +, _ with /
    B64_1 = binary:replace(Data, <<"-">>, <<"+">>, [global]),
    B64_2 = binary:replace(B64_1, <<"_">>, <<"/">>, [global]),
    %% Add padding if needed
    Padding = case byte_size(B64_2) rem 4 of
        0 -> <<>>;
        2 -> <<"==">>;
        3 -> <<"=">>
    end,
    base64:decode(<<B64_2/binary, Padding/binary>>).
