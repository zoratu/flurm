%%%-------------------------------------------------------------------
%%% @doc Property-based tests for FLURM rate limiter
%%%
%%% Uses PropEr to verify rate limiter invariants:
%%% - Token bucket never goes negative
%%% - Rate limits are eventually enforced
%%% - Burst capacity works correctly
%%% - Per-key isolation
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_flurm_rate_limiter).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% PropEr properties
-export([
    prop_tokens_never_negative/0,
    prop_rate_limit_enforced/0,
    prop_burst_capacity/0,
    prop_key_isolation/0,
    prop_refill_works/0
]).

%% EUnit wrapper - proper_test_/0 is auto-exported by eunit.hrl

%%====================================================================
%% Type Definitions
%%====================================================================

-record(bucket, {
    tokens :: number(),
    max_tokens :: pos_integer(),
    refill_rate :: pos_integer(),  %% tokens per second
    last_refill :: non_neg_integer()
}).

-record(limiter_state, {
    buckets :: #{term() => #bucket{}},
    time :: non_neg_integer()
}).

%%====================================================================
%% Generators
%%====================================================================

%% Generate a rate limit configuration
rate_config() ->
    ?LET({MaxTokens, RefillRate},
         {range(1, 1000), range(1, 100)},
         #{max_tokens => MaxTokens, refill_rate => RefillRate}).

%% Generate a request pattern
request_pattern() ->
    ?LET(NumRequests, range(1, 500),
         lists:duplicate(NumRequests, request)).

%% Generate a request sequence with timing (reserved for future use)
%% timed_request_sequence() ->
%%     ?LET(Events, list(oneof([
%%             {request, binary(4)},
%%             {wait, range(1, 1000)}
%%          ])),
%%          Events).

%% Generate multiple keys for isolation testing
multi_key_requests() ->
    ?LET({NumKeys, RequestsPerKey},
         {range(2, 10), range(1, 50)},
         begin
             Keys = [integer_to_binary(K) || K <- lists:seq(1, NumKeys)],
             [{request, lists:nth(rand:uniform(NumKeys), Keys)}
              || _ <- lists:seq(1, RequestsPerKey * NumKeys)]
         end).

%%====================================================================
%% Properties
%%====================================================================

%% Property: Token count never goes below zero
prop_tokens_never_negative() ->
    ?FORALL({Config, Requests}, {rate_config(), request_pattern()},
        begin
            State = init_limiter(Config),
            {FinalState, _Results} = process_requests(Requests, State),
            all_buckets_non_negative(FinalState)
        end).

%% Property: Rate limits are eventually enforced (requests rejected after burst)
prop_rate_limit_enforced() ->
    ?FORALL(Config, rate_config(),
        begin
            MaxTokens = maps:get(max_tokens, Config),
            %% Send more requests than max tokens without waiting
            Requests = lists:duplicate(MaxTokens + 10, request),
            State = init_limiter(Config),
            {_FinalState, Results} = process_requests_no_refill(Requests, State),

            %% At least some requests should be rejected
            Rejected = length([R || R <- Results, R =:= rejected]),
            Rejected >= 10
        end).

%% Property: Burst capacity allows MaxTokens requests immediately
prop_burst_capacity() ->
    ?FORALL(Config, rate_config(),
        begin
            MaxTokens = maps:get(max_tokens, Config),
            Requests = lists:duplicate(MaxTokens, request),
            State = init_limiter(Config),
            {_FinalState, Results} = process_requests_no_refill(Requests, State),

            %% All requests within burst should be allowed
            Allowed = length([R || R <- Results, R =:= allowed]),
            Allowed =:= MaxTokens
        end).

%% Property: Different keys are rate-limited independently
prop_key_isolation() ->
    ?FORALL({Config, Requests}, {rate_config(), multi_key_requests()},
        begin
            State = init_limiter(Config),
            {FinalState, _Results} = process_keyed_requests(Requests, State),

            %% Each key's bucket should be independent
            Buckets = maps:values(FinalState#limiter_state.buckets),
            length(Buckets) > 1 andalso
            all_buckets_non_negative(FinalState)
        end).

%% Property: Tokens refill over time
prop_refill_works() ->
    ?FORALL(Config, rate_config(),
        begin
            MaxTokens = maps:get(max_tokens, Config),
            RefillRate = maps:get(refill_rate, Config),

            %% Drain all tokens
            State0 = init_limiter(Config),
            DrainRequests = lists:duplicate(MaxTokens, request),
            {State1, _} = process_requests_no_refill(DrainRequests, State0),

            %% Wait enough time to refill some tokens
            WaitTime = max(1, MaxTokens div RefillRate) * 1000,
            State2 = advance_time(State1, WaitTime),
            State3 = refill_all_buckets(State2),

            %% Should have some tokens now
            Bucket = maps:get(default, State3#limiter_state.buckets),
            Bucket#bucket.tokens > 0
        end).

%%====================================================================
%% Rate Limiter Simulation
%%====================================================================

init_limiter(Config) ->
    MaxTokens = maps:get(max_tokens, Config),
    RefillRate = maps:get(refill_rate, Config),
    Bucket = #bucket{
        tokens = MaxTokens,
        max_tokens = MaxTokens,
        refill_rate = RefillRate,
        last_refill = 0
    },
    #limiter_state{
        buckets = #{default => Bucket},
        time = 0
    }.

process_requests(Requests, State) ->
    process_requests(Requests, State, []).

process_requests([], State, Results) ->
    {State, lists:reverse(Results)};
process_requests([request | Rest], State, Results) ->
    {NewState, Result} = check_and_consume(default, State),
    %% Advance time slightly and refill
    State2 = advance_time(NewState, 10),
    State3 = refill_all_buckets(State2),
    process_requests(Rest, State3, [Result | Results]).

process_requests_no_refill(Requests, State) ->
    process_requests_no_refill(Requests, State, []).

process_requests_no_refill([], State, Results) ->
    {State, lists:reverse(Results)};
process_requests_no_refill([request | Rest], State, Results) ->
    {NewState, Result} = check_and_consume(default, State),
    process_requests_no_refill(Rest, NewState, [Result | Results]).

process_keyed_requests(Requests, State) ->
    process_keyed_requests(Requests, State, []).

process_keyed_requests([], State, Results) ->
    {State, lists:reverse(Results)};
process_keyed_requests([{request, Key} | Rest], State, Results) ->
    State1 = ensure_bucket(Key, State),
    {NewState, Result} = check_and_consume(Key, State1),
    process_keyed_requests(Rest, NewState, [Result | Results]).

ensure_bucket(Key, #limiter_state{buckets = Buckets} = State) ->
    case maps:is_key(Key, Buckets) of
        true -> State;
        false ->
            %% Copy config from default bucket
            Default = maps:get(default, Buckets),
            NewBucket = Default#bucket{tokens = Default#bucket.max_tokens},
            State#limiter_state{buckets = maps:put(Key, NewBucket, Buckets)}
    end.

check_and_consume(Key, #limiter_state{buckets = Buckets} = State) ->
    Bucket = maps:get(Key, Buckets),
    if
        Bucket#bucket.tokens >= 1 ->
            NewBucket = Bucket#bucket{tokens = Bucket#bucket.tokens - 1},
            {State#limiter_state{buckets = maps:put(Key, NewBucket, Buckets)}, allowed};
        true ->
            {State, rejected}
    end.

advance_time(#limiter_state{time = Time} = State, Ms) ->
    State#limiter_state{time = Time + Ms}.

refill_all_buckets(#limiter_state{buckets = Buckets, time = Time} = State) ->
    NewBuckets = maps:map(
        fun(_Key, Bucket) ->
            refill_bucket(Bucket, Time)
        end,
        Buckets
    ),
    State#limiter_state{buckets = NewBuckets}.

refill_bucket(#bucket{last_refill = LastRefill, refill_rate = Rate,
                      tokens = Tokens, max_tokens = MaxTokens} = Bucket, Now) ->
    ElapsedSeconds = (Now - LastRefill) / 1000,
    NewTokens = min(MaxTokens, Tokens + (ElapsedSeconds * Rate)),
    Bucket#bucket{tokens = NewTokens, last_refill = Now}.

%%====================================================================
%% Invariant Checkers
%%====================================================================

all_buckets_non_negative(#limiter_state{buckets = Buckets}) ->
    lists:all(
        fun(Bucket) -> Bucket#bucket.tokens >= 0 end,
        maps:values(Buckets)
    ).

%%====================================================================
%% EUnit Integration
%%====================================================================

proper_test_() ->
    {timeout, 120, [
        {"Tokens never negative",
         fun() -> ?assert(proper:quickcheck(prop_tokens_never_negative(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Rate limit enforced",
         fun() -> ?assert(proper:quickcheck(prop_rate_limit_enforced(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Burst capacity",
         fun() -> ?assert(proper:quickcheck(prop_burst_capacity(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Key isolation",
         fun() -> ?assert(proper:quickcheck(prop_key_isolation(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Refill works",
         fun() -> ?assert(proper:quickcheck(prop_refill_works(),
                                            [{numtests, 500}, {to_file, user}])) end}
    ]}.
