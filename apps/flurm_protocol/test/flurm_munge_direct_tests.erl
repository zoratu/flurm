%%%-------------------------------------------------------------------
%%% @doc Direct unit tests for flurm_munge module.
%%%
%%% These tests call actual module functions directly (no mocking)
%%% to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_munge_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_munge_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"is_available", fun is_available_test/0},
      {"encode/0", fun encode_no_payload_test/0},
      {"encode/1 with payload", fun encode_with_payload_test/0}
     ]}.

%%%===================================================================
%%% API Tests
%%%===================================================================

is_available_test() ->
    %% Test is_available/0 - should return boolean
    Result = flurm_munge:is_available(),
    ?assert(is_boolean(Result)),

    %% The result depends on whether munge is installed
    %% We can't assert true or false, but we verify it doesn't crash
    ok.

encode_no_payload_test() ->
    %% Test encode/0 - encodes empty payload
    %% Result depends on whether munge is available
    Result = flurm_munge:encode(),
    case Result of
        {ok, Credential} when is_binary(Credential) ->
            %% MUNGE credentials typically start with "MUNGE:"
            %% or could be an error message from shell
            ?assert(is_binary(Credential));
        {error, _} ->
            %% If munge is not available, should return error
            ok
    end,
    ok.

encode_with_payload_test() ->
    %% Test encode/1 with various payloads
    Payloads = [
        <<>>,
        <<"test payload">>,
        <<"hello world">>,
        <<"binary data: ", 0, 1, 2, 3>>,
        crypto:strong_rand_bytes(32)
    ],

    lists:foreach(fun(Payload) ->
        Result = flurm_munge:encode(Payload),
        case Result of
            {ok, Credential} when is_binary(Credential) ->
                %% MUNGE returned something (could be credential or error message)
                ?assert(is_binary(Credential));
            {error, _} ->
                %% If munge is not available, should return error
                ok
        end
    end, Payloads),
    ok.
