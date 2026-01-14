%%%-------------------------------------------------------------------
%%% @doc Direct unit tests for flurm_protocol module.
%%%
%%% These tests call actual module functions directly (no mocking)
%%% to achieve code coverage.
%%%
%%% Note: flurm_protocol.erl uses jsx for JSON encoding which may not
%%% be available in test environment. These tests focus on the type
%%% encoding/decoding internal functions via the public API.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_direct_tests).

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

flurm_protocol_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"encode error handling - missing type", fun encode_error_missing_type_test/0},
      {"encode error handling - missing payload", fun encode_error_missing_payload_test/0},
      {"decode error handling - empty", fun decode_error_empty_test/0},
      {"decode error handling - incomplete", fun decode_error_incomplete_test/0}
     ]}.

%%%===================================================================
%%% Error Handling Tests
%%%===================================================================

encode_error_missing_type_test() ->
    %% Test error case - missing type field
    BadMsg1 = #{payload => #{}},
    Result1 = flurm_protocol:encode(BadMsg1),
    %% Should return error due to missing type
    case Result1 of
        {error, _} -> ok;  %% Expected - no type field
        {ok, _} -> ?assert(false)  %% Should not succeed
    end,
    ok.

encode_error_missing_payload_test() ->
    %% Test error case - missing payload field
    BadMsg2 = #{type => job_submit},
    Result2 = flurm_protocol:encode(BadMsg2),
    %% Should return error due to missing payload
    case Result2 of
        {error, _} -> ok;  %% Expected - no payload field
        {ok, _} -> ?assert(false)  %% Should not succeed
    end,
    ok.

decode_error_empty_test() ->
    %% Test error case - empty binary
    Result1 = flurm_protocol:decode(<<>>),
    ?assertMatch({error, _}, Result1),
    ok.

decode_error_incomplete_test() ->
    %% Test error case - incomplete header (too short)
    Result2 = flurm_protocol:decode(<<1, 2>>),
    ?assertMatch({error, _}, Result2),

    %% Test error case - payload size mismatch
    Result3 = flurm_protocol:decode(<<1:16, 1000:32, "short">>),
    ?assertMatch({error, _}, Result3),

    %% Test decode_message error with bad binary
    Result4 = flurm_protocol:decode_message(<<>>),
    ?assertMatch({error, _}, Result4),
    ok.
