%%%-------------------------------------------------------------------
%%% @doc Tests for -ifdef(TEST) exported functions in flurm_controller_connector
%%%
%%% These tests directly call the internal helper functions that are
%%% exported via -ifdef(TEST) to provide real code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% decode_messages/2 Tests
%%====================================================================

decode_messages_empty_buffer_test() ->
    %% Empty buffer should return empty list with empty buffer
    {Messages, Remaining} = flurm_controller_connector:decode_messages(<<>>, []),
    ?assertEqual([], Messages),
    ?assertEqual(<<>>, Remaining).

decode_messages_incomplete_header_test() ->
    %% Buffer too short for header (less than 4 bytes for length)
    {Messages, Remaining} = flurm_controller_connector:decode_messages(<<1, 2>>, []),
    ?assertEqual([], Messages),
    ?assertEqual(<<1, 2>>, Remaining).

decode_messages_incomplete_message_test() ->
    %% Buffer has length but not enough data
    %% Length = 100, but only 10 bytes of data
    Buffer = <<100:32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
    {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
    ?assertEqual([], Messages),
    ?assertEqual(Buffer, Remaining).

decode_messages_with_accumulator_test() ->
    %% Test that accumulator is preserved
    ExistingMsg = #{type => test},
    {Messages, Remaining} = flurm_controller_connector:decode_messages(<<>>, [ExistingMsg]),
    ?assertEqual([ExistingMsg], Messages),
    ?assertEqual(<<>>, Remaining).

%%====================================================================
%% find_job_by_pid/2 Tests
%%====================================================================

find_job_by_pid_found_test() ->
    %% Test finding a job by PID
    Pid = self(),
    Jobs = #{123 => Pid, 456 => spawn(fun() -> ok end)},
    Result = flurm_controller_connector:find_job_by_pid(Pid, Jobs),
    ?assertEqual({ok, 123}, Result).

find_job_by_pid_not_found_test() ->
    %% Test when PID is not in the map
    Pid = self(),
    Jobs = #{123 => spawn(fun() -> ok end), 456 => spawn(fun() -> ok end)},
    Result = flurm_controller_connector:find_job_by_pid(Pid, Jobs),
    ?assertEqual(error, Result).

find_job_by_pid_empty_map_test() ->
    %% Test with empty jobs map
    Result = flurm_controller_connector:find_job_by_pid(self(), #{}),
    ?assertEqual(error, Result).

%%====================================================================
%% cancel_timer/1 Tests
%%====================================================================

cancel_timer_undefined_test() ->
    %% cancel_timer with undefined should return ok
    Result = flurm_controller_connector:cancel_timer(undefined),
    ?assertEqual(ok, Result).

cancel_timer_valid_ref_test() ->
    %% cancel_timer with a valid timer reference
    Ref = erlang:send_after(60000, self(), test_msg),
    Result = flurm_controller_connector:cancel_timer(Ref),
    %% erlang:cancel_timer returns remaining time or false
    ?assert(is_integer(Result) orelse Result =:= false).

cancel_timer_already_fired_test() ->
    %% cancel_timer with a timer that already fired
    Ref = erlang:send_after(0, self(), test_msg),
    timer:sleep(10),
    Result = flurm_controller_connector:cancel_timer(Ref),
    %% Should return false when timer has already fired
    ?assertEqual(false, Result),
    %% Clean up the message from the mailbox
    receive test_msg -> ok after 0 -> ok end.

%%====================================================================
%% detect_features/0 Tests
%%====================================================================

detect_features_test() ->
    %% detect_features should return a list of binaries
    Features = flurm_controller_connector:detect_features(),
    ?assert(is_list(Features)),
    lists:foreach(fun(F) ->
        ?assert(is_binary(F))
    end, Features).

detect_features_returns_list_test() ->
    %% The result should always be a list
    Result = flurm_controller_connector:detect_features(),
    ?assert(is_list(Result)).

%%====================================================================
%% check_cpu_flag/1 Tests
%%====================================================================

check_cpu_flag_avx_test() ->
    %% Check for AVX flag - result depends on system but should be boolean
    Result = flurm_controller_connector:check_cpu_flag("avx"),
    ?assert(is_boolean(Result)).

check_cpu_flag_nonexistent_test() ->
    %% Check for a flag that definitely doesn't exist
    Result = flurm_controller_connector:check_cpu_flag("nonexistent_flag_xyz123"),
    ?assertEqual(false, Result).

check_cpu_flag_empty_test() ->
    %% Empty flag string
    Result = flurm_controller_connector:check_cpu_flag(""),
    %% Empty string might match, but result should be boolean
    ?assert(is_boolean(Result)).

check_cpu_flag_sse_test() ->
    %% Check for SSE flag
    Result = flurm_controller_connector:check_cpu_flag("sse"),
    ?assert(is_boolean(Result)).

%%====================================================================
%% Test Fixtures
%%====================================================================

decode_messages_test_() ->
    [
        {"empty buffer returns empty list", fun decode_messages_empty_buffer_test/0},
        {"incomplete header preserved", fun decode_messages_incomplete_header_test/0},
        {"incomplete message preserved", fun decode_messages_incomplete_message_test/0},
        {"accumulator preserved", fun decode_messages_with_accumulator_test/0}
    ].

find_job_by_pid_test_() ->
    [
        {"finds job by pid", fun find_job_by_pid_found_test/0},
        {"returns error when not found", fun find_job_by_pid_not_found_test/0},
        {"handles empty map", fun find_job_by_pid_empty_map_test/0}
    ].

cancel_timer_test_() ->
    [
        {"handles undefined", fun cancel_timer_undefined_test/0},
        {"cancels valid timer", fun cancel_timer_valid_ref_test/0},
        {"handles already fired timer", fun cancel_timer_already_fired_test/0}
    ].

detect_features_test_() ->
    [
        {"returns list of binaries", fun detect_features_test/0},
        {"always returns list", fun detect_features_returns_list_test/0}
    ].

check_cpu_flag_test_() ->
    [
        {"checks avx flag", fun check_cpu_flag_avx_test/0},
        {"returns false for nonexistent", fun check_cpu_flag_nonexistent_test/0},
        {"handles empty string", fun check_cpu_flag_empty_test/0},
        {"checks sse flag", fun check_cpu_flag_sse_test/0}
    ].
