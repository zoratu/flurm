%%%-------------------------------------------------------------------
%%% @doc Cover-Effective Tests for flurm_controller_connector
%%%
%%% Tests the -ifdef(TEST) exported functions:
%%% - decode_messages/2 (lines 332-342)
%%% - find_job_by_pid/2 (lines 514-518)
%%% - cancel_timer/1 (lines 520-521)
%%% - detect_features/0 (lines 565-582)
%%% - check_cpu_flag/1 (lines 584-590)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Helper for meck
%%====================================================================

with_meck(Mods, Fun) ->
    lists:foreach(fun(M) -> meck:new(M, [non_strict]) end, Mods),
    try Fun()
    after
        lists:foreach(fun(M) -> catch meck:unload(M) end, Mods)
    end.

%%====================================================================
%% decode_messages/2 Tests
%%====================================================================

decode_messages_test_() ->
    {"decode_messages/2 coverage tests", [
        fun decode_messages_empty_buffer_test/0,
        fun decode_messages_with_acc_test/0,
        fun decode_messages_partial_header_test/0,
        fun decode_messages_incomplete_data_test/0,
        fun decode_messages_single_complete_test/0,
        fun decode_messages_multiple_complete_test/0,
        fun decode_messages_malformed_skip_test/0,
        fun decode_messages_zero_length_test/0
    ]}.

decode_messages_empty_buffer_test() ->
    %% Empty buffer with no accumulated messages
    {Messages, Remaining} = flurm_controller_connector:decode_messages(<<>>, []),
    ?assertEqual([], Messages),
    ?assertEqual(<<>>, Remaining).

decode_messages_with_acc_test() ->
    %% Empty buffer with accumulated messages - should reverse
    Msg1 = #{type => test1},
    Msg2 = #{type => test2},
    {Messages, Remaining} = flurm_controller_connector:decode_messages(<<>>, [Msg2, Msg1]),
    ?assertEqual([Msg1, Msg2], Messages),
    ?assertEqual(<<>>, Remaining).

decode_messages_partial_header_test() ->
    %% Buffer has < 4 bytes
    {Messages, Remaining} = flurm_controller_connector:decode_messages(<<1, 2>>, []),
    ?assertEqual([], Messages),
    ?assertEqual(<<1, 2>>, Remaining).

decode_messages_incomplete_data_test() ->
    %% Length field present but data is shorter
    Buffer = <<50:32, 1, 2, 3>>,  % Says 50 bytes but only 3
    {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
    ?assertEqual([], Messages),
    ?assertEqual(<<50:32, 1, 2, 3>>, Remaining).

decode_messages_single_complete_test() ->
    %% Single complete message decodes successfully
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun(_) ->
            {ok, #{type => test_msg, payload => #{}}}
        end),
        MsgData = <<0, 1, 2, 3, 4>>,
        Buffer = <<5:32, MsgData/binary>>,
        {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
        ?assertEqual([#{type => test_msg, payload => #{}}], Messages),
        ?assertEqual(<<>>, Remaining)
    end).

decode_messages_multiple_complete_test() ->
    %% Multiple complete messages
    Counter = atomics:new(1, []),
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun(_) ->
            N = atomics:add_get(Counter, 1, 1),
            {ok, #{type => msg, n => N}}
        end),
        Msg1 = <<1, 2, 3>>,
        Msg2 = <<4, 5, 6>>,
        Buffer = <<3:32, Msg1/binary, 3:32, Msg2/binary>>,
        {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
        ?assertEqual(2, length(Messages)),
        ?assertEqual(<<>>, Remaining)
    end).

decode_messages_malformed_skip_test() ->
    %% Malformed message is skipped
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun(_) -> {error, malformed} end),
        Buffer = <<5:32, 1, 2, 3, 4, 5>>,
        {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
        ?assertEqual([], Messages),
        ?assertEqual(<<>>, Remaining)
    end).

decode_messages_zero_length_test() ->
    %% Zero length message
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun(<<>>) ->
            {ok, #{type => empty}}
        end),
        Buffer = <<0:32>>,
        {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
        ?assertEqual([#{type => empty}], Messages),
        ?assertEqual(<<>>, Remaining)
    end).

%%====================================================================
%% find_job_by_pid/2 Tests
%%====================================================================

find_job_by_pid_test_() ->
    {"find_job_by_pid/2 coverage tests", [
        fun find_job_by_pid_empty_map_test/0,
        fun find_job_by_pid_single_match_test/0,
        fun find_job_by_pid_multiple_jobs_match_test/0,
        fun find_job_by_pid_not_found_test/0,
        fun find_job_by_pid_large_map_test/0
    ]}.

find_job_by_pid_empty_map_test() ->
    Result = flurm_controller_connector:find_job_by_pid(self(), #{}),
    ?assertEqual(error, Result).

find_job_by_pid_single_match_test() ->
    Pid = self(),
    Jobs = #{42 => Pid},
    Result = flurm_controller_connector:find_job_by_pid(Pid, Jobs),
    ?assertEqual({ok, 42}, Result).

find_job_by_pid_multiple_jobs_match_test() ->
    Pid = self(),
    P1 = spawn(fun() -> receive _ -> ok end end),
    P2 = spawn(fun() -> receive _ -> ok end end),
    Jobs = #{100 => P1, 200 => Pid, 300 => P2},
    Result = flurm_controller_connector:find_job_by_pid(Pid, Jobs),
    ?assertEqual({ok, 200}, Result),
    P1 ! stop, P2 ! stop.

find_job_by_pid_not_found_test() ->
    SearchPid = self(),
    P1 = spawn(fun() -> receive _ -> ok end end),
    Jobs = #{100 => P1},
    Result = flurm_controller_connector:find_job_by_pid(SearchPid, Jobs),
    ?assertEqual(error, Result),
    P1 ! stop.

find_job_by_pid_large_map_test() ->
    TargetPid = self(),
    Pids = [spawn(fun() -> receive _ -> ok end end) || _ <- lists:seq(1, 20)],
    Jobs0 = maps:from_list(lists:zip(lists:seq(1, 20), Pids)),
    Jobs = maps:put(999, TargetPid, Jobs0),
    Result = flurm_controller_connector:find_job_by_pid(TargetPid, Jobs),
    ?assertEqual({ok, 999}, Result),
    [P ! stop || P <- Pids].

%%====================================================================
%% cancel_timer/1 Tests
%%====================================================================

cancel_timer_test_() ->
    {"cancel_timer/1 coverage tests", [
        fun cancel_timer_undefined_test/0,
        fun cancel_timer_valid_ref_test/0,
        fun cancel_timer_expired_test/0,
        fun cancel_timer_twice_test/0
    ]}.

cancel_timer_undefined_test() ->
    Result = flurm_controller_connector:cancel_timer(undefined),
    ?assertEqual(ok, Result).

cancel_timer_valid_ref_test() ->
    Ref = erlang:send_after(60000, self(), test_msg),
    Result = flurm_controller_connector:cancel_timer(Ref),
    ?assert(is_integer(Result) andalso Result > 0).

cancel_timer_expired_test() ->
    Ref = erlang:send_after(0, self(), test_expired),
    receive test_expired -> ok after 100 -> ok end,
    Result = flurm_controller_connector:cancel_timer(Ref),
    ?assertEqual(false, Result).

cancel_timer_twice_test() ->
    Ref = erlang:send_after(60000, self(), test_double),
    Result1 = flurm_controller_connector:cancel_timer(Ref),
    ?assert(is_integer(Result1)),
    Result2 = flurm_controller_connector:cancel_timer(Ref),
    ?assertEqual(false, Result2).

%%====================================================================
%% detect_features/0 Tests
%%====================================================================

detect_features_test_() ->
    {"detect_features/0 coverage tests", [
        fun detect_features_returns_list_test/0,
        fun detect_features_all_binaries_test/0,
        fun detect_features_deterministic_test/0
    ]}.

detect_features_returns_list_test() ->
    Features = flurm_controller_connector:detect_features(),
    ?assert(is_list(Features)).

detect_features_all_binaries_test() ->
    Features = flurm_controller_connector:detect_features(),
    ?assert(lists:all(fun is_binary/1, Features)).

detect_features_deterministic_test() ->
    F1 = flurm_controller_connector:detect_features(),
    F2 = flurm_controller_connector:detect_features(),
    ?assertEqual(F1, F2).

%%====================================================================
%% check_cpu_flag/1 Tests
%%====================================================================

check_cpu_flag_test_() ->
    {"check_cpu_flag/1 coverage tests", [
        fun check_cpu_flag_returns_boolean_test/0,
        fun check_cpu_flag_common_flags_test/0,
        fun check_cpu_flag_nonexistent_test/0
    ]}.

check_cpu_flag_returns_boolean_test() ->
    Result = flurm_controller_connector:check_cpu_flag("fpu"),
    ?assert(is_boolean(Result)).

check_cpu_flag_common_flags_test() ->
    %% These may or may not be present, but function should return boolean
    lists:foreach(fun(Flag) ->
        R = flurm_controller_connector:check_cpu_flag(Flag),
        ?assert(is_boolean(R))
    end, ["fpu", "sse", "sse2", "avx", "avx2"]).

check_cpu_flag_nonexistent_test() ->
    Result = flurm_controller_connector:check_cpu_flag("definitely_not_a_real_cpu_flag_xyz"),
    ?assertEqual(false, Result).
