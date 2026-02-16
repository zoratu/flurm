%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Acceptor Coverage Tests
%%%
%%% Comprehensive coverage tests for flurm_controller_acceptor module.
%%% Tests all exported -ifdef(TEST) helper functions for maximum coverage.
%%%
%%% These tests focus on:
%%% - Binary to hex conversion
%%% - Hex prefix extraction
%%% - Initial state creation
%%% - Buffer status checking
%%% - Message extraction
%%% - Duration calculation
%%% - MUNGE authentication helpers
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% binary_to_hex Tests
%%====================================================================

binary_to_hex_test_() ->
    [
        {"empty binary", fun() ->
            ?assertEqual(<<>>, flurm_controller_acceptor:binary_to_hex(<<>>))
        end},
        {"single byte zero", fun() ->
            ?assertEqual(<<"00">>, flurm_controller_acceptor:binary_to_hex(<<0>>))
        end},
        {"single byte one", fun() ->
            ?assertEqual(<<"01">>, flurm_controller_acceptor:binary_to_hex(<<1>>))
        end},
        {"single byte mid", fun() ->
            ?assertEqual(<<"80">>, flurm_controller_acceptor:binary_to_hex(<<128>>))
        end},
        {"single byte max", fun() ->
            ?assertEqual(<<"FF">>, flurm_controller_acceptor:binary_to_hex(<<255>>))
        end},
        {"two bytes", fun() ->
            ?assertEqual(<<"0102">>, flurm_controller_acceptor:binary_to_hex(<<1, 2>>))
        end},
        {"multiple bytes ascending", fun() ->
            ?assertEqual(<<"0102030405">>, flurm_controller_acceptor:binary_to_hex(<<1,2,3,4,5>>))
        end},
        {"DEADBEEF pattern", fun() ->
            ?assertEqual(<<"DEADBEEF">>, flurm_controller_acceptor:binary_to_hex(<<222,173,190,239>>))
        end},
        {"CAFEBABE pattern", fun() ->
            ?assertEqual(<<"CAFEBABE">>, flurm_controller_acceptor:binary_to_hex(<<202,254,186,190>>))
        end},
        {"all zeros", fun() ->
            ?assertEqual(<<"00000000">>, flurm_controller_acceptor:binary_to_hex(<<0,0,0,0>>))
        end},
        {"all ones", fun() ->
            ?assertEqual(<<"FFFFFFFF">>, flurm_controller_acceptor:binary_to_hex(<<255,255,255,255>>))
        end},
        {"SLURM header example", fun() ->
            Header = <<16#00, 16#26, 16#00, 16#00, 16#03, 16#F0, 16#00, 16#00, 16#00, 16#10>>,
            ?assertEqual(<<"0026000003F000000010">>, flurm_controller_acceptor:binary_to_hex(Header))
        end},
        {"mixed high low nibbles", fun() ->
            ?assertEqual(<<"0F10F0">>, flurm_controller_acceptor:binary_to_hex(<<15, 16, 240>>))
        end}
    ].

%%====================================================================
%% get_hex_prefix Tests
%%====================================================================

get_hex_prefix_test_() ->
    [
        {"empty data", fun() ->
            ?assertEqual(<<>>, flurm_controller_acceptor:get_hex_prefix(<<>>))
        end},
        {"single byte", fun() ->
            ?assertEqual(<<"AB">>, flurm_controller_acceptor:get_hex_prefix(<<171>>))
        end},
        {"short data - 5 bytes", fun() ->
            Data = <<1, 2, 3, 4, 5>>,
            ?assertEqual(<<"0102030405">>, flurm_controller_acceptor:get_hex_prefix(Data))
        end},
        {"exactly 23 bytes", fun() ->
            Data = binary:copy(<<16#AB>>, 23),
            Expected = binary:copy(<<"AB">>, 23),
            ?assertEqual(Expected, flurm_controller_acceptor:get_hex_prefix(Data))
        end},
        {"exactly 24 bytes", fun() ->
            Data = binary:copy(<<16#CD>>, 24),
            Expected = binary:copy(<<"CD">>, 24),
            ?assertEqual(Expected, flurm_controller_acceptor:get_hex_prefix(Data))
        end},
        {"25 bytes - truncated to 24", fun() ->
            Data = <<(binary:copy(<<16#EF>>, 24))/binary, 16#00>>,
            Expected = binary:copy(<<"EF">>, 24),
            ?assertEqual(Expected, flurm_controller_acceptor:get_hex_prefix(Data))
        end},
        {"50 bytes - truncated to 24", fun() ->
            Data = binary:copy(<<16#AA>>, 50),
            Expected = binary:copy(<<"AA">>, 24),
            ?assertEqual(Expected, flurm_controller_acceptor:get_hex_prefix(Data))
        end},
        {"100 bytes - truncated", fun() ->
            Data = binary:copy(<<16#BB>>, 100),
            Expected = binary:copy(<<"BB">>, 24),
            ?assertEqual(Expected, flurm_controller_acceptor:get_hex_prefix(Data))
        end}
    ].

%%====================================================================
%% create_initial_state Tests
%%====================================================================

create_initial_state_test_() ->
    [
        {"creates map with all required fields", fun() ->
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
            ?assert(maps:is_key(socket, State)),
            ?assert(maps:is_key(transport, State)),
            ?assert(maps:is_key(opts, State)),
            ?assert(maps:is_key(buffer, State)),
            ?assert(maps:is_key(request_count, State)),
            ?assert(maps:is_key(start_time, State))
        end},
        {"buffer is empty", fun() ->
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
            ?assertEqual(<<>>, maps:get(buffer, State))
        end},
        {"request_count is zero", fun() ->
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
            ?assertEqual(0, maps:get(request_count, State))
        end},
        {"start_time is set to current time", fun() ->
            Before = erlang:system_time(millisecond),
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
            After = erlang:system_time(millisecond),
            StartTime = maps:get(start_time, State),
            ?assert(StartTime >= Before),
            ?assert(StartTime =< After)
        end},
        {"socket is stored correctly", fun() ->
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
            ?assertEqual(Socket, maps:get(socket, State))
        end},
        {"transport is stored correctly", fun() ->
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_ssl, #{}),
            ?assertEqual(ranch_ssl, maps:get(transport, State))
        end},
        {"opts are stored correctly", fun() ->
            Socket = make_ref(),
            Opts = #{key => value, another => 123},
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, Opts),
            ?assertEqual(Opts, maps:get(opts, State))
        end}
    ].

%%====================================================================
%% check_buffer_status Tests
%%====================================================================

check_buffer_status_test_() ->
    [
        {"empty buffer", fun() ->
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(<<>>))
        end},
        {"1 byte", fun() ->
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(<<1>>))
        end},
        {"2 bytes", fun() ->
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(<<1, 2>>))
        end},
        {"3 bytes", fun() ->
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(<<1, 2, 3>>))
        end},
        {"4 bytes - length only", fun() ->
            Buffer = <<100:32/big>>,
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"length too small - 0", fun() ->
            Buffer = <<0:32/big, 0:80>>,
            ?assertEqual(invalid_length, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"length too small - 5", fun() ->
            Buffer = <<5:32/big, 0:40>>,
            ?assertEqual(invalid_length, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"length too small - 9", fun() ->
            Buffer = <<9:32/big, 0:72>>,
            ?assertEqual(invalid_length, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"incomplete message", fun() ->
            %% Length says 100, only 10 bytes of data
            Buffer = <<100:32/big, 0:80>>,
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"complete - minimum size (10)", fun() ->
            Length = ?SLURM_HEADER_SIZE,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"complete - with body", fun() ->
            Length = ?SLURM_HEADER_SIZE + 20,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"complete - extra data after message", fun() ->
            %% Length must be >= SLURM_HEADER_SIZE (16) to be valid
            Length = ?SLURM_HEADER_SIZE + 5,
            Buffer = <<Length:32/big, 0:(Length*8), "extra data">>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"complete - larger message", fun() ->
            %% Length must be >= SLURM_HEADER_SIZE (16) to be valid
            Length = 1000,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end}
    ].

%%====================================================================
%% extract_message Tests
%%====================================================================

extract_message_test_() ->
    [
        {"empty buffer", fun() ->
            ?assertEqual({incomplete, <<>>}, flurm_controller_acceptor:extract_message(<<>>))
        end},
        {"partial length prefix", fun() ->
            Buffer = <<1, 2, 3>>,
            ?assertMatch({incomplete, _}, flurm_controller_acceptor:extract_message(Buffer))
        end},
        {"incomplete message", fun() ->
            Buffer = <<100:32/big, 0:80>>,
            ?assertMatch({incomplete, _}, flurm_controller_acceptor:extract_message(Buffer))
        end},
        {"complete message - exact", fun() ->
            Length = 15,
            MessageData = binary:copy(<<$A>>, Length),
            Buffer = <<Length:32/big, MessageData/binary>>,
            {ok, FullMsg, Remaining} = flurm_controller_acceptor:extract_message(Buffer),
            ?assertEqual(<<Length:32/big, MessageData/binary>>, FullMsg),
            ?assertEqual(<<>>, Remaining)
        end},
        {"complete message - with remainder", fun() ->
            Length = 10,
            MessageData = binary:copy(<<$B>>, Length),
            ExtraData = <<"extra">>,
            Buffer = <<Length:32/big, MessageData/binary, ExtraData/binary>>,
            {ok, FullMsg, Remaining} = flurm_controller_acceptor:extract_message(Buffer),
            ?assertEqual(<<Length:32/big, MessageData/binary>>, FullMsg),
            ?assertEqual(ExtraData, Remaining)
        end},
        {"message includes length prefix", fun() ->
            Length = 20,
            MessageData = binary:copy(<<$X>>, Length),
            Buffer = <<Length:32/big, MessageData/binary>>,
            {ok, FullMsg, _} = flurm_controller_acceptor:extract_message(Buffer),
            <<ExtractedLen:32/big, ExtractedData/binary>> = FullMsg,
            ?assertEqual(Length, ExtractedLen),
            ?assertEqual(MessageData, ExtractedData)
        end},
        {"minimum valid message", fun() ->
            Length = ?SLURM_HEADER_SIZE,
            MessageData = binary:copy(<<0>>, Length),
            Buffer = <<Length:32/big, MessageData/binary>>,
            {ok, FullMsg, Remaining} = flurm_controller_acceptor:extract_message(Buffer),
            ?assertEqual(4 + Length, byte_size(FullMsg)),
            ?assertEqual(<<>>, Remaining)
        end}
    ].

%%====================================================================
%% calculate_duration Tests
%%====================================================================

calculate_duration_test_() ->
    [
        {"zero duration", fun() ->
            Now = 1000,
            ?assertEqual(0, flurm_controller_acceptor:calculate_duration(Now, Now))
        end},
        {"positive duration - 1 second", fun() ->
            ?assertEqual(1000, flurm_controller_acceptor:calculate_duration(2000, 1000))
        end},
        {"positive duration - 1 minute", fun() ->
            ?assertEqual(60000, flurm_controller_acceptor:calculate_duration(70000, 10000))
        end},
        {"positive duration - 1 hour", fun() ->
            ?assertEqual(3600000, flurm_controller_acceptor:calculate_duration(3600000, 0))
        end},
        {"large duration", fun() ->
            %% Simulate 24 hours
            ?assertEqual(86400000, flurm_controller_acceptor:calculate_duration(86400000, 0))
        end},
        {"real timestamps", fun() ->
            StartTime = erlang:system_time(millisecond) - 500,
            EndTime = erlang:system_time(millisecond),
            Duration = flurm_controller_acceptor:calculate_duration(EndTime, StartTime),
            ?assert(Duration >= 500),
            ?assert(Duration < 1000)
        end}
    ].

%%====================================================================
%% get_munge_auth_mode Tests
%%====================================================================

get_munge_auth_mode_test_() ->
    {setup,
     fun() ->
         %% Save original value
         OrigVal = application:get_env(flurm_controller, munge_auth_enabled),
         OrigVal
     end,
     fun(OrigVal) ->
         %% Restore original value
         case OrigVal of
             undefined ->
                 application:unset_env(flurm_controller, munge_auth_enabled);
             {ok, V} ->
                 application:set_env(flurm_controller, munge_auth_enabled, V)
         end
     end,
     [
        {"default mode", fun() ->
            application:unset_env(flurm_controller, munge_auth_enabled),
            Result = flurm_controller_acceptor:get_munge_auth_mode(),
            ?assert(Result =:= true orelse Result =:= false orelse Result =:= strict)
        end},
        {"disabled mode", fun() ->
            application:set_env(flurm_controller, munge_auth_enabled, false),
            ?assertEqual(false, flurm_controller_acceptor:get_munge_auth_mode())
        end},
        {"permissive mode", fun() ->
            application:set_env(flurm_controller, munge_auth_enabled, true),
            ?assertEqual(true, flurm_controller_acceptor:get_munge_auth_mode())
        end},
        {"strict mode", fun() ->
            application:set_env(flurm_controller, munge_auth_enabled, strict),
            ?assertEqual(strict, flurm_controller_acceptor:get_munge_auth_mode())
        end}
     ]}.

%%====================================================================
%% verify_munge_credential Tests
%%====================================================================

verify_munge_credential_test_() ->
    {setup,
     fun() ->
         %% Set to disabled for predictable tests
         application:set_env(flurm_controller, munge_auth_enabled, false)
     end,
     fun(_) ->
         application:unset_env(flurm_controller, munge_auth_enabled)
     end,
     [
        {"disabled mode returns disabled", fun() ->
            Result = flurm_controller_acceptor:verify_munge_credential(#{}),
            ?assertEqual({ok, disabled}, Result)
        end},
        {"empty auth info", fun() ->
            Result = flurm_controller_acceptor:verify_munge_credential(#{}),
            ?assertMatch({ok, _}, Result)
        end},
        {"non-munge auth type", fun() ->
            AuthInfo = #{auth_type => none},
            Result = flurm_controller_acceptor:verify_munge_credential(AuthInfo),
            ?assertMatch({ok, _}, Result)
        end}
     ]}.

verify_munge_credential_permissive_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_controller, munge_auth_enabled, true)
     end,
     fun(_) ->
         application:unset_env(flurm_controller, munge_auth_enabled)
     end,
     [
        {"empty auth info in permissive mode", fun() ->
            Result = flurm_controller_acceptor:verify_munge_credential(#{}),
            ?assertMatch({ok, _}, Result)
        end},
        {"munge type without credential", fun() ->
            AuthInfo = #{auth_type => munge},
            Result = flurm_controller_acceptor:verify_munge_credential(AuthInfo),
            ?assertMatch({ok, _}, Result)
        end},
        {"munge type with undefined credential", fun() ->
            AuthInfo = #{auth_type => munge, credential => undefined},
            Result = flurm_controller_acceptor:verify_munge_credential(AuthInfo),
            ?assertMatch({ok, _}, Result)
        end},
        {"munge type with empty credential", fun() ->
            AuthInfo = #{auth_type => munge, credential => <<>>},
            Result = flurm_controller_acceptor:verify_munge_credential(AuthInfo),
            ?assertMatch({ok, _}, Result)
        end}
     ]}.

%%====================================================================
%% Integration and Edge Case Tests
%%====================================================================

integration_test_() ->
    [
        {"buffer check then extract flow", fun() ->
            Length = ?SLURM_HEADER_SIZE + 10,
            MessageData = binary:copy(<<$M>>, Length),
            Buffer = <<Length:32/big, MessageData/binary>>,

            case flurm_controller_acceptor:check_buffer_status(Buffer) of
                {complete, ExtractLen} ->
                    ?assertEqual(Length, ExtractLen),
                    {ok, FullMsg, <<>>} = flurm_controller_acceptor:extract_message(Buffer),
                    ?assertEqual(4 + Length, byte_size(FullMsg));
                _ ->
                    ?assert(false)
            end
        end},
        {"multiple message extraction", fun() ->
            Len1 = 10,
            Len2 = 15,
            Msg1Data = binary:copy(<<$1>>, Len1),
            Msg2Data = binary:copy(<<$2>>, Len2),
            Buffer = <<Len1:32/big, Msg1Data/binary, Len2:32/big, Msg2Data/binary>>,

            {ok, FullMsg1, Rest1} = flurm_controller_acceptor:extract_message(Buffer),
            ?assertEqual(<<Len1:32/big, Msg1Data/binary>>, FullMsg1),

            {ok, FullMsg2, Rest2} = flurm_controller_acceptor:extract_message(Rest1),
            ?assertEqual(<<Len2:32/big, Msg2Data/binary>>, FullMsg2),
            ?assertEqual(<<>>, Rest2)
        end},
        {"state creation and duration", fun() ->
            Socket = make_ref(),
            State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
            StartTime = maps:get(start_time, State),
            timer:sleep(10),
            EndTime = erlang:system_time(millisecond),
            Duration = flurm_controller_acceptor:calculate_duration(EndTime, StartTime),
            ?assert(Duration >= 10)
        end},
        {"hex prefix for logging", fun() ->
            IncomingData = <<16#00, 16#26, 16#00, 16#00, 16#03, 16#F0,
                            16#00, 16#00, 16#00, 16#10, 16#AA, 16#BB>>,
            HexPrefix = flurm_controller_acceptor:get_hex_prefix(IncomingData),
            ?assertEqual(<<"0026000003F000000010AABB">>, HexPrefix)
        end}
    ].

edge_cases_test_() ->
    [
        {"exactly 4 bytes (just length)", fun() ->
            Buffer = <<100:32/big>>,
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"max uint32 length incomplete", fun() ->
            Buffer = <<16#FFFFFFFF:32/big, 0, 1, 2>>,
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"binary with null bytes", fun() ->
            Data = <<0, 0, 0, 0, 0>>,
            Hex = flurm_controller_acceptor:binary_to_hex(Data),
            ?assertEqual(<<"0000000000">>, Hex)
        end},
        {"SLURM protocol version hex", fun() ->
            VersionBin = <<?SLURM_PROTOCOL_VERSION:16/big>>,
            Hex = flurm_controller_acceptor:binary_to_hex(VersionBin),
            ?assertEqual(<<"2600">>, Hex)
        end}
    ].

%%====================================================================
%% SLURM Protocol Specific Tests
%%====================================================================

slurm_protocol_test_() ->
    [
        {"valid SLURM header size", fun() ->
            Length = ?SLURM_HEADER_SIZE,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual({complete, ?SLURM_HEADER_SIZE},
                        flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"SLURM PING message buffer", fun() ->
            %% SLURM_HEADER_SIZE is 16 bytes
            %% Build a proper header that's at least 16 bytes
            Version = ?SLURM_PROTOCOL_VERSION,
            Flags = 0,
            MsgType = ?REQUEST_PING,
            BodyLen = 0,
            %% Full 16-byte header: version(2) + flags(2) + msg_type(2) + body_len(4) + padding(6)
            Header = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLen:32/big, 0:48>>,
            Length = byte_size(Header),
            Buffer = <<Length:32/big, Header/binary>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"SLURM JOB_INFO message buffer", fun() ->
            Version = ?SLURM_PROTOCOL_VERSION,
            Flags = 0,
            MsgType = ?REQUEST_JOB_INFO,
            BodyLen = 4,  % job_id
            %% Full 16-byte header: version(2) + flags(2) + msg_type(2) + body_len(4) + padding(6)
            Header = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLen:32/big, 0:48>>,
            Body = <<123:32/big>>,
            Length = byte_size(Header) + byte_size(Body),
            Buffer = <<Length:32/big, Header/binary, Body/binary>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end}
    ].

%%====================================================================
%% Boundary Tests
%%====================================================================

boundary_test_() ->
    [
        {"buffer exactly at header size boundary", fun() ->
            %% Length = header size exactly
            Length = ?SLURM_HEADER_SIZE,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"buffer one byte short of complete", fun() ->
            Length = 20,
            %% Only 19 bytes of data
            Buffer = <<Length:32/big, 0:152>>,
            ?assertEqual(need_more_data, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"buffer one byte over complete", fun() ->
            %% Length must be >= SLURM_HEADER_SIZE (16) to be valid
            Length = ?SLURM_HEADER_SIZE + 4,
            Buffer = <<Length:32/big, 0:(Length*8), 16#FF>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"length at boundary of invalid", fun() ->
            %% Length = SLURM_HEADER_SIZE - 1 (15) is invalid
            Length = ?SLURM_HEADER_SIZE - 1,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual(invalid_length, flurm_controller_acceptor:check_buffer_status(Buffer))
        end},
        {"length at boundary of valid", fun() ->
            %% Length = SLURM_HEADER_SIZE (16) is valid
            Length = ?SLURM_HEADER_SIZE,
            Buffer = <<Length:32/big, 0:(Length*8)>>,
            ?assertEqual({complete, Length}, flurm_controller_acceptor:check_buffer_status(Buffer))
        end}
    ].
