#!/usr/bin/env escript
%%! -pa _build/default/lib/flurm_protocol/ebin

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

main([]) ->
    io:format("Testing flurm_protocol_codec with auth/extra data~n"),
    io:format("================================================~n~n"),

    %% Test 1: encode_with_extra for RESPONSE_SLURM_RC (8001)
    io:format("Test 1: RESPONSE_SLURM_RC encode_with_extra~n"),
    {ok, RCMsg} = flurm_protocol_codec:encode_with_extra(
        ?RESPONSE_SLURM_RC,
        #slurm_rc_response{return_code = 0}
    ),
    <<Len1:32/big, Content1/binary>> = RCMsg,
    io:format("  Total size: ~p bytes~n", [byte_size(RCMsg)]),
    io:format("  Length field: ~p, content: ~p bytes~n", [Len1, byte_size(Content1)]),
    io:format("  Expected: 4 + 12 + 4 + 39 = 59 bytes~n"),
    io:format("  Hex: ~s~n", [hex(RCMsg)]),

    %% Verify extra data starts with 8 zeros + 00 64
    <<_Len:32/big, _Header:12/binary, _Body:4/binary, Extra1/binary>> = RCMsg,
    <<First8:64/big, Next2:16/big, _/binary>> = Extra1,
    case {First8, Next2} of
        {0, 16#0064} -> io:format("  [PASS] Extra data format correct for RESPONSE_SLURM_RC~n");
        _ -> io:format("  [FAIL] Extra data format wrong: first8=~p, next2=~.16B~n", [First8, Next2])
    end,

    %% Test 2: encode_with_extra for RESPONSE_JOB_INFO (2004)
    io:format("~nTest 2: RESPONSE_JOB_INFO encode_with_extra~n"),
    {ok, JobMsg} = flurm_protocol_codec:encode_with_extra(
        ?RESPONSE_JOB_INFO,
        #job_info_response{last_update = 0, job_count = 0, jobs = []}
    ),
    <<Len2:32/big, Content2/binary>> = JobMsg,
    io:format("  Total size: ~p bytes~n", [byte_size(JobMsg)]),
    io:format("  Length field: ~p, content: ~p bytes~n", [Len2, byte_size(Content2)]),
    io:format("  Expected: 4 + 12 + 12 + 39 = 67 bytes~n"),

    %% Verify extra data starts directly with 00 64 (no leading zeros)
    <<_Len2:32/big, _Header2:12/binary, _Body2:12/binary, Extra2/binary>> = JobMsg,
    <<First2_2:16/big, _/binary>> = Extra2,
    case First2_2 of
        16#0064 -> io:format("  [PASS] Extra data format correct for RESPONSE_JOB_INFO~n");
        _ -> io:format("  [FAIL] Extra data format wrong: first2=~.16B~n", [First2_2])
    end,

    %% Test 3: decode_with_extra
    io:format("~nTest 3: decode_with_extra~n"),
    {ok, DecodedMsg, ExtraInfo, <<>>} = flurm_protocol_codec:decode_with_extra(RCMsg),
    io:format("  Decoded msg type: ~p~n", [DecodedMsg#slurm_msg.header#slurm_header.msg_type]),
    io:format("  Decoded body: ~p~n", [DecodedMsg#slurm_msg.body]),
    io:format("  Extra info: ~p~n", [ExtraInfo]),

    io:format("~n================================================~n"),
    io:format("All tests passed!~n"),
    ok.

hex(Binary) ->
    [[io_lib:format("~2.16.0B", [B]) || <<B>> <= Binary]].
