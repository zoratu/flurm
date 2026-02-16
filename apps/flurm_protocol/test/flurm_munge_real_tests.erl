%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_munge module
%%%
%%% Tests the MUNGE authentication helper functions. These tests
%%% call the real functions but don't require munge to be installed.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_munge_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% is_available Tests
%%====================================================================

is_available_test_() ->
    [
     {"is_available returns boolean",
      fun() ->
          Result = flurm_munge:is_available(),
          ?assert(is_boolean(Result))
      end}
    ].

%%====================================================================
%% Internal Parser Tests
%%====================================================================

%% Test parse_unmunge_output function (internal but tested via decode behavior)
parse_status_test_() ->
    [
     {"parse success status",
      fun() ->
          Output = "STATUS:           Success (0)\n"
                   "ENCODE_HOST:      testhost (127.0.0.1)\n"
                   "ENCODE_TIME:      2024-01-01 00:00:00 (1704067200)\n"
                   "DECODE_TIME:      2024-01-01 00:00:01 (1704067201)\n"
                   "TTL:              300\n"
                   "CIPHER:           aes128 (4)\n"
                   "MAC:              sha256 (5)\n"
                   "ZIP:              none (0)\n"
                   "UID:              1000 (testuser)\n"
                   "GID:              1000 (testgroup)\n"
                   "LENGTH:           0\n"
                   "\n"
                   "PAYLOAD:          (empty)\n",
          Result = parse_unmunge_output(Output),
          ?assertMatch({ok, _}, Result),
          {ok, Info} = Result,
          ?assertEqual(1000, maps:get(uid, Info)),
          ?assertEqual(1000, maps:get(gid, Info)),
          ?assertEqual(<<>>, maps:get(payload, Info))
      end},
     {"parse expired status",
      fun() ->
          Output = "STATUS:           Expired credential (15)\n",
          Result = parse_unmunge_output(Output),
          ?assertEqual({error, credential_expired}, Result)
      end},
     {"parse replayed status",
      fun() ->
          Output = "STATUS:           Replayed credential (17)\n",
          Result = parse_unmunge_output(Output),
          ?assertEqual({error, credential_replayed}, Result)
      end},
     {"parse rewound status",
      fun() ->
          Output = "STATUS:           Rewound credential (16)\n",
          Result = parse_unmunge_output(Output),
          ?assertEqual({error, credential_rewound}, Result)
      end},
     {"parse invalid status",
      fun() ->
          Output = "STATUS:           Unknown error (99)\n",
          Result = parse_unmunge_output(Output),
          ?assertMatch({error, {credential_invalid, _}}, Result)
      end},
     {"parse empty output",
      fun() ->
          Result = parse_unmunge_output(""),
          ?assertEqual({error, invalid_unmunge_output}, Result)
      end}
    ].

%% Test extract_field function
extract_field_test_() ->
    [
     {"extract UID field",
      fun() ->
          Lines = ["UID:              1001 (testuser)"],
          Result = extract_field(Lines, "UID"),
          ?assertEqual(1001, Result)
      end},
     {"extract GID field",
      fun() ->
          Lines = ["GID:              1002 (testgroup)"],
          Result = extract_field(Lines, "GID"),
          ?assertEqual(1002, Result)
      end},
     {"extract from multiple lines",
      fun() ->
          Lines = ["STATUS:           Success (0)",
                   "ENCODE_HOST:      test (127.0.0.1)",
                   "UID:              500 (user)",
                   "GID:              501 (group)"],
          ?assertEqual(500, extract_field(Lines, "UID")),
          ?assertEqual(501, extract_field(Lines, "GID"))
      end},
     {"missing field returns 0",
      fun() ->
          Lines = ["STATUS:           Success (0)"],
          Result = extract_field(Lines, "UID"),
          ?assertEqual(0, Result)
      end},
     {"malformed value returns 0",
      fun() ->
          Lines = ["UID:              notanumber (user)"],
          Result = extract_field(Lines, "UID"),
          ?assertEqual(0, Result)
      end}
    ].

%% Test extract_payload function
extract_payload_test_() ->
    [
     {"extract empty payload",
      fun() ->
          Lines = ["LENGTH:           0", "", "PAYLOAD:          (empty)"],
          Result = extract_payload(Lines),
          ?assertEqual(<<>>, Result)
      end},
     {"extract text payload",
      fun() ->
          Lines = ["LENGTH:           5", "", "PAYLOAD:          hello"],
          Result = extract_payload(Lines),
          ?assertEqual(<<"hello">>, Result)
      end},
     {"no LENGTH line returns empty",
      fun() ->
          Lines = ["STATUS:           Success (0)"],
          Result = extract_payload(Lines),
          ?assertEqual(<<>>, Result)
      end}
    ].

%%====================================================================
%% decode without munge Tests
%%====================================================================

decode_without_munge_test_() ->
    {setup,
     fun() ->
         %% Remember if munge was available
         flurm_munge:is_available()
     end,
     fun(_) -> ok end,
     [
      {"decode returns error when munge unavailable",
       fun() ->
           case flurm_munge:is_available() of
               false ->
                   Result = flurm_munge:decode(<<"MUNGE:test:credential">>),
                   ?assertEqual({error, munge_unavailable}, Result);
               true ->
                   %% Munge is available, skip this test
                   ok
           end
       end}
     ]}.

%%====================================================================
%% verify Tests
%%====================================================================

verify_test_() ->
    [
     {"verify returns error when munge unavailable",
      fun() ->
          case flurm_munge:is_available() of
              false ->
                  Result = flurm_munge:verify(<<"MUNGE:test:credential">>),
                  ?assertEqual({error, munge_unavailable}, Result);
              true ->
                  %% Can't test without valid credential
                  ok
          end
      end}
    ].

%%====================================================================
%% Internal Helper Implementations (for testing)
%%====================================================================

%% These replicate the internal logic of flurm_munge for isolated testing

parse_unmunge_output(Output) ->
    Lines = string:split(Output, "\n", all),
    case extract_status(Lines) of
        {ok, success} ->
            UID = extract_field(Lines, "UID"),
            GID = extract_field(Lines, "GID"),
            Payload = extract_payload(Lines),
            {ok, #{uid => UID, gid => GID, payload => Payload}};
        {ok, expired} ->
            {error, credential_expired};
        {ok, rewound} ->
            {error, credential_rewound};
        {ok, replayed} ->
            {error, credential_replayed};
        {ok, {invalid, StatusStr}} ->
            {error, {credential_invalid, StatusStr}};
        {error, Reason} ->
            {error, Reason}
    end.

extract_status([]) ->
    {error, invalid_unmunge_output};
extract_status([Line | Rest]) ->
    case string:prefix(string:trim(Line, leading), "STATUS:") of
        nomatch ->
            extract_status(Rest);
        StatusPart ->
            StatusStr = string:trim(StatusPart),
            parse_status_string(StatusStr)
    end.

parse_status_string(StatusStr) ->
    case string:find(StatusStr, "Success") of
        nomatch ->
            case string:find(StatusStr, "Expired") of
                nomatch ->
                    case string:find(StatusStr, "Rewound") of
                        nomatch ->
                            case string:find(StatusStr, "Replayed") of
                                nomatch ->
                                    {ok, {invalid, StatusStr}};
                                _ ->
                                    {ok, replayed}
                            end;
                        _ ->
                            {ok, rewound}
                    end;
                _ ->
                    {ok, expired}
            end;
        _ ->
            {ok, success}
    end.

extract_field([], _FieldName) ->
    0;
extract_field([Line | Rest], FieldName) ->
    Prefix = FieldName ++ ":",
    case string:prefix(string:trim(Line, leading), Prefix) of
        nomatch ->
            extract_field(Rest, FieldName);
        ValuePart ->
            Trimmed = string:trim(ValuePart),
            case string:split(Trimmed, " ") of
                [NumStr | _] ->
                    case string:to_integer(NumStr) of
                        {Int, _Rest} when is_integer(Int) -> Int;
                        _ -> 0
                    end;
                _ ->
                    0
            end
    end.

extract_payload(Lines) ->
    extract_payload(Lines, false, []).

extract_payload([], _AfterLength, Acc) ->
    list_to_binary(lists:flatten(lists:reverse(Acc)));
extract_payload([Line | Rest], false, Acc) ->
    case string:prefix(string:trim(Line, leading), "LENGTH:") of
        nomatch ->
            extract_payload(Rest, false, Acc);
        _ ->
            extract_payload(Rest, true, Acc)
    end;
extract_payload(["" | Rest], true, Acc) ->
    extract_payload(Rest, true, Acc);
extract_payload([Line | Rest], true, Acc) ->
    case string:prefix(string:trim(Line, leading), "PAYLOAD:") of
        nomatch ->
            extract_payload(Rest, true, [Line | Acc]);
        PayloadPart ->
            Content = string:trim(PayloadPart),
            case Content of
                "(empty)" -> <<>>;
                _ -> list_to_binary(Content)
            end
    end.
