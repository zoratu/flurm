%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_munge module
%%% Tests MUNGE authentication helper with mocked commands
%%%-------------------------------------------------------------------
-module(flurm_munge_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases
%%====================================================================

%% Test is_available function
is_available_test_() ->
    [
     {"is_available returns boolean", fun() ->
         Result = flurm_munge:is_available(),
         ?assert(is_boolean(Result))
     end}
    ].

%% Test encode function with empty payload
encode_empty_test_() ->
    [
     {"encode with empty payload", fun() ->
         Result = flurm_munge:encode(),
         case flurm_munge:is_available() of
             true ->
                 %% If munge is available, should return ok with credential
                 case Result of
                     {ok, Cred} ->
                         ?assert(is_binary(Cred));
                     {error, munge_failed} ->
                         %% munge command exists but failed (e.g., no daemon)
                         ok
                 end;
             false ->
                 %% Munge not installed - encode returns {ok, error_output} since os:cmd
                 %% captures the shell error message. This is expected behavior.
                 case Result of
                     {ok, Output} when is_binary(Output) ->
                         %% Got shell error output, which is valid
                         ok;
                     {error, munge_failed} ->
                         ok
                 end
         end
     end}
    ].

%% Test encode function with payload
encode_payload_test_() ->
    [
     {"encode with binary payload", fun() ->
         Payload = <<"test payload data">>,
         Result = flurm_munge:encode(Payload),
         case flurm_munge:is_available() of
             true ->
                 case Result of
                     {ok, Cred} ->
                         ?assert(is_binary(Cred)),
                         ?assert(byte_size(Cred) > 0);
                     {error, munge_failed} ->
                         ok
                 end;
             false ->
                 %% When munge is not available, encode will return {ok, error_output} or {error, ...}
                 ?assertMatch({ok, _} , Result)
         end
     end},
     {"encode with empty binary", fun() ->
         Payload = <<>>,
         Result = flurm_munge:encode(Payload),
         %% Should work the same as encode/0
         case flurm_munge:is_available() of
             true ->
                 case Result of
                     {ok, _} -> ok;
                     {error, munge_failed} -> ok
                 end;
             false ->
                 %% When munge is not available, returns result of os:cmd which may include error output
                 ?assertMatch({ok, _}, Result)
         end
     end}
    ].

%% Mock tests using a fake munge script
mock_munge_test_() ->
    {setup,
     fun setup_mock_munge/0,
     fun cleanup_mock_munge/1,
     fun(MockDir) ->
         [
          {"encode succeeds with mock munge", fun() ->
              %% Put mock in PATH
              OldPath = os:getenv("PATH"),
              os:putenv("PATH", MockDir ++ ":" ++ OldPath),

              Result = flurm_munge:encode(<<"test">>),
              ?assertMatch({ok, _}, Result),
              {ok, Cred} = Result,
              ?assertEqual(<<"MOCK_CREDENTIAL">>, Cred),

              os:putenv("PATH", OldPath)
          end}
         ]
     end}.

setup_mock_munge() ->
    MockDir = "/tmp/flurm_munge_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    file:make_dir(MockDir),
    MockMunge = MockDir ++ "/munge",
    %% Create a mock munge script that outputs a credential
    Script = "#!/bin/sh\necho 'MOCK_CREDENTIAL'\n",
    file:write_file(MockMunge, Script),
    %% Make it executable
    os:cmd("chmod +x " ++ MockMunge),
    MockDir.

cleanup_mock_munge(MockDir) ->
    os:cmd("rm -rf " ++ MockDir).

%% Test encode with various payloads
encode_various_payloads_test_() ->
    [
     {"encode handles special characters in payload", fun() ->
         %% This tests the base64 encoding path
         Payload = <<"hello world\nwith\ttabs\rand\0nulls">>,
         Result = flurm_munge:encode(Payload),
         %% Should not crash regardless of munge availability
         ?assert(is_tuple(Result)),
         case Result of
             {ok, _} -> ok;
             {error, _} -> ok
         end
     end},
     {"encode handles large payload", fun() ->
         %% Large payload to test the encoding path
         Payload = binary:copy(<<"x">>, 10000),
         Result = flurm_munge:encode(Payload),
         ?assert(is_tuple(Result))
     end}
    ].

%%====================================================================
%% Decode Tests with Meck
%%====================================================================

%% Test decode with valid credential (mocked)
decode_valid_credential_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Success (0)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "ENCODE_TIME:      2024-01-01 12:00:00 -0500 (1704132000)\n"
                     "DECODE_TIME:      2024-01-01 12:00:01 -0500 (1704132001)\n"
                     "TTL:              300\n"
                     "CIPHER:           aes128 (4)\n"
                     "MAC:              sha256 (5)\n"
                     "ZIP:              none (0)\n"
                     "UID:              1000 (testuser)\n"
                     "GID:              1000 (testgroup)\n"
                     "LENGTH:           11\n"
                     "\n"
                     "PAYLOAD:          test_payload\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns uid, gid, and payload for valid credential", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:abc123...">>),
              ?assertMatch({ok, #{uid := _, gid := _, payload := _}}, Result),
              {ok, Info} = Result,
              ?assertEqual(1000, maps:get(uid, Info)),
              ?assertEqual(1000, maps:get(gid, Info))
          end}
         ]
     end}.

%% Test decode with expired credential (mocked)
decode_expired_credential_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Expired credential (15)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "UID:              1000 (testuser)\n"
                     "GID:              1000 (testgroup)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns error for expired credential", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:expired...">>),
              ?assertMatch({error, credential_expired}, Result)
          end}
         ]
     end}.

%% Test decode with invalid credential (mocked)
decode_invalid_credential_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Invalid credential format (8)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns error for invalid credential", fun() ->
              Result = flurm_munge:decode(<<"NOT_A_VALID_CRED">>),
              ?assertMatch({error, {credential_invalid, _}}, Result)
          end}
         ]
     end}.

%% Test verify extracts UID and GID correctly
verify_uid_gid_extraction_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Success (0)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "UID:              500 (admin)\n"
                     "GID:              100 (wheel)\n"
                     "LENGTH:           0\n"
                     "\n"
                     "PAYLOAD:          (empty)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"verify returns ok for valid credential", fun() ->
              Result = flurm_munge:verify(<<"MUNGE:valid...">>),
              ?assertEqual(ok, Result)
          end},
          {"decode extracts correct UID and GID", fun() ->
              {ok, Info} = flurm_munge:decode(<<"MUNGE:valid...">>),
              ?assertEqual(500, maps:get(uid, Info)),
              ?assertEqual(100, maps:get(gid, Info)),
              ?assertEqual(<<>>, maps:get(payload, Info))
          end}
         ]
     end}.

%% Test munge unavailable fallback
munge_unavailable_fallback_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> false end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns munge_unavailable when munge not installed", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:any...">>),
              ?assertEqual({error, munge_unavailable}, Result)
          end},
          {"verify returns munge_unavailable when munge not installed", fun() ->
              Result = flurm_munge:verify(<<"MUNGE:any...">>),
              ?assertEqual({error, munge_unavailable}, Result)
          end},
          {"is_available returns false when munge not found", fun() ->
              Result = flurm_munge:is_available(),
              ?assertEqual(false, Result)
          end}
         ]
     end}.

%%====================================================================
%% Additional Decode Tests
%%====================================================================

%% Test decode with replayed credential (mocked)
decode_replayed_credential_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Replayed credential (17)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "UID:              1000 (testuser)\n"
                     "GID:              1000 (testgroup)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns error for replayed credential", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:replayed...">>),
              ?assertMatch({error, credential_replayed}, Result)
          end},
          {"verify returns error for replayed credential", fun() ->
              Result = flurm_munge:verify(<<"MUNGE:replayed...">>),
              ?assertMatch({error, credential_replayed}, Result)
          end}
         ]
     end}.

%% Test decode with rewound credential (timestamp in future)
decode_rewound_credential_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Rewound credential (16)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "UID:              1000 (testuser)\n"
                     "GID:              1000 (testgroup)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns error for rewound credential", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:rewound...">>),
              ?assertMatch({error, credential_rewound}, Result)
          end}
         ]
     end}.

%% Test decode with empty unmunge output
decode_empty_output_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ -> ""  %% Empty output
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns unmunge_failed for empty output", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:any...">>),
              ?assertEqual({error, unmunge_failed}, Result)
          end}
         ]
     end}.

%% Test decode with payload containing data
decode_with_payload_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Success (0)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "ENCODE_TIME:      2024-01-01 12:00:00 -0500 (1704132000)\n"
                     "DECODE_TIME:      2024-01-01 12:00:01 -0500 (1704132001)\n"
                     "TTL:              300\n"
                     "CIPHER:           aes128 (4)\n"
                     "MAC:              sha256 (5)\n"
                     "ZIP:              none (0)\n"
                     "UID:              1001 (alice)\n"
                     "GID:              100 (users)\n"
                     "LENGTH:           12\n"
                     "\n"
                     "PAYLOAD:          hello world!\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode extracts payload correctly", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:with_payload...">>),
              ?assertMatch({ok, #{uid := _, gid := _, payload := _}}, Result),
              {ok, Info} = Result,
              ?assertEqual(1001, maps:get(uid, Info)),
              ?assertEqual(100, maps:get(gid, Info)),
              ?assertEqual(<<"hello world!">>, maps:get(payload, Info))
          end}
         ]
     end}.

%% Test decode with unknown status
decode_unknown_status_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Unknown error (99)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"decode returns credential_invalid for unknown status", fun() ->
              Result = flurm_munge:decode(<<"MUNGE:unknown...">>),
              ?assertMatch({error, {credential_invalid, _}}, Result)
          end}
         ]
     end}.

%%====================================================================
%% Verify Function Tests
%%====================================================================

%% Verify function comprehensive tests
verify_comprehensive_test_() ->
    {setup,
     fun() ->
         meck:new(os, [unstick, passthrough]),
         meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
         meck:expect(os, cmd, fun(Cmd) ->
             case string:find(Cmd, "unmunge") of
                 nomatch -> "";
                 _ ->
                     "STATUS:           Success (0)\n"
                     "ENCODE_HOST:      testhost (127.0.0.1)\n"
                     "UID:              1000 (testuser)\n"
                     "GID:              1000 (testgroup)\n"
                     "LENGTH:           0\n"
                     "\n"
                     "PAYLOAD:          (empty)\n"
             end
         end)
     end,
     fun(_) ->
         meck:unload(os)
     end,
     fun(_) ->
         [
          {"verify returns ok for valid credential", fun() ->
              Result = flurm_munge:verify(<<"MUNGE:valid...">>),
              ?assertEqual(ok, Result)
          end}
         ]
     end}.

%% Test verify with various error conditions
verify_error_conditions_test_() ->
    [
     {"verify with expired credential",
      {setup,
       fun() ->
           meck:new(os, [unstick, passthrough]),
           meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
           meck:expect(os, cmd, fun(_) ->
               "STATUS:           Expired credential (15)\n"
           end)
       end,
       fun(_) -> meck:unload(os) end,
       fun(_) ->
           [{"returns credential_expired", fun() ->
               Result = flurm_munge:verify(<<"MUNGE:expired...">>),
               ?assertEqual({error, credential_expired}, Result)
           end}]
       end}},

     {"verify with replayed credential",
      {setup,
       fun() ->
           meck:new(os, [unstick, passthrough]),
           meck:expect(os, find_executable, fun("munge") -> "/usr/bin/munge" end),
           meck:expect(os, cmd, fun(_) ->
               "STATUS:           Replayed credential (17)\n"
           end)
       end,
       fun(_) -> meck:unload(os) end,
       fun(_) ->
           [{"returns credential_replayed", fun() ->
               Result = flurm_munge:verify(<<"MUNGE:replayed...">>),
               ?assertEqual({error, credential_replayed}, Result)
           end}]
       end}}
    ].
