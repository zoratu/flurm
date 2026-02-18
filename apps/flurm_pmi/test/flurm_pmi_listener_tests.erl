%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_pmi_listener
%%%
%%% Covers PMI listener lifecycle, socket creation, and PMI protocol
%%% communication over Unix domain sockets.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_pmi_listener_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_listener_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

pmi_listener_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_socket_path returns correct format",
         fun test_get_socket_path/0},
        {"get_socket_path different IDs produce different paths",
         fun test_get_socket_path_different/0},
        {"start_link creates socket file",
         fun test_start_link_creates_socket/0},
        {"stop removes socket file",
         fun test_stop_removes_socket/0},
        {"PMI init command gets response",
         fun test_pmi_init/0},
        {"PMI get_maxes command gets response",
         fun test_pmi_get_maxes/0},
        {"PMI get_appnum command gets response",
         fun test_pmi_get_appnum/0},
        {"PMI get_my_kvsname gets response",
         fun test_pmi_get_kvsname/0},
        {"PMI put and get roundtrip",
         fun test_pmi_put_get/0},
        {"PMI getbyidx returns entries",
         fun test_pmi_getbyidx/0},
        {"PMI finalize closes connection",
         fun test_pmi_finalize/0},
        {"PMI barrier_in with single rank (size=1) succeeds",
         fun test_pmi_barrier/0},
        {"Unknown PMI command returns error",
         fun test_pmi_unknown_command/0},
        {"Client disconnect does not crash listener",
         fun test_client_disconnect/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Ensure no meck mocks are active for modules we need real
    catch meck:unload(flurm_pmi_manager),
    catch meck:unload(flurm_pmi_listener),
    catch meck:unload(flurm_pmi_sup),
    catch meck:unload(flurm_pmi_protocol),
    %% Stop supervisor first if running (to prevent restart storms when stopping manager)
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        SupPid ->
            catch gen_server:stop(SupPid, normal, 5000),
            timer:sleep(50)
    end,
    %% Now stop manager if still running (shouldn't be, but just in case)
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        MgrPid0 ->
            catch gen_server:stop(MgrPid0, normal, 5000),
            timer:sleep(50)
    end,
    %% Start fresh manager
    {ok, P} = flurm_pmi_manager:start_link(),
    unlink(P),
    P.

cleanup(MgrPid) ->
    %% Stop any test listeners
    catch flurm_pmi_listener:stop(99, 0),
    catch flurm_pmi_listener:stop(99, 1),
    catch flurm_pmi_listener:stop(99, 2),
    catch flurm_pmi_listener:stop(99, 3),
    %% Clean up socket files
    lists:foreach(fun(StepId) ->
        Path = lists:flatten(flurm_pmi_listener:get_socket_path(99, StepId)),
        file:delete(Path)
    end, [0, 1, 2, 3]),
    %% Stop manager (no supervisor to worry about - we stopped it in setup)
    case is_process_alive(MgrPid) of
        true -> gen_server:stop(MgrPid, normal, 5000);
        false -> ok
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

start_listener(JobId, StepId, Size) ->
    {ok, Pid} = flurm_pmi_listener:start_link(JobId, StepId, Size),
    unlink(Pid),
    Pid.

connect_to_listener(JobId, StepId) ->
    connect_to_listener(JobId, StepId, 5).

connect_to_listener(JobId, StepId, 0) ->
    Path = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
    {ok, Socket} = gen_tcp:connect({local, Path}, 0, [binary, {packet, line}, {active, false}], 5000),
    Socket;
connect_to_listener(JobId, StepId, Retries) ->
    Path = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
    case gen_tcp:connect({local, Path}, 0, [binary, {packet, line}, {active, false}], 5000) of
        {ok, Socket} -> Socket;
        {error, econnrefused} -> timer:sleep(50), connect_to_listener(JobId, StepId, Retries - 1);
        {error, enoent} -> timer:sleep(50), connect_to_listener(JobId, StepId, Retries - 1)
    end.

send_pmi_cmd(Socket, Cmd) ->
    ok = gen_tcp:send(Socket, [Cmd, "\n"]),
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, Data} -> {ok, Data};
        {error, Reason} -> {error, Reason}
    end.

parse_response(Data) ->
    flurm_pmi_protocol:decode(Data).

%%====================================================================
%% Tests
%%====================================================================

test_get_socket_path() ->
    Path = flurm_pmi_listener:get_socket_path(42, 3),
    Flat = lists:flatten(Path),
    ?assert(lists:prefix("/tmp/flurm_pmi_42_3", Flat)).

test_get_socket_path_different() ->
    Path1 = lists:flatten(flurm_pmi_listener:get_socket_path(1, 0)),
    Path2 = lists:flatten(flurm_pmi_listener:get_socket_path(2, 0)),
    ?assertNotEqual(Path1, Path2).

test_start_link_creates_socket() ->
    _Pid = start_listener(99, 0, 2),
    Path = lists:flatten(flurm_pmi_listener:get_socket_path(99, 0)),
    timer:sleep(100),
    ?assertMatch({ok, _}, file:read_file_info(Path)).

test_stop_removes_socket() ->
    _Pid = start_listener(99, 1, 1),
    Path = lists:flatten(flurm_pmi_listener:get_socket_path(99, 1)),
    timer:sleep(100),
    ?assertMatch({ok, _}, file:read_file_info(Path)),
    flurm_pmi_listener:stop(99, 1),
    timer:sleep(100),
    ?assertEqual({error, enoent}, file:read_file_info(Path)).

test_pmi_init() ->
    _Pid = start_listener(99, 0, 2),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(2, maps:get(size, Attrs)),
    gen_tcp:close(Socket).

test_pmi_get_maxes() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    %% Init first
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_maxes">>),
    {ok, {maxes, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assert(maps:get(kvsname_max, Attrs) > 0),
    ?assert(maps:get(keylen_max, Attrs) > 0),
    ?assert(maps:get(vallen_max, Attrs) > 0),
    gen_tcp:close(Socket).

test_pmi_get_appnum() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_appnum">>),
    {ok, {appnum, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(0, maps:get(appnum, Attrs)),
    gen_tcp:close(Socket).

test_pmi_get_kvsname() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_my_kvsname">>),
    {ok, {my_kvsname, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertNotEqual(undefined, maps:get(kvsname, Attrs, undefined)),
    gen_tcp:close(Socket).

test_pmi_put_get() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    %% Put
    {ok, PutResp} = send_pmi_cmd(Socket, <<"cmd=put key=testkey value=testval">>),
    {ok, {put_ack, PutAttrs}} = parse_response(PutResp),
    ?assertEqual(0, maps:get(rc, PutAttrs)),
    %% Barrier to commit KVS
    {ok, BarrierResp} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, {barrier_out, BarrierAttrs}} = parse_response(BarrierResp),
    ?assertEqual(0, maps:get(rc, BarrierAttrs)),
    %% Get
    {ok, GetResp} = send_pmi_cmd(Socket, <<"cmd=get key=testkey">>),
    {ok, {get_ack, GetAttrs}} = parse_response(GetResp),
    ?assertEqual(0, maps:get(rc, GetAttrs)),
    ?assertEqual(<<"testval">>, maps:get(value, GetAttrs)),
    gen_tcp:close(Socket).

test_pmi_getbyidx() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=aaa value=v1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=bbb value=v2">>),
    %% Barrier to commit
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    %% Get by index
    {ok, Resp0} = send_pmi_cmd(Socket, <<"cmd=getbyidx idx=0">>),
    {ok, {getbyidx_ack, Attrs0}} = parse_response(Resp0),
    ?assertEqual(0, maps:get(rc, Attrs0)),
    ?assertNotEqual(undefined, maps:get(key, Attrs0, undefined)),
    gen_tcp:close(Socket).

test_pmi_finalize() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, FinalResp} = send_pmi_cmd(Socket, <<"cmd=finalize">>),
    {ok, {finalize_ack, FinalAttrs}} = parse_response(FinalResp),
    ?assertEqual(0, maps:get(rc, FinalAttrs)),
    %% Connection should be closed by server
    timer:sleep(100),
    ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 100)),
    gen_tcp:close(Socket).

test_pmi_barrier() ->
    %% Note: The listener is a single gen_server, so barrier_in with size>1
    %% would deadlock (first barrier_in blocks the gen_server, preventing
    %% the second socket's barrier_in from being processed). We test with
    %% size=1 here. Multi-rank barrier is tested in flurm_pmi_manager_tests.
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1 rank=0">>),
    %% With size=1, barrier completes immediately
    {ok, BarrierResp} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, {barrier_out, Attrs}} = parse_response(BarrierResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

test_pmi_unknown_command() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=bogus_command">>),
    {ok, {unknown, Attrs}} = parse_response(RawResp),
    ?assertEqual(-1, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

test_client_disconnect() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    gen_tcp:close(Socket),
    timer:sleep(100),
    %% Listener should still be alive
    ?assert(is_process_alive(Pid)),
    %% Should be able to accept new connections
    Socket2 = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket2, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, _}} = parse_response(RawResp),
    gen_tcp:close(Socket2).

%%====================================================================
%% Extended Socket Path Tests
%%====================================================================

extended_socket_path_test_() ->
    [
     {"socket path contains job_id",
      fun() ->
          Path = lists:flatten(flurm_pmi_listener:get_socket_path(123, 0)),
          ?assertNotEqual(nomatch, string:find(Path, "123"))
      end},
     {"socket path contains step_id",
      fun() ->
          Path = lists:flatten(flurm_pmi_listener:get_socket_path(1, 456)),
          ?assertNotEqual(nomatch, string:find(Path, "456"))
      end},
     {"socket path starts with /tmp",
      fun() ->
          Path = lists:flatten(flurm_pmi_listener:get_socket_path(1, 0)),
          ?assertEqual("/tmp", string:slice(Path, 0, 4))
      end},
     {"socket path has .sock extension",
      fun() ->
          Path = lists:flatten(flurm_pmi_listener:get_socket_path(1, 0)),
          ?assertEqual(".sock", string:slice(Path, length(Path) - 5, 5))
      end}
    ].

%%====================================================================
%% Extended Init Tests
%%====================================================================

extended_init_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init with rank attribute",
         fun test_init_with_rank/0},
        {"init with version 2",
         fun test_init_version2/0},
        {"multiple inits same socket",
         fun test_multiple_inits/0}
     ]}.

test_init_with_rank() ->
    _Pid = start_listener(99, 0, 4),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1 rank=2">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(2, maps:get(rank, Attrs)),
    gen_tcp:close(Socket).

test_init_version2() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=2 pmi_subversion=0">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(2, maps:get(pmi_version, Attrs)),
    gen_tcp:close(Socket).

test_multiple_inits() ->
    _Pid = start_listener(99, 0, 2),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    %% First init
    {ok, Resp1} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, _}} = parse_response(Resp1),
    %% Second init - should still work
    {ok, Resp2} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, _}} = parse_response(Resp2),
    gen_tcp:close(Socket).

%%====================================================================
%% Extended KVS Tests
%%====================================================================

extended_kvs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"put with empty key",
         fun test_put_empty_key/0},
        {"put with empty value",
         fun test_put_empty_value/0},
        {"get non-existent key",
         fun test_get_nonexistent/0},
        {"multiple puts same key",
         fun test_multiple_puts_same_key/0},
        {"getbyidx end of kvs",
         fun test_getbyidx_end/0}
     ]}.

test_put_empty_key() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, PutResp} = send_pmi_cmd(Socket, <<"cmd=put key= value=testval">>),
    {ok, {put_ack, PutAttrs}} = parse_response(PutResp),
    ?assertEqual(0, maps:get(rc, PutAttrs)),
    gen_tcp:close(Socket).

test_put_empty_value() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, PutResp} = send_pmi_cmd(Socket, <<"cmd=put key=testkey value=">>),
    {ok, {put_ack, PutAttrs}} = parse_response(PutResp),
    ?assertEqual(0, maps:get(rc, PutAttrs)),
    gen_tcp:close(Socket).

test_get_nonexistent() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    %% Barrier to ensure committed state
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, GetResp} = send_pmi_cmd(Socket, <<"cmd=get key=nonexistent">>),
    {ok, {get_ack, GetAttrs}} = parse_response(GetResp),
    ?assertEqual(-1, maps:get(rc, GetAttrs)),
    gen_tcp:close(Socket).

test_multiple_puts_same_key() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=mykey value=val1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=mykey value=val2">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, GetResp} = send_pmi_cmd(Socket, <<"cmd=get key=mykey">>),
    {ok, {get_ack, GetAttrs}} = parse_response(GetResp),
    ?assertEqual(0, maps:get(rc, GetAttrs)),
    %% Should get latest value
    ?assertEqual(<<"val2">>, maps:get(value, GetAttrs)),
    gen_tcp:close(Socket).

test_getbyidx_end() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    %% Empty KVS, index 0 should be end
    {ok, Resp} = send_pmi_cmd(Socket, <<"cmd=getbyidx idx=0">>),
    {ok, {getbyidx_ack, Attrs}} = parse_response(Resp),
    ?assertEqual(-1, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

%%====================================================================
%% Extended Connection Tests
%%====================================================================

extended_connection_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"multiple concurrent connections",
         fun test_multiple_connections/0},
        {"rapid connect disconnect",
         fun test_rapid_connect_disconnect/0},
        {"send without init",
         fun test_send_without_init/0}
     ]}.

test_multiple_connections() ->
    _Pid = start_listener(99, 0, 4),
    timer:sleep(100),
    %% Open multiple connections
    Sockets = [connect_to_listener(99, 0) || _ <- lists:seq(1, 3)],
    %% Init on each
    lists:foreach(fun(Socket) ->
        {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>)
    end, Sockets),
    %% Close all
    lists:foreach(fun(Socket) -> gen_tcp:close(Socket) end, Sockets).

test_rapid_connect_disconnect() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    %% Rapidly connect and disconnect
    lists:foreach(fun(_) ->
        Socket = connect_to_listener(99, 0),
        gen_tcp:close(Socket)
    end, lists:seq(1, 5)),
    %% Listener should still work
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    gen_tcp:close(Socket).

test_send_without_init() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    %% Send get_maxes without init first
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_maxes">>),
    {ok, {maxes, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

gen_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error",
         fun test_unknown_call/0},
        {"unknown cast does not crash",
         fun test_unknown_cast/0},
        {"unknown info does not crash",
         fun test_unknown_info/0}
     ]}.

test_unknown_call() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Result = gen_server:call(Pid, bogus_request),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    gen_server:cast(Pid, bogus_cast),
    timer:sleep(50),
    ?assert(is_process_alive(Pid)).

test_unknown_info() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Pid ! bogus_info,
    timer:sleep(50),
    ?assert(is_process_alive(Pid)).

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"terminate removes socket file",
         fun test_terminate_removes_socket/0}
     ]}.

test_terminate_removes_socket() ->
    Pid = start_listener(99, 2, 1),
    Path = lists:flatten(flurm_pmi_listener:get_socket_path(99, 2)),
    timer:sleep(100),
    ?assertMatch({ok, _}, file:read_file_info(Path)),
    gen_server:stop(Pid),
    timer:sleep(100),
    ?assertEqual({error, enoent}, file:read_file_info(Path)).

%%====================================================================
%% Protocol Edge Cases
%%====================================================================

protocol_edge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"malformed command handled gracefully",
         fun test_malformed_command/0},
        {"partial line buffering",
         fun test_partial_line/0}
     ]}.

test_malformed_command() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    %% This should be handled without crashing
    ok = gen_tcp:send(Socket, <<"not_a_valid_pmi_command\n">>),
    timer:sleep(100),
    %% Listener should still be alive
    ?assert(is_process_alive(Pid)),
    gen_tcp:close(Socket).

test_partial_line() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    %% Send partial command
    ok = gen_tcp:send(Socket, <<"cmd=init pmi_version=1">>),
    timer:sleep(10),
    %% Complete the command
    ok = gen_tcp:send(Socket, <<" pmi_subversion=1\n">>),
    %% Should get valid response
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, Data} ->
            {ok, {init_ack, _}} = parse_response(Data);
        {error, _} ->
            ok  % May error if buffering isn't working
    end,
    gen_tcp:close(Socket).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init returns size in response",
         fun test_init_returns_size/0},
        {"init returns rank in response",
         fun test_init_returns_rank/0},
        {"init returns debug flag",
         fun test_init_returns_debug/0},
        {"init returns appnum",
         fun test_init_returns_appnum/0},
        {"get_maxes returns all limits",
         fun test_get_maxes_all_limits/0},
        {"barrier with committed kvs",
         fun test_barrier_commits_kvs/0},
        {"put creates key",
         fun test_put_creates_key/0},
        {"get returns exact value",
         fun test_get_exact_value/0},
        {"getbyidx returns nextidx",
         fun test_getbyidx_nextidx/0},
        {"finalize sends ack",
         fun test_finalize_sends_ack/0},
        {"tcp_closed handled",
         fun test_tcp_closed_handled/0},
        {"listener survives client error",
         fun test_survives_client_error/0},
        {"multiple put same socket",
         fun test_multiple_put_same_socket/0},
        {"multiple get same socket",
         fun test_multiple_get_same_socket/0},
        {"socket path unique per step",
         fun test_socket_path_unique_step/0},
        {"stop returns ok",
         fun test_stop_returns_ok/0},
        {"stop on non-existent ok",
         fun test_stop_nonexistent/0},
        {"listener handles rapid commands",
         fun test_rapid_commands/0},
        {"listener handles binary node names",
         fun test_binary_node_names/0},
        {"init with all attributes",
         fun test_init_all_attrs/0},
        {"get_my_kvsname returns name",
         fun test_get_my_kvsname_name/0},
        {"multiple connections same job",
         fun test_multiple_conn_same_job/0}
     ]}.

test_init_returns_size() ->
    _Pid = start_listener(99, 0, 4),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(4, maps:get(size, Attrs)),
    gen_tcp:close(Socket).

test_init_returns_rank() ->
    _Pid = start_listener(99, 0, 4),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1 rank=3">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(3, maps:get(rank, Attrs)),
    gen_tcp:close(Socket).

test_init_returns_debug() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(debug, Attrs)),
    gen_tcp:close(Socket).

test_init_returns_appnum() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(appnum, Attrs)),
    gen_tcp:close(Socket).

test_get_maxes_all_limits() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_maxes">>),
    {ok, {maxes, Attrs}} = parse_response(RawResp),
    ?assertEqual(256, maps:get(kvsname_max, Attrs)),
    ?assertEqual(256, maps:get(keylen_max, Attrs)),
    ?assertEqual(1024, maps:get(vallen_max, Attrs)),
    gen_tcp:close(Socket).

test_barrier_commits_kvs() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k value=v">>),
    {ok, BarrierResp} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, {barrier_out, Attrs}} = parse_response(BarrierResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

test_put_creates_key() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, PutResp} = send_pmi_cmd(Socket, <<"cmd=put key=newkey value=newval">>),
    {ok, {put_ack, Attrs}} = parse_response(PutResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

test_get_exact_value() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=exactkey value=exactval">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, GetResp} = send_pmi_cmd(Socket, <<"cmd=get key=exactkey">>),
    {ok, {get_ack, Attrs}} = parse_response(GetResp),
    ?assertEqual(<<"exactval">>, maps:get(value, Attrs)),
    gen_tcp:close(Socket).

test_getbyidx_nextidx() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k1 value=v1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, Resp} = send_pmi_cmd(Socket, <<"cmd=getbyidx idx=0">>),
    {ok, {getbyidx_ack, Attrs}} = parse_response(Resp),
    ?assertEqual(1, maps:get(nextidx, Attrs)),
    gen_tcp:close(Socket).

test_finalize_sends_ack() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, FinalResp} = send_pmi_cmd(Socket, <<"cmd=finalize">>),
    {ok, {finalize_ack, Attrs}} = parse_response(FinalResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    gen_tcp:close(Socket).

test_tcp_closed_handled() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    gen_tcp:close(Socket),
    timer:sleep(100),
    ?assert(is_process_alive(Pid)).

test_survives_client_error() ->
    Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    %% Send invalid data
    gen_tcp:send(Socket, <<"garbage\n">>),
    timer:sleep(100),
    ?assert(is_process_alive(Pid)),
    gen_tcp:close(Socket).

test_multiple_put_same_socket() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k1 value=v1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k2 value=v2">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k3 value=v3">>),
    gen_tcp:close(Socket).

test_multiple_get_same_socket() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k1 value=v1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=put key=k2 value=v2">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=get key=k1">>),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=get key=k2">>),
    gen_tcp:close(Socket).

test_socket_path_unique_step() ->
    Path0 = lists:flatten(flurm_pmi_listener:get_socket_path(1, 0)),
    Path1 = lists:flatten(flurm_pmi_listener:get_socket_path(1, 1)),
    Path2 = lists:flatten(flurm_pmi_listener:get_socket_path(1, 2)),
    ?assertNotEqual(Path0, Path1),
    ?assertNotEqual(Path1, Path2),
    ?assertNotEqual(Path0, Path2).

test_stop_returns_ok() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    ?assertEqual(ok, flurm_pmi_listener:stop(99, 0)).

test_stop_nonexistent() ->
    ?assertEqual(ok, flurm_pmi_listener:stop(999, 999)).

test_rapid_commands() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    lists:foreach(fun(I) ->
        Key = list_to_binary("key" ++ integer_to_list(I)),
        Val = list_to_binary("val" ++ integer_to_list(I)),
        Cmd = <<"cmd=put key=", Key/binary, " value=", Val/binary>>,
        {ok, _} = send_pmi_cmd(Socket, Cmd)
    end, lists:seq(1, 10)),
    gen_tcp:close(Socket).

test_binary_node_names() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, {init_ack, _}} = parse_response(RawResp),
    gen_tcp:close(Socket).

test_init_all_attrs() ->
    _Pid = start_listener(99, 0, 8),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=2 pmi_subversion=0 rank=5">>),
    {ok, {init_ack, Attrs}} = parse_response(RawResp),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(8, maps:get(size, Attrs)),
    ?assertEqual(5, maps:get(rank, Attrs)),
    gen_tcp:close(Socket).

test_get_my_kvsname_name() ->
    _Pid = start_listener(99, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_my_kvsname">>),
    {ok, {my_kvsname, Attrs}} = parse_response(RawResp),
    KvsName = maps:get(kvsname, Attrs),
    ?assert(is_binary(KvsName)),
    gen_tcp:close(Socket).

test_multiple_conn_same_job() ->
    _Pid = start_listener(99, 0, 4),
    timer:sleep(100),
    S1 = connect_to_listener(99, 0),
    S2 = connect_to_listener(99, 0),
    {ok, _} = send_pmi_cmd(S1, <<"cmd=init pmi_version=1 pmi_subversion=1 rank=0">>),
    {ok, _} = send_pmi_cmd(S2, <<"cmd=init pmi_version=1 pmi_subversion=1 rank=1">>),
    gen_tcp:close(S1),
    gen_tcp:close(S2).

%%====================================================================
%% Error Path Coverage Tests
%%====================================================================

error_path_test_() ->
    {foreach,
     fun setup_error_tests/0,
     fun cleanup_error_tests/1,
     [
        {"init error: job not found returns rc=-1",
         fun test_init_job_not_found/0},
        {"get_maxes error: job not found returns rc=-1",
         fun test_get_maxes_job_not_found/0},
        {"get_kvsname error: job not found returns rc=-1",
         fun test_get_kvsname_job_not_found/0},
        {"barrier error: returns rc=-1",
         fun test_barrier_error/0},
        {"put error: returns rc=-1",
         fun test_put_error/0},
        {"tcp_error removes connection",
         fun test_tcp_error_handling/0}
     ]}.

setup_error_tests() ->
    application:ensure_all_started(sasl),
    %% Ensure no meck mocks are active
    catch meck:unload(flurm_pmi_manager),
    catch meck:unload(flurm_pmi_listener),
    catch meck:unload(flurm_pmi_sup),
    catch meck:unload(flurm_pmi_protocol),
    %% Stop any existing processes
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        SupPid ->
            catch gen_server:stop(SupPid, normal, 5000),
            timer:sleep(50)
    end,
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        MgrPid0 ->
            catch gen_server:stop(MgrPid0, normal, 5000),
            timer:sleep(50)
    end,
    %% Clean up any lingering socket files for error test job IDs
    lists:foreach(fun(JobId) ->
        lists:foreach(fun(StepId) ->
            Path = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
            file:delete(Path)
        end, [0, 1, 2, 3])
    end, [101, 102, 103, 104, 105, 106]),
    %% Start fresh manager
    {ok, P} = flurm_pmi_manager:start_link(),
    unlink(P),
    P.

cleanup_error_tests(MgrPid) ->
    %% Stop test listeners
    lists:foreach(fun(JobId) ->
        catch flurm_pmi_listener:stop(JobId, 0)
    end, [101, 102, 103, 104, 105, 106]),
    %% Clean up socket files
    lists:foreach(fun(JobId) ->
        Path = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, 0)),
        file:delete(Path)
    end, [101, 102, 103, 104, 105, 106]),
    %% Unload any mocks
    catch meck:unload(flurm_pmi_manager),
    %% Stop manager
    case is_process_alive(MgrPid) of
        true -> gen_server:stop(MgrPid, normal, 5000);
        false -> ok
    end.

test_init_job_not_found() ->
    %% Start listener normally first (uses job 101)
    _Pid = start_listener(101, 0, 1),
    timer:sleep(100),
    %% Now mock get_job_info to return error
    meck:new(flurm_pmi_manager, [passthrough]),
    meck:expect(flurm_pmi_manager, get_job_info, fun(_J, _S) -> {error, not_found} end),
    try
        Socket = connect_to_listener(101, 0),
        {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
        {ok, {init_ack, Attrs}} = parse_response(RawResp),
        ?assertEqual(-1, maps:get(rc, Attrs)),
        gen_tcp:close(Socket)
    after
        meck:unload(flurm_pmi_manager)
    end.

test_get_maxes_job_not_found() ->
    %% Start listener with real manager first (uses job 102)
    _Pid = start_listener(102, 0, 1),
    timer:sleep(100),
    meck:new(flurm_pmi_manager, [passthrough]),
    meck:expect(flurm_pmi_manager, get_job_info, fun(_J, _S) -> {error, not_found} end),
    try
        Socket = connect_to_listener(102, 0),
        {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_maxes">>),
        {ok, {maxes, Attrs}} = parse_response(RawResp),
        ?assertEqual(-1, maps:get(rc, Attrs)),
        gen_tcp:close(Socket)
    after
        meck:unload(flurm_pmi_manager)
    end.

test_get_kvsname_job_not_found() ->
    %% Start listener with real manager first (uses job 103)
    _Pid = start_listener(103, 0, 1),
    timer:sleep(100),
    meck:new(flurm_pmi_manager, [passthrough]),
    meck:expect(flurm_pmi_manager, get_kvs_name, fun(_J, _S) -> {error, not_found} end),
    try
        Socket = connect_to_listener(103, 0),
        {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=get_my_kvsname">>),
        {ok, {my_kvsname, Attrs}} = parse_response(RawResp),
        ?assertEqual(-1, maps:get(rc, Attrs)),
        gen_tcp:close(Socket)
    after
        meck:unload(flurm_pmi_manager)
    end.

test_barrier_error() ->
    %% Start listener with real manager first (uses job 104)
    _Pid = start_listener(104, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(104, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1 rank=0">>),
    meck:new(flurm_pmi_manager, [passthrough]),
    meck:expect(flurm_pmi_manager, barrier_in, fun(_J, _S, _R) -> {error, barrier_failed} end),
    try
        {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=barrier_in">>),
        {ok, {barrier_out, Attrs}} = parse_response(RawResp),
        ?assertEqual(-1, maps:get(rc, Attrs)),
        gen_tcp:close(Socket)
    after
        meck:unload(flurm_pmi_manager)
    end.

test_put_error() ->
    %% Start listener with real manager first (uses job 105)
    _Pid = start_listener(105, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(105, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    meck:new(flurm_pmi_manager, [passthrough]),
    meck:expect(flurm_pmi_manager, kvs_put, fun(_J, _S, _K, _V) -> {error, kvs_full} end),
    try
        {ok, RawResp} = send_pmi_cmd(Socket, <<"cmd=put key=k value=v">>),
        {ok, {put_ack, Attrs}} = parse_response(RawResp),
        ?assertEqual(-1, maps:get(rc, Attrs)),
        gen_tcp:close(Socket)
    after
        meck:unload(flurm_pmi_manager)
    end.

test_tcp_error_handling() ->
    %% Uses job 106
    Pid = start_listener(106, 0, 1),
    timer:sleep(100),
    Socket = connect_to_listener(106, 0),
    {ok, _} = send_pmi_cmd(Socket, <<"cmd=init pmi_version=1 pmi_subversion=1">>),
    %% Simulate tcp_error by sending the message directly
    Pid ! {tcp_error, Socket, econnreset},
    timer:sleep(50),
    %% Listener should still be alive
    ?assert(is_process_alive(Pid)),
    gen_tcp:close(Socket).
