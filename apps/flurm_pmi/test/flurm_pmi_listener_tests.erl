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
    %% Stop any existing manager
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,
    {ok, MgrPid} = flurm_pmi_manager:start_link(),
    unlink(MgrPid),
    MgrPid.

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
    %% Stop manager
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
    Path = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
    {ok, Socket} = gen_tcp:connect({local, Path}, 0, [
        binary,
        {packet, line},
        {active, false}
    ], 5000),
    Socket.

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
