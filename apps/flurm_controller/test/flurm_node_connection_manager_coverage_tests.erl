%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_node_connection_manager
%%%
%%% Tests the node connection manager gen_server directly. The actual
%%% gen_server is started and tested. External dependencies like
%%% flurm_node_manager_server are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_connection_manager_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_node_connection_manager_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link starts server", fun test_start_link/0},
      {"register_connection adds connection", fun test_register_connection/0},
      {"register_connection replaces existing", fun test_register_connection_replace/0},
      {"unregister_connection removes connection", fun test_unregister_connection/0},
      {"unregister_connection non-existent ok", fun test_unregister_connection_nonexistent/0},
      {"get_connection returns pid", fun test_get_connection_found/0},
      {"get_connection returns error when not connected", fun test_get_connection_not_found/0},
      {"find_by_socket returns hostname", fun test_find_by_socket/0},
      {"find_by_socket returns error", fun test_find_by_socket_not_found/0},
      {"send_to_node sends message", fun test_send_to_node_success/0},
      {"send_to_node returns error when not connected", fun test_send_to_node_not_connected/0},
      {"send_to_nodes sends to multiple", fun test_send_to_nodes/0},
      {"list_connected_nodes returns all", fun test_list_connected_nodes/0},
      {"is_node_connected returns true", fun test_is_node_connected_true/0},
      {"is_node_connected returns false", fun test_is_node_connected_false/0},
      {"handle_info DOWN removes connection", fun test_handle_info_down/0},
      {"handle_info unknown message ignored", fun test_handle_info_unknown/0},
      {"handle_call unknown request", fun test_handle_call_unknown/0},
      {"handle_cast unknown message", fun test_handle_cast_unknown/0},
      {"terminate callback", fun test_terminate/0}
     ]}.

setup() ->
    case whereis(flurm_node_connection_manager) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
    end,
    meck:new(flurm_node_manager_server, [non_strict]),
    meck:expect(flurm_node_manager_server, update_node, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    case whereis(flurm_node_connection_manager) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
    end,
    catch meck:unload(flurm_node_manager_server),
    ok.

%%====================================================================
%% Lifecycle Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_node_connection_manager:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

%%====================================================================
%% register_connection Tests
%%====================================================================

test_register_connection() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid = spawn(fun() -> receive stop -> ok end end),

    Result = flurm_node_connection_manager:register_connection(<<"node001">>, Pid),

    ?assertEqual(ok, Result),
    {ok, ReturnedPid} = flurm_node_connection_manager:get_connection(<<"node001">>),
    ?assertEqual(Pid, ReturnedPid),
    Pid ! stop.

test_register_connection_replace() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid1 = spawn(fun() -> receive stop -> ok end end),
    Pid2 = spawn(fun() -> receive stop -> ok end end),

    flurm_node_connection_manager:register_connection(<<"node001">>, Pid1),
    flurm_node_connection_manager:register_connection(<<"node001">>, Pid2),

    {ok, ReturnedPid} = flurm_node_connection_manager:get_connection(<<"node001">>),
    ?assertEqual(Pid2, ReturnedPid),
    Pid1 ! stop,
    Pid2 ! stop.

%%====================================================================
%% unregister_connection Tests
%%====================================================================

test_unregister_connection() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid = spawn(fun() -> receive stop -> ok end end),

    flurm_node_connection_manager:register_connection(<<"node001">>, Pid),
    flurm_node_connection_manager:unregister_connection(<<"node001">>),
    timer:sleep(20),

    Result = flurm_node_connection_manager:get_connection(<<"node001">>),
    ?assertEqual({error, not_connected}, Result),
    Pid ! stop.

test_unregister_connection_nonexistent() ->
    {ok, _} = flurm_node_connection_manager:start_link(),

    flurm_node_connection_manager:unregister_connection(<<"nonexistent">>),
    timer:sleep(20),
    ?assert(is_process_alive(whereis(flurm_node_connection_manager))).

%%====================================================================
%% get_connection Tests
%%====================================================================

test_get_connection_found() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid = spawn(fun() -> receive stop -> ok end end),
    flurm_node_connection_manager:register_connection(<<"node001">>, Pid),

    Result = flurm_node_connection_manager:get_connection(<<"node001">>),

    ?assertEqual({ok, Pid}, Result),
    Pid ! stop.

test_get_connection_not_found() ->
    {ok, _} = flurm_node_connection_manager:start_link(),

    Result = flurm_node_connection_manager:get_connection(<<"nonexistent">>),

    ?assertEqual({error, not_connected}, Result).

%%====================================================================
%% find_by_socket Tests
%%====================================================================

test_find_by_socket() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    flurm_node_connection_manager:register_connection(<<"mynode">>, self()),

    Result = flurm_node_connection_manager:find_by_socket(fake_socket),

    ?assertEqual({ok, <<"mynode">>}, Result).

test_find_by_socket_not_found() ->
    {ok, _} = flurm_node_connection_manager:start_link(),

    Result = flurm_node_connection_manager:find_by_socket(fake_socket),

    ?assertEqual(error, Result).

%%====================================================================
%% send_to_node Tests
%%====================================================================

test_send_to_node_success() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Self = self(),
    flurm_node_connection_manager:register_connection(<<"node001">>, Self),

    Result = flurm_node_connection_manager:send_to_node(<<"node001">>, #{type => test}),

    ?assertEqual(ok, Result),
    receive
        {send, #{type := test}} -> ok
    after 100 ->
        ?assert(false)
    end.

test_send_to_node_not_connected() ->
    {ok, _} = flurm_node_connection_manager:start_link(),

    Result = flurm_node_connection_manager:send_to_node(<<"nonexistent">>, #{type => test}),

    ?assertEqual({error, not_connected}, Result).

%%====================================================================
%% send_to_nodes Tests
%%====================================================================

test_send_to_nodes() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Self = self(),
    Pid2 = spawn(fun() ->
        receive {send, _} -> ok end
    end),
    flurm_node_connection_manager:register_connection(<<"node001">>, Self),
    flurm_node_connection_manager:register_connection(<<"node002">>, Pid2),

    Results = flurm_node_connection_manager:send_to_nodes(
        [<<"node001">>, <<"node002">>, <<"nonexistent">>],
        #{type => broadcast}
    ),

    ?assertEqual(3, length(Results)),
    ?assert(lists:member({<<"node001">>, ok}, Results)),
    ?assert(lists:member({<<"node002">>, ok}, Results)),
    ?assert(lists:member({<<"nonexistent">>, {error, not_connected}}, Results)),
    receive
        {send, #{type := broadcast}} -> ok
    after 100 ->
        ?assert(false)
    end.

%%====================================================================
%% list_connected_nodes Tests
%%====================================================================

test_list_connected_nodes() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid1 = spawn(fun() -> receive stop -> ok end end),
    Pid2 = spawn(fun() -> receive stop -> ok end end),
    flurm_node_connection_manager:register_connection(<<"node001">>, Pid1),
    flurm_node_connection_manager:register_connection(<<"node002">>, Pid2),

    Nodes = flurm_node_connection_manager:list_connected_nodes(),

    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"node001">>, Nodes)),
    ?assert(lists:member(<<"node002">>, Nodes)),
    Pid1 ! stop,
    Pid2 ! stop.

%%====================================================================
%% is_node_connected Tests
%%====================================================================

test_is_node_connected_true() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid = spawn(fun() -> receive stop -> ok end end),
    flurm_node_connection_manager:register_connection(<<"node001">>, Pid),

    Result = flurm_node_connection_manager:is_node_connected(<<"node001">>),

    ?assertEqual(true, Result),
    Pid ! stop.

test_is_node_connected_false() ->
    {ok, _} = flurm_node_connection_manager:start_link(),

    Result = flurm_node_connection_manager:is_node_connected(<<"nonexistent">>),

    ?assertEqual(false, Result).

%%====================================================================
%% handle_info Tests
%%====================================================================

test_handle_info_down() ->
    {ok, _} = flurm_node_connection_manager:start_link(),
    Pid = spawn(fun() -> receive stop -> ok end end),
    flurm_node_connection_manager:register_connection(<<"node001">>, Pid),

    Pid ! stop,
    timer:sleep(50),

    Result = flurm_node_connection_manager:get_connection(<<"node001">>),
    ?assertEqual({error, not_connected}, Result).

test_handle_info_unknown() ->
    {ok, Pid} = flurm_node_connection_manager:start_link(),

    Pid ! {unknown, message, here},
    timer:sleep(20),

    ?assert(is_process_alive(Pid)).

%%====================================================================
%% handle_call/handle_cast Edge Cases
%%====================================================================

test_handle_call_unknown() ->
    {ok, Pid} = flurm_node_connection_manager:start_link(),

    Result = gen_server:call(Pid, {unknown_request, arg1, arg2}),

    ?assertEqual({error, unknown_request}, Result).

test_handle_cast_unknown() ->
    {ok, Pid} = flurm_node_connection_manager:start_link(),

    gen_server:cast(Pid, {unknown_cast, data}),
    timer:sleep(20),

    ?assert(is_process_alive(Pid)).

%%====================================================================
%% terminate Tests
%%====================================================================

test_terminate() ->
    {ok, Pid} = flurm_node_connection_manager:start_link(),

    gen_server:stop(Pid, normal, 5000),
    timer:sleep(20),

    ?assertEqual(undefined, whereis(flurm_node_connection_manager)).
