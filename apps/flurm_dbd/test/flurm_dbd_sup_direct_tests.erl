%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd_sup module
%%%
%%% These tests call the actual flurm_dbd_sup supervisor functions
%%% directly to achieve code coverage. External dependencies like
%%% lager and ranch are mocked.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

flurm_dbd_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link starts supervisor", fun test_start_link/0},
      {"init returns correct child specs", fun test_init/0},
      {"start_listener starts ranch listener", fun test_start_listener/0},
      {"start_listener handles already started", fun test_start_listener_already_started/0},
      {"start_listener handles error", fun test_start_listener_error/0},
      {"stop_listener stops ranch listener", fun test_stop_listener/0},
      {"listener_info returns info when running", fun test_listener_info/0},
      {"listener_info returns error when not running", fun test_listener_info_not_found/0}
     ]}.

setup() ->
    %% Ensure meck is not already mocking
    catch meck:unload(lager),
    catch meck:unload(flurm_dbd_storage),
    catch meck:unload(flurm_dbd_server),

    %% Mock lager for logging
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Mock child processes to avoid starting actual servers
    meck:new(flurm_dbd_storage, [passthrough, no_link]),
    meck:expect(flurm_dbd_storage, start_link, fun() -> {ok, spawn(fun() -> receive stop -> ok end end)} end),

    meck:new(flurm_dbd_server, [passthrough, no_link]),
    meck:expect(flurm_dbd_server, start_link, fun() -> {ok, spawn(fun() -> receive stop -> ok end end)} end),

    %% Set default config
    application:set_env(flurm_dbd, listen_port, 6819),
    application:set_env(flurm_dbd, listen_address, "0.0.0.0"),
    application:set_env(flurm_dbd, num_acceptors, 5),
    application:set_env(flurm_dbd, max_connections, 100),
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    case whereis(flurm_dbd_sup) of
        undefined -> ok;
        Pid ->
            catch supervisor:terminate_child(Pid, flurm_dbd_server),
            catch supervisor:terminate_child(Pid, flurm_dbd_storage),
            flurm_test_utils:kill_and_wait(Pid)
    end,
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(flurm_dbd_storage),
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Supervisor Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_dbd_sup:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Verify children were started
    Children = supervisor:which_children(Pid),
    ?assertEqual(2, length(Children)).

test_init() ->
    %% Call init directly to verify child specs
    {ok, {SupFlags, Children}} = flurm_dbd_sup:init([]),

    %% Check supervisor flags
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(10, maps:get(period, SupFlags)),

    %% Check child specs
    ?assertEqual(2, length(Children)),

    %% Verify storage child spec
    [StorageChild | _] = Children,
    ?assertEqual(flurm_dbd_storage, maps:get(id, StorageChild)),
    ?assertEqual({flurm_dbd_storage, start_link, []}, maps:get(start, StorageChild)),
    ?assertEqual(permanent, maps:get(restart, StorageChild)),
    ?assertEqual(worker, maps:get(type, StorageChild)),

    %% Verify server child spec
    [_, ServerChild] = Children,
    ?assertEqual(flurm_dbd_server, maps:get(id, ServerChild)),
    ?assertEqual({flurm_dbd_server, start_link, []}, maps:get(start, ServerChild)),
    ?assertEqual(permanent, maps:get(restart, ServerChild)).

%%====================================================================
%% Listener Tests
%%====================================================================

test_start_listener() ->
    %% Mock ranch
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    ListenerPid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {ok, ListenerPid} end),

    {ok, _} = flurm_dbd_sup:start_link(),

    Result = flurm_dbd_sup:start_listener(),
    ?assertMatch({ok, _Pid}, Result),
    ?assert(meck:called(ranch, start_listener, '_')),

    catch meck:unload(ranch).

test_start_listener_already_started() ->
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    ExistingPid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {error, {already_started, ExistingPid}} end),

    {ok, _} = flurm_dbd_sup:start_link(),

    Result = flurm_dbd_sup:start_listener(),
    ?assertMatch({ok, _Pid}, Result),

    catch meck:unload(ranch).

test_start_listener_error() ->
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {error, eaddrinuse} end),

    {ok, _} = flurm_dbd_sup:start_link(),

    Result = flurm_dbd_sup:start_listener(),
    ?assertEqual({error, eaddrinuse}, Result),

    catch meck:unload(ranch).

test_stop_listener() ->
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, stop_listener, fun(_) -> ok end),

    Result = flurm_dbd_sup:stop_listener(),
    ?assertEqual(ok, Result),
    ?assert(meck:called(ranch, stop_listener, [flurm_dbd_listener])),

    catch meck:unload(ranch).

test_listener_info() ->
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, get_port, fun(_) -> 6819 end),
    meck:expect(ranch, get_max_connections, fun(_) -> 100 end),
    meck:expect(ranch, procs, fun(_, _) -> 5 end),

    Result = flurm_dbd_sup:listener_info(),
    ?assert(is_map(Result)),
    ?assertEqual(6819, maps:get(port, Result)),
    ?assertEqual(100, maps:get(max_connections, Result)),
    ?assertEqual(5, maps:get(active_connections, Result)),
    ?assertEqual(running, maps:get(status, Result)),

    catch meck:unload(ranch).

test_listener_info_not_found() ->
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, get_port, fun(_) -> error(not_found) end),

    Result = flurm_dbd_sup:listener_info(),
    ?assertEqual({error, not_found}, Result),

    catch meck:unload(ranch).

%%====================================================================
%% Configuration Tests
%%====================================================================

config_test_() ->
    {foreach,
     fun setup_config/0,
     fun cleanup_config/1,
     [
      {"parse_address handles 0.0.0.0", fun test_parse_address_any_ipv4/0},
      {"parse_address handles ::", fun test_parse_address_any_ipv6/0},
      {"parse_address handles valid IP", fun test_parse_address_valid/0},
      {"parse_address handles invalid IP", fun test_parse_address_invalid/0},
      {"parse_address handles tuple", fun test_parse_address_tuple/0},
      {"config reads from application env", fun test_config_from_env/0}
     ]}.

setup_config() ->
    catch meck:unload(lager),
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    ok.

cleanup_config(_) ->
    catch meck:unload(lager),
    catch meck:unload(ranch),
    ok.

%% These tests verify internal functions via start_listener behavior
%% Since parse_address is internal, we test it indirectly

test_parse_address_any_ipv4() ->
    %% Set address to 0.0.0.0 and verify listener config uses it
    application:set_env(flurm_dbd, listen_address, "0.0.0.0"),

    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({0, 0, 0, 0}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_any_ipv6() ->
    application:set_env(flurm_dbd, listen_address, "::"),

    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_valid() ->
    application:set_env(flurm_dbd, listen_address, "192.168.1.100"),

    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({192, 168, 1, 100}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_invalid() ->
    application:set_env(flurm_dbd, listen_address, "not.valid.ip.address"),

    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        %% Invalid address falls back to 0.0.0.0
        ?assertEqual({0, 0, 0, 0}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_tuple() ->
    application:set_env(flurm_dbd, listen_address, {10, 0, 0, 1}),

    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({10, 0, 0, 1}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_config_from_env() ->
    %% Set custom config values
    application:set_env(flurm_dbd, listen_port, 7000),
    application:set_env(flurm_dbd, listen_address, "127.0.0.1"),
    application:set_env(flurm_dbd, num_acceptors, 10),
    application:set_env(flurm_dbd, max_connections, 200),

    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        Port = proplists:get_value(port, SocketOpts),
        IP = proplists:get_value(ip, SocketOpts),
        NumAcceptors = maps:get(num_acceptors, TransportOpts),
        MaxConns = maps:get(max_connections, TransportOpts),

        ?assertEqual(7000, Port),
        ?assertEqual({127, 0, 0, 1}, IP),
        ?assertEqual(10, NumAcceptors),
        ?assertEqual(200, MaxConns),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener(),

    %% Reset to defaults
    application:set_env(flurm_dbd, listen_port, 6819),
    application:set_env(flurm_dbd, listen_address, "0.0.0.0"),
    application:set_env(flurm_dbd, num_acceptors, 5),
    application:set_env(flurm_dbd, max_connections, 100).

%%====================================================================
%% Socket Options Test
%%====================================================================

socket_options_test_() ->
    {foreach,
     fun() ->
         catch meck:unload(lager),
         meck:new(lager, [no_link, non_strict]),
         meck:expect(lager, info, fun(_, _) -> ok end),
         meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
         ok
     end,
     fun(_) ->
         catch meck:unload(lager),
         catch meck:unload(ranch),
         ok
     end,
     [
      {"start_listener sets correct socket options", fun test_socket_options/0}
     ]}.

test_socket_options() ->
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),

        %% Verify all required socket options
        ?assert(proplists:is_defined(ip, SocketOpts)),
        ?assert(proplists:is_defined(port, SocketOpts)),
        ?assertEqual(true, proplists:get_value(nodelay, SocketOpts)),
        ?assertEqual(true, proplists:get_value(keepalive, SocketOpts)),
        ?assertEqual(true, proplists:get_value(reuseaddr, SocketOpts)),
        ?assertEqual(128, proplists:get_value(backlog, SocketOpts)),

        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().
