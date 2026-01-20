%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_dbd_sup module
%%%
%%% Tests the DBD supervisor directly. External dependencies like
%%% lager and ranch are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_dbd_sup_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link starts supervisor", fun test_start_link/0},
      {"init/1 returns correct child specs", fun test_init/0},
      {"start_listener starts ranch listener", fun test_start_listener/0},
      {"start_listener handles already_started", fun test_start_listener_already_started/0},
      {"start_listener handles error", fun test_start_listener_error/0},
      {"stop_listener stops ranch listener", fun test_stop_listener/0},
      {"listener_info returns map when running", fun test_listener_info_running/0},
      {"listener_info returns error when not found", fun test_listener_info_not_found/0},
      {"parse_address handles 0.0.0.0", fun test_parse_address_ipv4_any/0},
      {"parse_address handles ::", fun test_parse_address_ipv6_any/0},
      {"parse_address handles valid address string", fun test_parse_address_valid/0},
      {"parse_address handles invalid address", fun test_parse_address_invalid/0},
      {"parse_address handles tuple", fun test_parse_address_tuple/0},
      {"get_listener_config reads app env", fun test_get_listener_config/0}
     ]}.

setup() ->
    %% Stop existing supervisor if running
    case whereis(flurm_dbd_sup) of
        undefined -> ok;
        Pid -> catch supervisor:terminate_child(Pid, flurm_dbd_storage),
               catch supervisor:terminate_child(Pid, flurm_dbd_server),
               flurm_test_utils:kill_and_wait(Pid)
    end,

    meck:new(lager, [non_strict]),
    meck:new(ranch, [non_strict]),

    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Default ranch expectations
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {ok, self()} end),
    meck:expect(ranch, stop_listener, fun(_) -> ok end),
    meck:expect(ranch, get_port, fun(_) -> 6819 end),
    meck:expect(ranch, get_max_connections, fun(_) -> 100 end),
    meck:expect(ranch, procs, fun(_, connections) -> [] end),

    %% Set default app env
    application:set_env(flurm_dbd, listen_port, 6819),
    application:set_env(flurm_dbd, listen_address, "0.0.0.0"),
    application:set_env(flurm_dbd, num_acceptors, 5),
    application:set_env(flurm_dbd, max_connections, 100),
    ok.

cleanup(_) ->
    case whereis(flurm_dbd_sup) of
        undefined -> ok;
        Pid -> flurm_test_utils:kill_and_wait(Pid)
    end,
    catch meck:unload(lager),
    catch meck:unload(ranch),
    ok.

%%====================================================================
%% start_link Tests
%%====================================================================

test_start_link() ->
    %% Mock the child processes to avoid actually starting them
    meck:new(flurm_dbd_storage, [non_strict]),
    meck:new(flurm_dbd_server, [non_strict]),
    meck:expect(flurm_dbd_storage, start_link, fun() -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(flurm_dbd_server, start_link, fun() -> {ok, spawn(fun() -> receive stop -> ok end end)} end),

    Result = flurm_dbd_sup:start_link(),

    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assert(is_process_alive(Pid)),

    meck:unload(flurm_dbd_storage),
    meck:unload(flurm_dbd_server).

%%====================================================================
%% init/1 Tests
%%====================================================================

test_init() ->
    {ok, {SupFlags, Children}} = flurm_dbd_sup:init([]),

    %% Verify supervisor flags
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(10, maps:get(period, SupFlags)),

    %% Verify children
    ?assertEqual(2, length(Children)),

    %% Find storage child spec
    StorageSpec = lists:keyfind(flurm_dbd_storage, 1, [{maps:get(id, C), C} || C <- Children]),
    ?assertNotEqual(false, StorageSpec),

    %% Find server child spec
    ServerSpec = lists:keyfind(flurm_dbd_server, 1, [{maps:get(id, C), C} || C <- Children]),
    ?assertNotEqual(false, ServerSpec).

%%====================================================================
%% start_listener Tests
%%====================================================================

test_start_listener() ->
    ListenerPid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {ok, ListenerPid} end),

    Result = flurm_dbd_sup:start_listener(),

    ?assertEqual({ok, ListenerPid}, Result),
    ListenerPid ! stop.

test_start_listener_already_started() ->
    ExistingPid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) ->
        {error, {already_started, ExistingPid}}
    end),

    Result = flurm_dbd_sup:start_listener(),

    ?assertEqual({ok, ExistingPid}, Result),
    ExistingPid ! stop.

test_start_listener_error() ->
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) ->
        {error, eaddrinuse}
    end),

    Result = flurm_dbd_sup:start_listener(),

    ?assertEqual({error, eaddrinuse}, Result).

%%====================================================================
%% stop_listener Tests
%%====================================================================

test_stop_listener() ->
    meck:expect(ranch, stop_listener, fun(flurm_dbd_listener) -> ok end),

    Result = flurm_dbd_sup:stop_listener(),

    ?assertEqual(ok, Result),
    ?assert(meck:called(ranch, stop_listener, [flurm_dbd_listener])).

%%====================================================================
%% listener_info Tests
%%====================================================================

test_listener_info_running() ->
    meck:expect(ranch, get_port, fun(flurm_dbd_listener) -> 6819 end),
    meck:expect(ranch, get_max_connections, fun(flurm_dbd_listener) -> 100 end),
    meck:expect(ranch, procs, fun(flurm_dbd_listener, connections) -> [self(), self()] end),

    Result = flurm_dbd_sup:listener_info(),

    ?assert(is_map(Result)),
    ?assertEqual(6819, maps:get(port, Result)),
    ?assertEqual(100, maps:get(max_connections, Result)),
    ?assertEqual(running, maps:get(status, Result)).

test_listener_info_not_found() ->
    meck:expect(ranch, get_port, fun(_) -> meck:exception(error, badarg) end),

    Result = flurm_dbd_sup:listener_info(),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% parse_address Tests
%%====================================================================

test_parse_address_ipv4_any() ->
    %% We need to test internal function via start_listener behavior
    application:set_env(flurm_dbd, listen_address, "0.0.0.0"),

    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({0, 0, 0, 0}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_ipv6_any() ->
    application:set_env(flurm_dbd, listen_address, "::"),

    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_valid() ->
    application:set_env(flurm_dbd, listen_address, "192.168.1.1"),

    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({192, 168, 1, 1}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_invalid() ->
    application:set_env(flurm_dbd, listen_address, "invalid.address"),

    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        %% Invalid address should default to 0.0.0.0
        ?assertEqual({0, 0, 0, 0}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

test_parse_address_tuple() ->
    application:set_env(flurm_dbd, listen_address, {10, 0, 0, 1}),

    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        IP = proplists:get_value(ip, SocketOpts),
        ?assertEqual({10, 0, 0, 1}, IP),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

%%====================================================================
%% get_listener_config Tests
%%====================================================================

test_get_listener_config() ->
    application:set_env(flurm_dbd, listen_port, 7000),
    application:set_env(flurm_dbd, listen_address, "127.0.0.1"),
    application:set_env(flurm_dbd, num_acceptors, 10),
    application:set_env(flurm_dbd, max_connections, 200),

    meck:expect(ranch, start_listener, fun(_, _, TransportOpts, _, _) ->
        SocketOpts = maps:get(socket_opts, TransportOpts),
        ?assertEqual(7000, proplists:get_value(port, SocketOpts)),
        ?assertEqual(10, maps:get(num_acceptors, TransportOpts)),
        ?assertEqual(200, maps:get(max_connections, TransportOpts)),
        {ok, self()}
    end),

    flurm_dbd_sup:start_listener().

%%====================================================================
%% Supervisor Behaviour Tests
%%====================================================================

behaviour_test_() ->
    {"flurm_dbd_sup implements supervisor behaviour", fun test_behaviour/0}.

test_behaviour() ->
    Exports = flurm_dbd_sup:module_info(exports),
    ?assert(lists:member({start_link, 0}, Exports)),
    ?assert(lists:member({init, 1}, Exports)).
