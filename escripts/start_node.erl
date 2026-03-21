#!/usr/bin/env escript
-mode(compile).

%% Usage:
%%   escript start_node.erl host1 port1 [host2:port2 host3:port3 ...]
%%   escript start_node.erl host1            (uses default port 6818)
%%   FLURM_CONTROLLERS=h1:p1,h2:p2 escript start_node.erl

main(Args) ->
    io:format("Starting FLURM node daemon...~n"),
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),

    %% Parse primary controller from first two args
    {ControllerHost, ControllerPort, ExtraArgs} = case Args of
        [Host, Port | Rest] ->
            case catch list_to_integer(Port) of
                P when is_integer(P) -> {Host, P, Rest};
                _ -> {Host, 6818, [Port | Rest]}  % Port wasn't a number, treat as extra controller
            end;
        [Host | Rest] ->
            {Host, 6818, Rest};
        [] ->
            {os:getenv("FLURM_CONTROLLER_HOST", "localhost"), 6818, []}
    end,

    %% Build controller list from:
    %% 1. Primary host:port
    %% 2. Extra args (host:port pairs)
    %% 3. FLURM_CONTROLLERS env var
    ExtraControllers = lists:map(fun(Entry) ->
        parse_host_port(Entry, ControllerPort)
    end, ExtraArgs),

    EnvControllers = case os:getenv("FLURM_CONTROLLERS") of
        false -> [];
        ControllersStr ->
            lists:map(fun(Entry) ->
                parse_host_port(string:trim(Entry), ControllerPort)
            end, string:split(ControllersStr, ",", all))
    end,

    %% Deduplicate: primary + extra args + env var
    Primary = {ControllerHost, ControllerPort},
    AllControllers = dedupe([Primary] ++ ExtraControllers ++ EnvControllers),

    %% Set config
    application:load(flurm_node_daemon),
    application:set_env(flurm_node_daemon, controller_host, ControllerHost),
    application:set_env(flurm_node_daemon, controller_port, ControllerPort),
    application:set_env(flurm_node_daemon, controllers, AllControllers),

    io:format("Controllers: ~p~n", [AllControllers]),

    application:ensure_all_started(lager),
    catch lager:set_loglevel(lager_console_backend, info),
    {ok, _} = application:ensure_all_started(flurm_node_daemon),

    io:format("FLURM node daemon started~n"),
    io:format("Press Ctrl+C to stop~n"),
    receive stop -> ok end.

parse_host_port(Entry, DefaultPort) ->
    case string:split(string:trim(Entry), ":") of
        [H, P] -> {H, list_to_integer(P)};
        [H] -> {H, DefaultPort}
    end.

dedupe(List) ->
    lists:usort(List).
