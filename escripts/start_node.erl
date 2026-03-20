#!/usr/bin/env escript
-mode(compile).

main(Args) ->
    io:format("Starting FLURM node daemon...~n"),
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),

    %% Parse controller host and port from args or environment
    {ControllerHost, ControllerPort} = case Args of
        [Host, Port | _] ->
            {Host, list_to_integer(Port)};
        [Host | _] ->
            PortFromEnv = case os:getenv("FLURM_CONTROLLER_PORT") of
                false -> 6818;
                P -> list_to_integer(P)
            end,
            {Host, PortFromEnv};
        [] ->
            {os:getenv("FLURM_CONTROLLER_HOST", "localhost"),
             case os:getenv("FLURM_CONTROLLER_PORT") of
                 false -> 6818;
                 P2 -> list_to_integer(P2)
             end}
    end,

    %% Set the controller host/port before starting
    application:load(flurm_node_daemon),
    application:set_env(flurm_node_daemon, controller_host, ControllerHost),
    application:set_env(flurm_node_daemon, controller_port, ControllerPort),

    %% Parse backup controllers from FLURM_CONTROLLERS env var
    %% Format: "host1:port1,host2:port2,..."
    case os:getenv("FLURM_CONTROLLERS") of
        false ->
            ok;
        ControllersStr ->
            Controllers = lists:map(fun(Entry) ->
                case string:split(string:trim(Entry), ":") of
                    [H, P] -> {H, list_to_integer(P)};
                    [H] -> {H, ControllerPort}
                end
            end, string:split(ControllersStr, ",", all)),
            application:set_env(flurm_node_daemon, controllers, Controllers),
            io:format("Backup controllers: ~p~n", [Controllers])
    end,

    io:format("Connecting to controller at ~s:~p~n", [ControllerHost, ControllerPort]),

    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, info),
    {ok, _} = application:ensure_all_started(flurm_node_daemon),

    io:format("FLURM node daemon started~n"),
    io:format("Press Ctrl+C to stop~n"),
    receive stop -> ok end.
