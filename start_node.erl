#!/usr/bin/env escript
-mode(compile).

main(Args) ->
    io:format("Starting FLURM node daemon...~n"),
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),

    %% Parse controller host from args or environment
    ControllerHost = case Args of
        [Host | _] -> Host;
        [] -> os:getenv("FLURM_CONTROLLER_HOST", "localhost")
    end,

    ControllerPort = case os:getenv("FLURM_CONTROLLER_PORT") of
        false -> 6818;
        Port -> list_to_integer(Port)
    end,

    %% Set the controller host/port before starting
    application:load(flurm_node_daemon),
    application:set_env(flurm_node_daemon, controller_host, ControllerHost),
    application:set_env(flurm_node_daemon, controller_port, ControllerPort),

    io:format("Connecting to controller at ~s:~p~n", [ControllerHost, ControllerPort]),

    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, info),
    {ok, _} = application:ensure_all_started(flurm_node_daemon),

    io:format("FLURM node daemon started~n"),
    io:format("Press Ctrl+C to stop~n"),
    receive stop -> ok end.
