#!/usr/bin/env escript
-mode(compile).

main(_) ->
    io:format("Starting FLURM controller...~n"),
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),

    %% Configure config file path
    ConfigFile = case os:getenv("FLURM_CONFIG_FILE") of
        false ->
            %% Try standard locations
            Candidates = ["/etc/flurm/slurm.conf", "/etc/slurm/slurm.conf", "slurm.conf"],
            case lists:filter(fun filelib:is_file/1, Candidates) of
                [First | _] -> First;
                [] -> undefined
            end;
        Path ->
            Path
    end,

    case ConfigFile of
        undefined ->
            io:format("No config file found~n");
        _ ->
            io:format("Using config file: ~s~n", [ConfigFile]),
            application:load(flurm_config),
            application:set_env(flurm_config, config_file, ConfigFile)
    end,

    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, info),
    {ok, _} = application:ensure_all_started(flurm_controller),
    io:format("FLURM controller listening on port 6817~n"),
    io:format("Press Ctrl+C to stop~n"),
    receive stop -> ok end.
