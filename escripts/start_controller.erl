#!/usr/bin/env escript
-mode(compile).

main(_) ->
    io:format("Starting FLURM controller...~n"),

    %% Start epmd if not running (required for distributed Erlang)
    os:cmd("epmd -daemon"),
    timer:sleep(500),  %% Give epmd time to start

    %% Start distributed Erlang
    NodeName = case os:getenv("FLURM_NODE_NAME") of
        false ->
            %% Generate node name from hostname
            {ok, Hostname} = inet:gethostname(),
            list_to_atom("flurmctld@" ++ Hostname);
        Name ->
            list_to_atom(Name)
    end,

    %% Start the distributed node
    case net_kernel:start([NodeName, shortnames]) of
        {ok, _} ->
            io:format("Node started as: ~p~n", [node()]);
        {error, {already_started, _}} ->
            io:format("Node already distributed: ~p~n", [node()]);
        {error, Reason} ->
            io:format("Warning: Could not start distributed node: ~p~n", [Reason]),
            io:format("Running in non-distributed mode (Ra cluster disabled)~n")
    end,

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

    %% Start the database daemon (accounting) if available
    case application:ensure_all_started(flurm_dbd) of
        {ok, _} ->
            io:format("FLURM database daemon (accounting) listening on port 6819~n");
        {error, DbdReason} ->
            io:format("Warning: Could not start FLURM DBD: ~p~n", [DbdReason])
    end,

    io:format("Press Ctrl+C to stop~n"),
    receive stop -> ok end.
