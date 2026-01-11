#!/usr/bin/env escript
-mode(compile).

main(_) ->
    io:format("Starting FLURM controller...~n"),
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),
    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, info),
    {ok, _} = application:ensure_all_started(flurm_controller),
    io:format("FLURM controller listening on port 6817~n"),
    io:format("Press Ctrl+C to stop~n"),
    receive stop -> ok end.
