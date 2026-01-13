#!/usr/bin/env escript
%% -*- erlang -*-

-mode(compile).

main(_) ->
    io:format("~n=== FLURM Performance Benchmarks ===~n"),
    io:format("Running on Erlang/OTP ~s~n~n", [erlang:system_info(otp_release)]),

    %% Binary operations (similar to protocol codec)
    io:format("--- Binary Operations (Protocol Codec Proxy) ---~n"),
    bench_binary_encode(),
    bench_binary_decode(),

    %% Data structure benchmarks
    io:format("~n--- Data Structures ---~n"),
    bench_ets(),
    bench_maps(),

    %% Concurrency benchmarks
    io:format("~n--- Concurrency ---~n"),
    bench_spawn(),
    bench_message_passing(),

    %% Gen_server simulation
    io:format("~n--- Gen_server Simulation ---~n"),
    bench_gen_server_calls(),

    io:format("~n=== Benchmarks Complete ===~n~n"),
    ok.

bench_binary_encode() ->
    N = 10000,
    %% Simulate SLURM header encoding (10 bytes)
    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            <<16#2600:16/big, 0:16/big, 4003:16/big, 100:32/big>>
        end, lists:seq(1, N))
    end),
    report("Binary header encode", N, Time).

bench_binary_decode() ->
    N = 10000,
    Header = <<16#2600:16/big, 0:16/big, 4003:16/big, 100:32/big>>,
    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            <<V:16/big, F:16/big, T:16/big, L:32/big>> = Header,
            {V, F, T, L}
        end, lists:seq(1, N))
    end),
    report("Binary header decode", N, Time).

bench_ets() ->
    N = 10000,
    Tab = ets:new(bench_tab, [set, public]),

    %% Insert
    {TimeInsert, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            ets:insert(Tab, {I, #{id => I, name => <<"job">>, state => pending}})
        end, lists:seq(1, N))
    end),
    report("ETS insert", N, TimeInsert),

    %% Lookup
    {TimeLookup, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            ets:lookup(Tab, I)
        end, lists:seq(1, N))
    end),
    report("ETS lookup", N, TimeLookup),

    ets:delete(Tab).

bench_maps() ->
    N = 10000,

    %% Build map
    {TimePut, Map} = timer:tc(fun() ->
        lists:foldl(fun(I, Acc) ->
            maps:put(I, #{id => I, name => <<"job">>, state => pending}, Acc)
        end, #{}, lists:seq(1, N))
    end),
    report("Map put", N, TimePut),

    %% Lookup
    {TimeGet, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            maps:get(I, Map)
        end, lists:seq(1, N))
    end),
    report("Map get", N, TimeGet).

bench_spawn() ->
    N = 10000,
    {Time, _} = timer:tc(fun() ->
        Pids = [spawn(fun() -> receive stop -> ok end end) || _ <- lists:seq(1, N)],
        [Pid ! stop || Pid <- Pids]
    end),
    report("Process spawn+stop", N, Time).

bench_message_passing() ->
    N = 100000,
    Self = self(),
    Pid = spawn(fun() ->
        msg_loop(0, N, Self)
    end),
    {Time, _} = timer:tc(fun() ->
        [Pid ! {ping, I} || I <- lists:seq(1, N)],
        receive {done, N} -> ok end
    end),
    report("Message send+receive", N, Time).

msg_loop(Count, Max, Parent) when Count >= Max ->
    Parent ! {done, Count};
msg_loop(Count, Max, Parent) ->
    receive
        {ping, _} -> msg_loop(Count + 1, Max, Parent)
    end.

bench_gen_server_calls() ->
    N = 10000,
    %% Start a simple gen_server-like process
    Pid = spawn(fun() -> gen_loop(0) end),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Ref = make_ref(),
            Pid ! {call, self(), Ref, {submit_job, I}},
            receive {Ref, ok} -> ok end
        end, lists:seq(1, N))
    end),
    Pid ! stop,
    report("Gen_server call (sync)", N, Time).

gen_loop(State) ->
    receive
        {call, From, Ref, {submit_job, _JobId}} ->
            %% Simulate job submission work
            From ! {Ref, ok},
            gen_loop(State + 1);
        stop ->
            ok
    end.

report(Name, N, TimeUs) ->
    TimeMs = TimeUs / 1000,
    OpsPerSec = round(N / (TimeUs / 1000000)),
    LatencyUs = TimeUs / N,
    io:format("  ~s: ~.2f ms, ~w ops/sec, ~.3f us/op~n",
              [Name, TimeMs, OpsPerSec, LatencyUs]).
