%%%-------------------------------------------------------------------
%%% @doc Test utilities for proper synchronization (no timer:sleep!)
%%%
%%% Erlang has sophisticated synchronization - use it.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_utils).

-export([
    %% Process death synchronization
    wait_for_death/1,
    wait_for_death/2,
    kill_and_wait/1,
    kill_and_wait/2,

    %% Process registration synchronization
    wait_for_registered/1,
    wait_for_registered/2,
    wait_for_unregistered/1,
    wait_for_unregistered/2,

    %% gen_server synchronization
    sync_cast/2,
    sync_send/2,

    %% Mock synchronization
    wait_for_mock_call/3,
    wait_for_mock_call/4,

    %% General utilities
    flush_mailbox/0,
    with_timeout/2
]).

-define(DEFAULT_TIMEOUT, 5000).

%%====================================================================
%% Process death synchronization
%%====================================================================

%% @doc Wait for a process to die. Returns ok when dead, or {error, timeout}.
-spec wait_for_death(pid()) -> ok | {error, timeout}.
wait_for_death(Pid) ->
    wait_for_death(Pid, ?DEFAULT_TIMEOUT).

-spec wait_for_death(pid(), timeout()) -> ok | {error, timeout}.
wait_for_death(Pid, Timeout) ->
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after Timeout ->
        demonitor(Ref, [flush]),
        case is_process_alive(Pid) of
            false -> ok;
            true -> {error, timeout}
        end
    end.

%% @doc Kill a process and wait for it to die.
-spec kill_and_wait(pid()) -> ok | {error, timeout}.
kill_and_wait(Pid) ->
    kill_and_wait(Pid, ?DEFAULT_TIMEOUT).

-spec kill_and_wait(pid(), timeout()) -> ok | {error, timeout}.
kill_and_wait(Pid, Timeout) ->
    Ref = monitor(process, Pid),
    unlink(Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after Timeout ->
        demonitor(Ref, [flush]),
        exit(Pid, kill),
        case wait_for_death(Pid, 100) of
            ok -> ok;
            _ -> {error, timeout}
        end
    end.

%%====================================================================
%% Process registration synchronization
%%====================================================================

%% @doc Wait for a name to be registered.
-spec wait_for_registered(atom()) -> pid() | {error, timeout}.
wait_for_registered(Name) ->
    wait_for_registered(Name, ?DEFAULT_TIMEOUT).

-spec wait_for_registered(atom(), timeout()) -> pid() | {error, timeout}.
wait_for_registered(Name, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_registered_loop(Name, Deadline).

wait_for_registered_loop(Name, Deadline) ->
    case whereis(Name) of
        undefined ->
            Now = erlang:monotonic_time(millisecond),
            case Now < Deadline of
                true ->
                    erlang:yield(),
                    wait_for_registered_loop(Name, Deadline);
                false ->
                    {error, timeout}
            end;
        Pid ->
            Pid
    end.

%% @doc Wait for a name to be unregistered.
-spec wait_for_unregistered(atom()) -> ok | {error, timeout}.
wait_for_unregistered(Name) ->
    wait_for_unregistered(Name, ?DEFAULT_TIMEOUT).

-spec wait_for_unregistered(atom(), timeout()) -> ok | {error, timeout}.
wait_for_unregistered(Name, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_unregistered_loop(Name, Deadline).

wait_for_unregistered_loop(Name, Deadline) ->
    case whereis(Name) of
        undefined -> ok;
        _Pid ->
            Now = erlang:monotonic_time(millisecond),
            case Now < Deadline of
                true ->
                    erlang:yield(),
                    wait_for_unregistered_loop(Name, Deadline);
                false ->
                    {error, timeout}
            end
    end.

%%====================================================================
%% gen_server synchronization
%%====================================================================

%% @doc Send a cast and synchronize by doing a call afterward.
%% This ensures the cast has been processed.
-spec sync_cast(pid() | atom(), term()) -> ok.
sync_cast(ServerRef, Msg) ->
    gen_server:cast(ServerRef, Msg),
    %% sys:get_state is synchronous - waits for current message processing
    _ = sys:get_state(ServerRef),
    ok.

%% @doc Send an info message and synchronize.
-spec sync_send(pid() | atom(), term()) -> ok.
sync_send(ServerRef, Msg) ->
    ServerRef ! Msg,
    _ = sys:get_state(ServerRef),
    ok.

%%====================================================================
%% Mock synchronization
%%====================================================================

%% @doc Wait for a meck mock to be called.
-spec wait_for_mock_call(module(), atom(), non_neg_integer()) -> ok | {error, timeout}.
wait_for_mock_call(Mod, Fun, Arity) ->
    wait_for_mock_call(Mod, Fun, Arity, ?DEFAULT_TIMEOUT).

-spec wait_for_mock_call(module(), atom(), non_neg_integer(), timeout()) -> ok | {error, timeout}.
wait_for_mock_call(Mod, Fun, Arity, Timeout) ->
    try
        meck:wait(Mod, Fun, Arity, Timeout),
        ok
    catch
        error:timeout -> {error, timeout}
    end.

%%====================================================================
%% General utilities
%%====================================================================

%% @doc Flush all messages from the current process mailbox.
-spec flush_mailbox() -> ok.
flush_mailbox() ->
    receive
        _ -> flush_mailbox()
    after 0 ->
        ok
    end.

%% @doc Execute a function with a timeout.
-spec with_timeout(fun(() -> T), timeout()) -> {ok, T} | {error, timeout}.
with_timeout(Fun, Timeout) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
        Result = Fun(),
        Parent ! {Ref, Result}
    end),
    receive
        {Ref, Result} -> {ok, Result}
    after Timeout ->
        exit(Pid, kill),
        {error, timeout}
    end.
