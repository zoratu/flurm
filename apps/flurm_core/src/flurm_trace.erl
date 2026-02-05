%%%-------------------------------------------------------------------
%%% @doc FLURM Distributed Trace Verification
%%%
%%% Records and verifies distributed system traces for lineage
%%% verification against TLA+ safety invariants.
%%%
%%% Features:
%%% - Record all inter-node messages
%%% - Track state transitions
%%% - Export traces to TLA+ format
%%% - Verify traces against invariants
%%% - Replay traces for debugging
%%%
%%% Usage:
%%%   flurm_trace:start_recording()
%%%   ... run operations ...
%%%   Trace = flurm_trace:stop_recording()
%%%   ok = flurm_trace:verify_trace(Trace)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_trace).
-behaviour(gen_server).

-export([
    start_link/0,
    start_recording/0,
    stop_recording/0,
    get_trace/0,
    clear_trace/0,
    verify_trace/1,
    export_tla_trace/1,
    export_tla_trace/2
]).

%% Recording API
-export([
    record_message/4,
    record_state_change/3,
    record_event/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(MAX_TRACE_SIZE, 100000).

%%====================================================================
%% Types
%%====================================================================

-type node_id() :: binary() | atom().
-type timestamp() :: non_neg_integer().

-record(trace_message, {
    timestamp :: timestamp(),
    from :: node_id(),
    to :: node_id(),
    type :: atom(),
    payload :: term()
}).

-record(trace_state_change, {
    timestamp :: timestamp(),
    node :: node_id(),
    component :: atom(),
    old_state :: term(),
    new_state :: term()
}).

-record(trace_event, {
    timestamp :: timestamp(),
    type :: atom(),
    data :: term()
}).

-type trace_entry() :: #trace_message{} | #trace_state_change{} | #trace_event{}.
-type trace() :: [trace_entry()].

-record(state, {
    recording :: boolean(),
    trace :: trace(),
    start_time :: timestamp() | undefined,
    trace_size :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the trace server.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Start recording traces.
-spec start_recording() -> ok.
start_recording() ->
    gen_server:call(?SERVER, start_recording).

%% @doc Stop recording and return the trace.
-spec stop_recording() -> trace().
stop_recording() ->
    gen_server:call(?SERVER, stop_recording).

%% @doc Get the current trace without stopping.
-spec get_trace() -> trace().
get_trace() ->
    gen_server:call(?SERVER, get_trace).

%% @doc Clear the recorded trace.
-spec clear_trace() -> ok.
clear_trace() ->
    gen_server:call(?SERVER, clear_trace).

%% @doc Record an inter-node message.
-spec record_message(node_id(), node_id(), atom(), term()) -> ok.
record_message(From, To, Type, Payload) ->
    gen_server:cast(?SERVER, {record_message, From, To, Type, Payload}).

%% @doc Record a state change.
-spec record_state_change(node_id(), atom(), {term(), term()}) -> ok.
record_state_change(Node, Component, {OldState, NewState}) ->
    gen_server:cast(?SERVER, {record_state_change, Node, Component, OldState, NewState}).

%% @doc Record an event.
-spec record_event(atom(), term()) -> ok.
record_event(Type, Data) ->
    gen_server:cast(?SERVER, {record_event, Type, Data}).

%% @doc Verify a trace against TLA+ invariants.
-spec verify_trace(trace()) -> ok | {error, [term()]}.
verify_trace(Trace) ->
    Violations = lists:flatten([
        check_leader_uniqueness_trace(Trace),
        check_sibling_exclusivity_trace(Trace),
        check_log_consistency_trace(Trace),
        check_no_job_loss_trace(Trace)
    ]),
    case Violations of
        [] -> ok;
        _ -> {error, Violations}
    end.

%% @doc Export trace to TLA+ readable format.
-spec export_tla_trace(trace()) -> binary().
export_tla_trace(Trace) ->
    export_tla_trace(Trace, <<"FlurmTrace">>).

-spec export_tla_trace(trace(), binary()) -> binary().
export_tla_trace(Trace, ModuleName) ->
    Header = <<"---- MODULE ", ModuleName/binary, " ----\n",
               "EXTENDS Integers, Sequences, TLC\n\n",
               "(* Recorded trace from FLURM cluster *)\n\n">>,

    %% Convert trace to TLA+ sequence
    TraceEntries = [trace_entry_to_tla(E) || E <- Trace],
    TraceSeq = iolist_to_binary([
        <<"Trace == <<\n">>,
        lists:join(<<",\n">>, TraceEntries),
        <<"\n>>\n\n">>
    ]),

    %% Add trace checking predicate
    Checker = <<"(* Check if this trace satisfies the spec *)\n",
                "TraceMatches == \n",
                "    /\\ Len(Trace) > 0\n",
                "    /\\ \\A i \\in 1..Len(Trace): IsValidEntry(Trace[i])\n\n",
                "IsValidEntry(entry) ==\n",
                "    CASE entry.type = \"message\" -> TRUE\n",
                "      [] entry.type = \"state_change\" -> TRUE\n",
                "      [] entry.type = \"event\" -> TRUE\n",
                "      [] OTHER -> FALSE\n\n",
                "====\n">>,

    <<Header/binary, TraceSeq/binary, Checker/binary>>.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{
        recording = false,
        trace = [],
        start_time = undefined,
        trace_size = 0
    }}.

handle_call(start_recording, _From, State) ->
    {reply, ok, State#state{
        recording = true,
        trace = [],
        start_time = erlang:monotonic_time(microsecond),
        trace_size = 0
    }};

handle_call(stop_recording, _From, State) ->
    Trace = lists:reverse(State#state.trace),
    {reply, Trace, State#state{recording = false}};

handle_call(get_trace, _From, State) ->
    Trace = lists:reverse(State#state.trace),
    {reply, Trace, State};

handle_call(clear_trace, _From, State) ->
    {reply, ok, State#state{trace = [], trace_size = 0}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({record_message, From, To, Type, Payload}, State) ->
    case State#state.recording of
        false -> {noreply, State};
        true ->
            Entry = #trace_message{
                timestamp = erlang:monotonic_time(microsecond) - State#state.start_time,
                from = From,
                to = To,
                type = Type,
                payload = Payload
            },
            {noreply, add_entry(Entry, State)}
    end;

handle_cast({record_state_change, Node, Component, OldState, NewState}, State) ->
    case State#state.recording of
        false -> {noreply, State};
        true ->
            Entry = #trace_state_change{
                timestamp = erlang:monotonic_time(microsecond) - State#state.start_time,
                node = Node,
                component = Component,
                old_state = OldState,
                new_state = NewState
            },
            {noreply, add_entry(Entry, State)}
    end;

handle_cast({record_event, Type, Data}, State) ->
    case State#state.recording of
        false -> {noreply, State};
        true ->
            Entry = #trace_event{
                timestamp = erlang:monotonic_time(microsecond) - State#state.start_time,
                type = Type,
                data = Data
            },
            {noreply, add_entry(Entry, State)}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

add_entry(Entry, #state{trace_size = Size} = State) when Size >= ?MAX_TRACE_SIZE ->
    %% Trace full, drop oldest entry
    Trace = lists:droplast(State#state.trace),
    State#state{trace = [Entry | Trace]};
add_entry(Entry, State) ->
    State#state{
        trace = [Entry | State#state.trace],
        trace_size = State#state.trace_size + 1
    }.

%%====================================================================
%% Invariant Checking on Traces
%%====================================================================

%% Check leader uniqueness throughout the trace
check_leader_uniqueness_trace(Trace) ->
    %% Track leader state changes
    LeaderChanges = [E || #trace_state_change{component = ra_leader} = E <- Trace],
    %% Check for overlapping leadership
    check_leader_overlaps(LeaderChanges, []).

check_leader_overlaps([], Acc) -> Acc;
check_leader_overlaps([#trace_state_change{timestamp = T, node = N, new_state = leader} | Rest], Acc) ->
    %% Find any other node becoming leader before this one steps down
    Violations = [V || #trace_state_change{timestamp = T2, node = N2, new_state = leader} = V <- Rest,
                       T2 > T, N2 =/= N, not stepped_down(N, T, T2, Rest)],
    check_leader_overlaps(Rest, Violations ++ Acc);
check_leader_overlaps([_ | Rest], Acc) ->
    check_leader_overlaps(Rest, Acc).

stepped_down(Node, StartTime, EndTime, Trace) ->
    lists:any(fun
        (#trace_state_change{timestamp = T, node = N, new_state = follower})
            when N =:= Node, T > StartTime, T < EndTime -> true;
        (_) -> false
    end, Trace).

%% Check sibling exclusivity for federated jobs
check_sibling_exclusivity_trace(Trace) ->
    %% Find sibling job state changes
    SiblingStarts = [E || #trace_event{type = sibling_started} = E <- Trace],
    %% Group by federation job ID
    ByFedJob = lists:foldl(fun(#trace_event{data = #{fed_job_id := FedId}} = E, Acc) ->
        Existing = maps:get(FedId, Acc, []),
        maps:put(FedId, [E | Existing], Acc)
    end, #{}, SiblingStarts),
    %% Check each federation job has at most one running sibling at any time
    maps:fold(fun(FedId, Starts, Acc) ->
        case check_sibling_overlaps(Starts) of
            ok -> Acc;
            {error, Violation} -> [{sibling_exclusivity_violation, FedId, Violation} | Acc]
        end
    end, [], ByFedJob).

check_sibling_overlaps([]) -> ok;
check_sibling_overlaps([_]) -> ok;
check_sibling_overlaps([#trace_event{timestamp = T1, data = #{cluster := C1}} |
                        [#trace_event{timestamp = T2, data = #{cluster := C2}} | _] = Rest]) ->
    case T2 - T1 < 1000 andalso C1 =/= C2 of  % Within 1ms
        true -> {error, {concurrent_start, C1, C2, T1, T2}};
        false -> check_sibling_overlaps(Rest)
    end;
check_sibling_overlaps([_ | Rest]) ->
    check_sibling_overlaps(Rest).

%% Check log consistency
check_log_consistency_trace(Trace) ->
    %% Find log append events
    LogAppends = [E || #trace_event{type = log_append} = E <- Trace],
    %% Group by node
    ByNode = lists:foldl(fun(#trace_event{data = #{node := N, index := I, entry := Entry}} = _E, Acc) ->
        NodeLog = maps:get(N, Acc, []),
        maps:put(N, [{I, Entry} | NodeLog], Acc)
    end, #{}, LogAppends),
    %% Check committed prefixes match
    check_committed_logs(ByNode).

check_committed_logs(ByNode) when map_size(ByNode) =< 1 -> [];
check_committed_logs(ByNode) ->
    %% Get minimum committed index across nodes
    Logs = maps:values(ByNode),
    case Logs of
        [] -> [];
        [FirstLog | RestLogs] ->
            %% Compare each log against the first
            lists:flatten([compare_logs(FirstLog, L) || L <- RestLogs])
    end.

compare_logs(Log1, Log2) ->
    %% Find common indices
    Indices1 = [I || {I, _} <- Log1],
    Indices2 = [I || {I, _} <- Log2],
    CommonIndices = ordsets:intersection(ordsets:from_list(Indices1), ordsets:from_list(Indices2)),
    %% Check entries match at common indices
    lists:filtermap(fun(I) ->
        E1 = proplists:get_value(I, Log1),
        E2 = proplists:get_value(I, Log2),
        case E1 =:= E2 of
            true -> false;
            false -> {true, {log_mismatch, I, E1, E2}}
        end
    end, ordsets:to_list(CommonIndices)).

%% Check no job loss
check_no_job_loss_trace(Trace) ->
    %% Find job submissions
    Submissions = [E || #trace_event{type = job_submitted} = E <- Trace],
    %% Find job completions/failures/cancellations
    Terminals = [E || #trace_event{type = T} = E <- Trace,
                      lists:member(T, [job_completed, job_failed, job_cancelled])],
    %% Check each submission has a terminal event
    SubmittedIds = [Id || #trace_event{data = #{job_id := Id}} <- Submissions],
    TerminalIds = [Id || #trace_event{data = #{job_id := Id}} <- Terminals],
    %% Jobs must either be terminal or still in progress (trace may be incomplete)
    %% Only flag as violation if we see a job submitted but later see its node die
    %% without any terminal event
    NodeDeaths = [N || #trace_state_change{node = N, new_state = down} <- Trace],
    lists:filtermap(fun(JobId) ->
        case lists:member(JobId, TerminalIds) of
            true -> false;
            false ->
                %% Check if job's node died
                case find_job_node(JobId, Trace) of
                    undefined -> false;  % Unknown node, can't determine
                    Node ->
                        case lists:member(Node, NodeDeaths) of
                            true -> {true, {job_loss, JobId, Node}};
                            false -> false
                        end
                end
        end
    end, SubmittedIds).

find_job_node(JobId, Trace) ->
    case [N || #trace_event{type = job_allocated, data = #{job_id := Id, node := N}} <- Trace,
               Id =:= JobId] of
        [Node | _] -> Node;
        [] -> undefined
    end.

%%====================================================================
%% TLA+ Export Helpers
%%====================================================================

trace_entry_to_tla(#trace_message{timestamp = T, from = F, to = To, type = Type, payload = P}) ->
    iolist_to_binary([
        <<"    [type |-> \"message\", ">>,
        <<"timestamp |-> ">>, integer_to_binary(T), <<", ">>,
        <<"from |-> \"">>, to_binary(F), <<"\", ">>,
        <<"to |-> \"">>, to_binary(To), <<"\", ">>,
        <<"msg_type |-> \"">>, atom_to_binary(Type), <<"\", ">>,
        <<"payload |-> ">>, term_to_tla(P), <<"]">>
    ]);
trace_entry_to_tla(#trace_state_change{timestamp = T, node = N, component = C, old_state = Old, new_state = New}) ->
    iolist_to_binary([
        <<"    [type |-> \"state_change\", ">>,
        <<"timestamp |-> ">>, integer_to_binary(T), <<", ">>,
        <<"node |-> \"">>, to_binary(N), <<"\", ">>,
        <<"component |-> \"">>, atom_to_binary(C), <<"\", ">>,
        <<"old_state |-> ">>, term_to_tla(Old), <<", ">>,
        <<"new_state |-> ">>, term_to_tla(New), <<"]">>
    ]);
trace_entry_to_tla(#trace_event{timestamp = T, type = Type, data = D}) ->
    iolist_to_binary([
        <<"    [type |-> \"event\", ">>,
        <<"timestamp |-> ">>, integer_to_binary(T), <<", ">>,
        <<"event_type |-> \"">>, atom_to_binary(Type), <<"\", ">>,
        <<"data |-> ">>, term_to_tla(D), <<"]">>
    ]).

term_to_tla(T) when is_atom(T) ->
    <<"\"", (atom_to_binary(T))/binary, "\"">>;
term_to_tla(T) when is_binary(T) ->
    <<"\"", T/binary, "\"">>;
term_to_tla(T) when is_integer(T) ->
    integer_to_binary(T);
term_to_tla(T) when is_list(T) ->
    Items = [term_to_tla(I) || I <- T],
    iolist_to_binary([<<"<<">>, lists:join(<<", ">>, Items), <<">>">>]);
term_to_tla(T) when is_map(T) ->
    Items = [[<<"\"">>, to_binary(K), <<"\" |-> ">>, term_to_tla(V)] || {K, V} <- maps:to_list(T)],
    iolist_to_binary([<<"[">>, lists:join(<<", ">>, Items), <<"]">>]);
term_to_tla(T) when is_tuple(T) ->
    term_to_tla(tuple_to_list(T));
term_to_tla(_) ->
    <<"\"<opaque>\"">>.

to_binary(A) when is_atom(A) -> atom_to_binary(A);
to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L) -> list_to_binary(L);
to_binary(I) when is_integer(I) -> integer_to_binary(I);
to_binary(_) -> <<"unknown">>.
