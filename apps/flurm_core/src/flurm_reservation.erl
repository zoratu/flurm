%%%-------------------------------------------------------------------
%%% @doc FLURM Resource Reservation System
%%%
%%% Implements SLURM-compatible advance reservations for guaranteed
%%% resource access.
%%%
%%% Reservation types:
%%% - maintenance: System maintenance windows
%%% - user: User-requested reservations
%%% - license: License-based reservations
%%% - partition: Partition-wide reservations
%%%
%%% Features:
%%% - Start and end time specifications
%%% - Node/TRES specifications
%%% - User/account restrictions
%%% - Recurring reservations
%%% - Floating reservations (start when resources available)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_reservation).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    create/1,
    get/1,
    list/0,
    list_active/0,
    update/2,
    delete/1,
    is_node_reserved/2,
    get_reservations_for_node/1,
    get_reservations_for_user/1,
    can_use_reservation/2,
    check_reservation_conflict/1,
    activate_reservation/1,
    deactivate_reservation/1,
    get_next_available_window/2
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
-define(RESERVATION_TABLE, flurm_reservations).

-type reservation_type() :: maintenance | user | license | partition.
-type reservation_flags() :: [maint | ignore_jobs | overlap |
                               part_nodes | no_hold_jobs_after |
                               daily | weekly | weekday | weekend |
                               flex | magnetic].

-record(reservation, {
    name :: binary(),
    type :: reservation_type(),
    start_time :: non_neg_integer(),        % Unix timestamp
    end_time :: non_neg_integer(),
    duration :: non_neg_integer(),          % Seconds (for recurring)
    nodes :: [binary()],                    % Specific node list
    node_count :: non_neg_integer(),        % Or number of nodes
    partition :: binary() | undefined,
    features :: [binary()],
    users :: [binary()],                    % Allowed users
    accounts :: [binary()],                 % Allowed accounts
    flags :: reservation_flags(),
    tres :: map(),                          % TRES requirements
    state :: inactive | active | expired,
    created_by :: binary(),
    created_time :: non_neg_integer(),
    purge_time :: non_neg_integer() | undefined  % Auto-delete after
}).

-record(state, {
    check_timer :: reference() | undefined
}).

-export_type([reservation_type/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Create a new reservation
-spec create(map()) -> {ok, binary()} | {error, term()}.
create(Spec) ->
    gen_server:call(?SERVER, {create, Spec}).

%% @doc Get a reservation by name
-spec get(binary()) -> {ok, #reservation{}} | {error, not_found}.
get(Name) ->
    case ets:lookup(?RESERVATION_TABLE, Name) of
        [Res] -> {ok, Res};
        [] -> {error, not_found}
    end.

%% @doc List all reservations
-spec list() -> [#reservation{}].
list() ->
    ets:tab2list(?RESERVATION_TABLE).

%% @doc List active reservations
-spec list_active() -> [#reservation{}].
list_active() ->
    Now = erlang:system_time(second),
    ets:select(?RESERVATION_TABLE, [{
        #reservation{state = active, start_time = '$1', end_time = '$2', _ = '_'},
        [{'=<', '$1', Now}, {'>=', '$2', Now}],
        ['$_']
    }]).

%% @doc Update a reservation
-spec update(binary(), map()) -> ok | {error, term()}.
update(Name, Updates) ->
    gen_server:call(?SERVER, {update, Name, Updates}).

%% @doc Delete a reservation
-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    gen_server:call(?SERVER, {delete, Name}).

%% @doc Check if a node is reserved at a given time
-spec is_node_reserved(binary(), non_neg_integer()) -> boolean().
is_node_reserved(NodeName, Time) ->
    Reservations = get_reservations_for_node(NodeName),
    lists:any(fun(#reservation{start_time = Start, end_time = End, state = State}) ->
        State =:= active andalso Time >= Start andalso Time < End
    end, Reservations).

%% @doc Get all reservations that include a node
-spec get_reservations_for_node(binary()) -> [#reservation{}].
get_reservations_for_node(NodeName) ->
    AllReservations = list(),
    lists:filter(fun(#reservation{nodes = Nodes, partition = Part}) ->
        lists:member(NodeName, Nodes) orelse
        (Part =/= undefined andalso node_in_partition(NodeName, Part))
    end, AllReservations).

%% @doc Get reservations accessible by a user
-spec get_reservations_for_user(binary()) -> [#reservation{}].
get_reservations_for_user(User) ->
    AllReservations = list(),
    lists:filter(fun(#reservation{users = Users, accounts = Accounts}) ->
        (Users =:= [] orelse lists:member(User, Users)) orelse
        user_in_accounts(User, Accounts)
    end, AllReservations).

%% @doc Check if a user can use a reservation
-spec can_use_reservation(binary(), binary()) -> boolean().
can_use_reservation(User, ReservationName) ->
    case ?MODULE:get(ReservationName) of
        {ok, #reservation{users = Users, accounts = Accounts, state = active}} ->
            (Users =:= [] andalso Accounts =:= []) orelse
            lists:member(User, Users) orelse
            user_in_accounts(User, Accounts);
        _ ->
            false
    end.

%% @doc Check if a new reservation would conflict with existing ones
-spec check_reservation_conflict(map()) -> ok | {error, term()}.
check_reservation_conflict(Spec) ->
    gen_server:call(?SERVER, {check_conflict, Spec}).

%% @doc Activate a reservation
-spec activate_reservation(binary()) -> ok | {error, term()}.
activate_reservation(Name) ->
    gen_server:call(?SERVER, {activate, Name}).

%% @doc Deactivate a reservation
-spec deactivate_reservation(binary()) -> ok | {error, term()}.
deactivate_reservation(Name) ->
    gen_server:call(?SERVER, {deactivate, Name}).

%% @doc Get the next available time window for resources
-spec get_next_available_window(map(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
get_next_available_window(ResourceRequest, Duration) ->
    gen_server:call(?SERVER, {next_window, ResourceRequest, Duration}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(?RESERVATION_TABLE, [
        named_table, public, set,
        {keypos, #reservation.name}
    ]),

    %% Start periodic check for reservation state changes
    Timer = erlang:send_after(60000, self(), check_reservations),

    {ok, #state{check_timer = Timer}}.

handle_call({create, Spec}, _From, State) ->
    Result = do_create(Spec),
    {reply, Result, State};

handle_call({update, Name, Updates}, _From, State) ->
    Result = do_update(Name, Updates),
    {reply, Result, State};

handle_call({delete, Name}, _From, State) ->
    Result = do_delete(Name),
    {reply, Result, State};

handle_call({check_conflict, Spec}, _From, State) ->
    Result = do_check_conflict(Spec),
    {reply, Result, State};

handle_call({activate, Name}, _From, State) ->
    Result = do_set_state(Name, active),
    {reply, Result, State};

handle_call({deactivate, Name}, _From, State) ->
    Result = do_set_state(Name, inactive),
    {reply, Result, State};

handle_call({next_window, ResourceRequest, Duration}, _From, State) ->
    Result = do_find_next_window(ResourceRequest, Duration),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_reservations, State) ->
    do_check_reservation_states(),
    Timer = erlang:send_after(60000, self(), check_reservations),
    {noreply, State#state{check_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

do_create(Spec) ->
    Name = maps:get(name, Spec),

    %% Check for existing reservation
    case ets:lookup(?RESERVATION_TABLE, Name) of
        [_] ->
            {error, already_exists};
        [] ->
            %% Validate the specification
            case validate_spec(Spec) of
                ok ->
                    Reservation = build_reservation(Spec),
                    ets:insert(?RESERVATION_TABLE, Reservation),
                    maybe_activate_reservation(Reservation),
                    {ok, Name};
                Error ->
                    Error
            end
    end.

validate_spec(Spec) ->
    StartTime = maps:get(start_time, Spec),
    EndTime = maps:get(end_time, Spec, undefined),
    Duration = maps:get(duration, Spec, undefined),
    Nodes = maps:get(nodes, Spec, []),
    NodeCount = maps:get(node_count, Spec, 0),

    %% Must have either end_time or duration
    case EndTime =:= undefined andalso Duration =:= undefined of
        true ->
            {error, missing_end_time_or_duration};
        false ->
            %% Must have either nodes or node_count
            case Nodes =:= [] andalso NodeCount =:= 0 of
                true ->
                    {error, missing_node_specification};
                false ->
                    %% Check start time is in the future
                    Now = erlang:system_time(second),
                    case StartTime < Now of
                        true ->
                            {error, start_time_in_past};
                        false ->
                            ok
                    end
            end
    end.

build_reservation(Spec) ->
    Now = erlang:system_time(second),
    StartTime = maps:get(start_time, Spec),
    Duration = maps:get(duration, Spec, 0),
    EndTime = maps:get(end_time, Spec, StartTime + Duration),

    #reservation{
        name = maps:get(name, Spec),
        type = maps:get(type, Spec, user),
        start_time = StartTime,
        end_time = EndTime,
        duration = Duration,
        nodes = maps:get(nodes, Spec, []),
        node_count = maps:get(node_count, Spec, 0),
        partition = maps:get(partition, Spec, undefined),
        features = maps:get(features, Spec, []),
        users = maps:get(users, Spec, []),
        accounts = maps:get(accounts, Spec, []),
        flags = maps:get(flags, Spec, []),
        tres = maps:get(tres, Spec, #{}),
        state = inactive,
        created_by = maps:get(created_by, Spec, <<"admin">>),
        created_time = Now,
        purge_time = maps:get(purge_time, Spec, undefined)
    }.

maybe_activate_reservation(#reservation{name = Name, start_time = Start}) ->
    Now = erlang:system_time(second),
    case Start =< Now of
        true -> do_set_state(Name, active);
        false -> ok
    end.

do_update(Name, Updates) ->
    case ets:lookup(?RESERVATION_TABLE, Name) of
        [Res] ->
            UpdatedRes = apply_reservation_updates(Res, Updates),
            ets:insert(?RESERVATION_TABLE, UpdatedRes),
            ok;
        [] ->
            {error, not_found}
    end.

apply_reservation_updates(Res, Updates) ->
    maps:fold(fun(Key, Value, Acc) ->
        case Key of
            start_time -> Acc#reservation{start_time = Value};
            end_time -> Acc#reservation{end_time = Value};
            duration -> Acc#reservation{duration = Value};
            nodes -> Acc#reservation{nodes = Value};
            node_count -> Acc#reservation{node_count = Value};
            partition -> Acc#reservation{partition = Value};
            features -> Acc#reservation{features = Value};
            users -> Acc#reservation{users = Value};
            accounts -> Acc#reservation{accounts = Value};
            flags -> Acc#reservation{flags = Value};
            tres -> Acc#reservation{tres = Value};
            _ -> Acc
        end
    end, Res, Updates).

do_delete(Name) ->
    case ets:lookup(?RESERVATION_TABLE, Name) of
        [#reservation{state = active}] ->
            %% Deactivate first
            do_set_state(Name, inactive),
            ets:delete(?RESERVATION_TABLE, Name),
            ok;
        [_] ->
            ets:delete(?RESERVATION_TABLE, Name),
            ok;
        [] ->
            {error, not_found}
    end.

do_set_state(Name, NewState) ->
    case ets:lookup(?RESERVATION_TABLE, Name) of
        [Res] ->
            ets:insert(?RESERVATION_TABLE, Res#reservation{state = NewState}),
            case NewState of
                active ->
                    notify_reservation_active(Res);
                inactive ->
                    notify_reservation_inactive(Res);
                expired ->
                    ok
            end,
            ok;
        [] ->
            {error, not_found}
    end.

do_check_conflict(Spec) ->
    StartTime = maps:get(start_time, Spec),
    EndTime = maps:get(end_time, Spec, StartTime + maps:get(duration, Spec, 0)),
    Nodes = maps:get(nodes, Spec, []),

    %% Get all reservations that overlap in time
    AllReservations = list(),
    Conflicts = lists:filter(fun(#reservation{start_time = S, end_time = E,
                                               nodes = RNodes, state = State}) ->
        State =/= expired andalso
        times_overlap(StartTime, EndTime, S, E) andalso
        nodes_overlap(Nodes, RNodes)
    end, AllReservations),

    case Conflicts of
        [] -> ok;
        _ -> {error, {conflicts, [R#reservation.name || R <- Conflicts]}}
    end.

times_overlap(S1, E1, S2, E2) ->
    not (E1 =< S2 orelse S1 >= E2).

nodes_overlap([], _) -> false;
nodes_overlap(_, []) -> false;
nodes_overlap(Nodes1, Nodes2) ->
    lists:any(fun(N) -> lists:member(N, Nodes2) end, Nodes1).

do_check_reservation_states() ->
    Now = erlang:system_time(second),
    AllReservations = list(),

    lists:foreach(fun(#reservation{name = Name, state = State,
                                    start_time = Start, end_time = End,
                                    purge_time = Purge, flags = Flags}) ->
        %% Check if should activate
        case State =:= inactive andalso Start =< Now andalso End > Now of
            true -> do_set_state(Name, active);
            false -> ok
        end,

        %% Check if should expire
        case State =:= active andalso End =< Now of
            true ->
                case lists:member(daily, Flags) orelse
                     lists:member(weekly, Flags) of
                    true ->
                        %% Recurring reservation - extend it
                        extend_recurring(Name, Flags);
                    false ->
                        do_set_state(Name, expired)
                end;
            false ->
                ok
        end,

        %% Check if should purge
        case State =:= expired andalso Purge =/= undefined andalso Purge =< Now of
            true -> ets:delete(?RESERVATION_TABLE, Name);
            false -> ok
        end
    end, AllReservations).

extend_recurring(Name, Flags) ->
    case ets:lookup(?RESERVATION_TABLE, Name) of
        [Res = #reservation{start_time = Start, end_time = End, duration = Duration}] ->
            Interval = case lists:member(daily, Flags) of
                true -> 86400;  % 24 hours
                false ->
                    case lists:member(weekly, Flags) of
                        true -> 604800;  % 7 days
                        false -> Duration
                    end
            end,
            NewStart = Start + Interval,
            NewEnd = End + Interval,
            ets:insert(?RESERVATION_TABLE, Res#reservation{
                start_time = NewStart,
                end_time = NewEnd,
                state = inactive
            });
        [] ->
            ok
    end.

do_find_next_window(ResourceRequest, Duration) ->
    Now = erlang:system_time(second),
    Nodes = maps:get(nodes, ResourceRequest, []),

    %% Get all reservations for the requested nodes
    RelevantReservations = lists:flatten([
        get_reservations_for_node(N) || N <- Nodes
    ]),

    %% Sort by start time
    Sorted = lists:sort(fun(A, B) ->
        A#reservation.start_time =< B#reservation.start_time
    end, RelevantReservations),

    %% Find first gap that fits the duration
    find_gap(Now, Sorted, Duration).

find_gap(StartFrom, [], _Duration) ->
    {ok, StartFrom};
find_gap(StartFrom, [#reservation{start_time = ResStart, end_time = ResEnd} | Rest],
         Duration) ->
    case ResStart > StartFrom + Duration of
        true ->
            %% Gap found before this reservation
            {ok, StartFrom};
        false ->
            %% Try after this reservation
            find_gap(max(StartFrom, ResEnd), Rest, Duration)
    end.

node_in_partition(NodeName, PartitionName) ->
    case catch flurm_partition_registry:get_partition(PartitionName) of
        {ok, Partition} ->
            lists:member(NodeName, Partition#partition.nodes);
        _ ->
            false
    end.

user_in_accounts(User, Accounts) ->
    case catch flurm_account_manager:get_user_accounts(User) of
        {ok, UserAccounts} ->
            lists:any(fun(A) -> lists:member(A, Accounts) end, UserAccounts);
        _ ->
            false
    end.

notify_reservation_active(#reservation{name = Name, nodes = Nodes}) ->
    error_logger:info_msg("Reservation ~s activated on nodes: ~p~n", [Name, Nodes]),
    catch flurm_scheduler:reservation_activated(Name).

notify_reservation_inactive(#reservation{name = Name}) ->
    error_logger:info_msg("Reservation ~s deactivated~n", [Name]),
    catch flurm_scheduler:reservation_deactivated(Name).
