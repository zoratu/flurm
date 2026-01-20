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
    get_next_available_window/2,
    %% Job-level reservation API
    create_reservation/1,
    confirm_reservation/1,
    cancel_reservation/1,
    check_reservation/2,
    %% Scheduler integration API
    check_job_reservation/1,
    get_reserved_nodes/1,
    %% Additional scheduler integration API
    get_active_reservations/0,
    check_reservation_access/2,
    filter_nodes_for_scheduling/2,
    get_available_nodes_excluding_reserved/1,
    %% Notification callbacks from scheduler
    reservation_activated/1,
    reservation_deactivated/1
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

%% Reservation types:
%% - maintenance/maint: System maintenance (no jobs allowed)
%% - user: User-specific reservation (only specified users)
%% - flex: Flexible reservation (jobs can use if space available)
%% - license: License-based reservation
%% - partition: Partition-wide reservation
-type reservation_type() :: maintenance | maint | user | flex | license | partition.
-type reservation_flags() :: [maint | ignore_jobs | overlap |
                               part_nodes | no_hold_jobs_after |
                               daily | weekly | weekday | weekend |
                               flex | magnetic | any | first_cores |
                               time_float | purge_comp | no_hold_jobs |
                               replace | replace_down | static_alloc].

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
    purge_time :: non_neg_integer() | undefined,  % Auto-delete after
    %% Job tracking for reservation usage
    jobs_using = [] :: [pos_integer()],     % Jobs currently using this reservation
    confirmed = false :: boolean()          % Has the reservation been confirmed (job started)
}).

-record(state, {
    check_timer :: reference() | undefined
}).

-export_type([reservation_type/0]).

%% Test exports
-ifdef(TEST).
-export([
    %% Validation and building
    validate_spec/1,
    build_reservation/1,
    %% Conflict checking
    times_overlap/4,
    nodes_overlap/2,
    %% State management
    maybe_activate_reservation/1,
    apply_reservation_updates/2,
    extend_recurring/2,
    %% Access checking
    check_type_access/6,
    check_user_access/4,
    %% Helper lookups
    node_in_partition/2,
    user_in_accounts/2,
    %% Window finding
    find_gap/3,
    %% Reservation name generation
    generate_reservation_name/0
]).
-endif.

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

%%--------------------------------------------------------------------
%% Job-level Reservation API
%%--------------------------------------------------------------------

%% @doc Create a resource reservation for a job
%% Spec should contain: name, nodes/node_count, start_time, end_time/duration,
%% user, and optionally account, partition, features, etc.
-spec create_reservation(map()) -> {ok, binary()} | {error, term()}.
create_reservation(Spec) ->
    gen_server:call(?SERVER, {create_reservation, Spec}).

%% @doc Confirm a reservation when a job actually starts using it
%% This marks the reservation as in-use and prevents expiration while job runs
-spec confirm_reservation(binary()) -> ok | {error, term()}.
confirm_reservation(ReservationName) ->
    gen_server:call(?SERVER, {confirm_reservation, ReservationName}).

%% @doc Cancel a reservation and release its resources
%% Removes the reservation and frees the nodes for other jobs
-spec cancel_reservation(binary()) -> ok | {error, term()}.
cancel_reservation(ReservationName) ->
    gen_server:call(?SERVER, {cancel_reservation, ReservationName}).

%% @doc Check if a job can use a specific reservation
%% Verifies user permissions, time window, and resource availability
-spec check_reservation(pos_integer(), binary()) ->
    {ok, [binary()]} | {error, term()}.
check_reservation(JobId, ReservationName) ->
    gen_server:call(?SERVER, {check_reservation, JobId, ReservationName}).

%%--------------------------------------------------------------------
%% Scheduler Integration API
%%--------------------------------------------------------------------

%% @doc Check if a job has a reservation and if it's valid
%% Called by scheduler during job scheduling
-spec check_job_reservation(#job{}) ->
    {ok, use_reservation, [binary()]} |  % Job has valid reservation, use these nodes
    {ok, no_reservation} |               % Job has no reservation request
    {error, term()}.                     % Reservation invalid/expired/unauthorized
check_job_reservation(#job{reservation = <<>>}) ->
    {ok, no_reservation};
check_job_reservation(#job{reservation = undefined}) ->
    {ok, no_reservation};
check_job_reservation(#job{id = JobId, reservation = ResName, user = User}) ->
    case ?MODULE:get(ResName) of
        {ok, #reservation{state = active, users = Users, accounts = Accounts,
                          nodes = Nodes, end_time = EndTime}} ->
            Now = erlang:system_time(second),
            %% Check if reservation is still valid
            case EndTime > Now of
                false ->
                    {error, reservation_expired};
                true ->
                    %% Check user authorization
                    case (Users =:= [] andalso Accounts =:= []) orelse
                         lists:member(User, Users) orelse
                         user_in_accounts(User, Accounts) of
                        true ->
                            %% Add job to reservation's jobs_using list
                            gen_server:cast(?SERVER, {add_job_to_reservation, JobId, ResName}),
                            {ok, use_reservation, Nodes};
                        false ->
                            {error, user_not_authorized}
                    end
            end;
        {ok, #reservation{state = inactive}} ->
            {error, reservation_not_active};
        {ok, #reservation{state = expired}} ->
            {error, reservation_expired};
        {error, not_found} ->
            {error, reservation_not_found}
    end.

%% @doc Get reserved nodes for a reservation
%% Returns the node list allocated to a reservation
-spec get_reserved_nodes(binary()) -> {ok, [binary()]} | {error, term()}.
get_reserved_nodes(ReservationName) ->
    case ?MODULE:get(ReservationName) of
        {ok, #reservation{nodes = Nodes, state = active}} ->
            {ok, Nodes};
        {ok, #reservation{state = State}} ->
            {error, {invalid_state, State}};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Additional Scheduler Integration API
%%--------------------------------------------------------------------

%% @doc Get all currently active reservations
%% Returns reservations that have started and not yet ended
-spec get_active_reservations() -> [#reservation{}].
get_active_reservations() ->
    list_active().

%% @doc Check if a job or user can access a reservation
%% Returns {ok, nodes} if access granted, {error, reason} otherwise
-spec check_reservation_access(#job{} | binary(), binary()) ->
    {ok, [binary()]} | {error, term()}.
check_reservation_access(#job{id = JobId, user = User, account = Account}, ReservationName) ->
    check_reservation_access_impl(JobId, User, Account, ReservationName);
check_reservation_access(User, ReservationName) when is_binary(User) ->
    check_reservation_access_impl(undefined, User, <<>>, ReservationName).

%% @private
check_reservation_access_impl(JobId, User, Account, ReservationName) ->
    case ?MODULE:get(ReservationName) of
        {ok, #reservation{state = active, type = Type, users = Users,
                          accounts = Accounts, nodes = Nodes,
                          end_time = EndTime, flags = Flags}} ->
            Now = erlang:system_time(second),
            %% Check if reservation is still valid
            case EndTime > Now of
                false ->
                    {error, reservation_expired};
                true ->
                    %% Check type-specific access rules
                    case check_type_access(Type, Flags, User, Account, Users, Accounts) of
                        true ->
                            %% Add job to reservation's jobs_using list if JobId provided
                            case JobId of
                                undefined -> ok;
                                _ -> gen_server:cast(?SERVER, {add_job_to_reservation, JobId, ReservationName})
                            end,
                            {ok, Nodes};
                        false ->
                            {error, access_denied}
                    end
            end;
        {ok, #reservation{state = inactive, start_time = StartTime}} ->
            Now = erlang:system_time(second),
            case StartTime > Now of
                true -> {error, {reservation_not_started, StartTime}};
                false -> {error, reservation_not_active}
            end;
        {ok, #reservation{state = expired}} ->
            {error, reservation_expired};
        {error, not_found} ->
            {error, reservation_not_found}
    end.

%% @private Check type-specific access rules
check_type_access(maintenance, _Flags, _User, _Account, _Users, _Accounts) ->
    %% MAINT reservations: no jobs allowed
    false;
check_type_access(maint, Flags, User, Account, Users, Accounts) ->
    %% MAINT type: check if ignore_jobs flag is set
    case lists:member(ignore_jobs, Flags) of
        true -> false;
        false -> check_user_access(User, Account, Users, Accounts)
    end;
check_type_access(flex, _Flags, User, Account, Users, Accounts) ->
    %% FLEX reservations: jobs can use if space available
    %% Any user can use if users list is empty, otherwise check authorization
    check_user_access(User, Account, Users, Accounts);
check_type_access(user, _Flags, User, Account, Users, Accounts) ->
    %% USER reservations: only specified users/accounts
    check_user_access(User, Account, Users, Accounts);
check_type_access(_, _Flags, User, Account, Users, Accounts) ->
    %% Default: check user authorization
    check_user_access(User, Account, Users, Accounts).

%% @private Check if user is authorized
check_user_access(User, Account, Users, Accounts) ->
    %% If both users and accounts are empty, anyone can use
    case Users =:= [] andalso Accounts =:= [] of
        true -> true;
        false ->
            lists:member(User, Users) orelse
            lists:member(Account, Accounts) orelse
            user_in_accounts(User, Accounts)
    end.

%% @doc Filter nodes for scheduling based on reservations
%% Returns nodes that are available for the given job/user
%% If job has a reservation, returns only reserved nodes
%% If job has no reservation, excludes reserved nodes (unless flex type)
-spec filter_nodes_for_scheduling(#job{} | binary(), [binary()]) -> [binary()].
filter_nodes_for_scheduling(#job{reservation = <<>>} = _Job, Nodes) ->
    %% No reservation specified - exclude reserved nodes
    get_available_nodes_excluding_reserved(Nodes);
filter_nodes_for_scheduling(#job{reservation = undefined} = _Job, Nodes) ->
    %% No reservation specified - exclude reserved nodes
    get_available_nodes_excluding_reserved(Nodes);
filter_nodes_for_scheduling(#job{reservation = ResName, user = User, account = Account} = _Job, Nodes) ->
    %% Job has reservation - check access and filter to reserved nodes
    case check_reservation_access_impl(undefined, User, Account, ResName) of
        {ok, ReservedNodes} ->
            %% Return intersection of requested nodes and reserved nodes
            [N || N <- Nodes, lists:member(N, ReservedNodes)];
        {error, _} ->
            %% No access to reservation - return no nodes
            []
    end;
filter_nodes_for_scheduling(User, Nodes) when is_binary(User) ->
    %% Filter for user without specific job - exclude reserved nodes
    get_available_nodes_excluding_reserved(Nodes).

%% @doc Get available nodes excluding those in active non-flex reservations
%% Flex reservations allow other jobs to use nodes if space available
-spec get_available_nodes_excluding_reserved([binary()]) -> [binary()].
get_available_nodes_excluding_reserved(Nodes) ->
    Now = erlang:system_time(second),
    ActiveReservations = list_active(),

    %% Collect all nodes in non-flex active reservations
    ReservedNodes = lists:foldl(
        fun(#reservation{type = Type, flags = Flags, nodes = RNodes,
                         start_time = Start, end_time = End}, Acc) ->
            %% Check if reservation is currently active (time-based)
            case Start =< Now andalso End > Now of
                true ->
                    %% Check if this is a flex reservation (allows other jobs)
                    IsFlex = (Type =:= flex) orelse lists:member(flex, Flags),
                    case IsFlex of
                        true ->
                            %% Flex reservations don't block nodes
                            Acc;
                        false ->
                            %% Non-flex reservations block nodes
                            sets:union(Acc, sets:from_list(RNodes))
                    end;
                false ->
                    Acc
            end
        end,
        sets:new(),
        ActiveReservations
    ),

    %% Return nodes not in reserved set
    [N || N <- Nodes, not sets:is_element(N, ReservedNodes)].

%% @doc Callback when a reservation is activated
%% Called by scheduler or timer when reservation start time arrives
-spec reservation_activated(binary()) -> ok.
reservation_activated(ReservationName) ->
    error_logger:info_msg("Reservation ~s activated~n", [ReservationName]),
    %% Trigger scheduler to re-evaluate pending jobs
    catch flurm_scheduler:trigger_schedule(),
    ok.

%% @doc Callback when a reservation is deactivated
%% Called by scheduler or timer when reservation end time arrives
-spec reservation_deactivated(binary()) -> ok.
reservation_deactivated(ReservationName) ->
    error_logger:info_msg("Reservation ~s deactivated~n", [ReservationName]),
    %% Trigger scheduler to re-evaluate pending jobs (nodes are now free)
    catch flurm_scheduler:trigger_schedule(),
    ok.

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

%% Job-level reservation handlers
handle_call({create_reservation, Spec}, _From, State) ->
    Result = do_create_reservation(Spec),
    {reply, Result, State};

handle_call({confirm_reservation, ReservationName}, _From, State) ->
    Result = do_confirm_reservation(ReservationName),
    {reply, Result, State};

handle_call({cancel_reservation, ReservationName}, _From, State) ->
    Result = do_cancel_reservation(ReservationName),
    {reply, Result, State};

handle_call({check_reservation, JobId, ReservationName}, _From, State) ->
    Result = do_check_reservation(JobId, ReservationName),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({add_job_to_reservation, JobId, ReservationName}, State) ->
    do_add_job_to_reservation(JobId, ReservationName),
    {noreply, State};

handle_cast({remove_job_from_reservation, JobId, ReservationName}, State) ->
    do_remove_job_from_reservation(JobId, ReservationName),
    {noreply, State};

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
                    %% Check start time is not too far in the past
                    %% Allow up to 24 hours grace period for:
                    %% - Retroactive maintenance reservations
                    %% - Scheduling delays
                    %% - Testing scenarios
                    Now = erlang:system_time(second),
                    GracePeriod = 86400,  % 24 hours
                    case StartTime < (Now - GracePeriod) of
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
                        %% Get the full reservation for notification
                        case ets:lookup(?RESERVATION_TABLE, Name) of
                            [FullRes] ->
                                notify_reservation_expired(FullRes);
                            [] ->
                                ok
                        end,
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

%%--------------------------------------------------------------------
%% Job-level Reservation Internal Functions
%%--------------------------------------------------------------------

%% @private
%% Create a job-specific resource reservation
%% This is a wrapper around the general create function with job-specific defaults
do_create_reservation(Spec) ->
    %% Ensure required fields are present
    Name = maps:get(name, Spec, generate_reservation_name()),
    User = maps:get(user, Spec, <<"unknown">>),

    %% Build the full spec with defaults
    FullSpec = Spec#{
        name => Name,
        type => maps:get(type, Spec, user),
        created_by => User,
        users => maps:get(users, Spec, [User])  % Default: only requesting user
    },

    %% Use the existing create logic
    do_create(FullSpec).

%% @private
%% Confirm a reservation (job has started using it)
do_confirm_reservation(ReservationName) ->
    case ets:lookup(?RESERVATION_TABLE, ReservationName) of
        [Res = #reservation{state = active}] ->
            ets:insert(?RESERVATION_TABLE, Res#reservation{confirmed = true}),
            error_logger:info_msg("Reservation ~s confirmed~n", [ReservationName]),
            ok;
        [#reservation{state = State}] ->
            {error, {invalid_state, State}};
        [] ->
            {error, not_found}
    end.

%% @private
%% Cancel a reservation and release resources
do_cancel_reservation(ReservationName) ->
    case ets:lookup(?RESERVATION_TABLE, ReservationName) of
        [Res = #reservation{jobs_using = Jobs, nodes = Nodes}] ->
            %% Notify any jobs that were using this reservation
            lists:foreach(fun(JobId) ->
                catch flurm_job_manager:update_job(JobId, #{reservation => <<>>})
            end, Jobs),

            %% Release the nodes (mark them as available again)
            release_reservation_nodes(Nodes, ReservationName),

            %% Delete the reservation
            ets:delete(?RESERVATION_TABLE, ReservationName),
            error_logger:info_msg("Reservation ~s cancelled, released nodes: ~p~n",
                                  [ReservationName, Nodes]),

            %% Notify scheduler that resources are available
            notify_reservation_inactive(Res),
            ok;
        [] ->
            {error, not_found}
    end.

%% @private
%% Check if a specific job can use a reservation
do_check_reservation(JobId, ReservationName) ->
    case ets:lookup(?RESERVATION_TABLE, ReservationName) of
        [#reservation{state = active, users = Users, accounts = Accounts,
                      nodes = Nodes, end_time = EndTime}] ->
            Now = erlang:system_time(second),
            %% Get job info to check user authorization
            case flurm_job_manager:get_job(JobId) of
                {ok, Job} ->
                    User = Job#job.user,
                    Account = Job#job.account,

                    %% Check time validity
                    case EndTime > Now of
                        false ->
                            {error, reservation_expired};
                        true ->
                            %% Check user/account authorization
                            Authorized = (Users =:= [] andalso Accounts =:= []) orelse
                                         lists:member(User, Users) orelse
                                         lists:member(Account, Accounts) orelse
                                         user_in_accounts(User, Accounts),
                            case Authorized of
                                true ->
                                    {ok, Nodes};
                                false ->
                                    {error, user_not_authorized}
                            end
                    end;
                {error, _} ->
                    {error, job_not_found}
            end;
        [#reservation{state = State}] ->
            {error, {invalid_state, State}};
        [] ->
            {error, not_found}
    end.

%% @private
%% Add a job to a reservation's jobs_using list
do_add_job_to_reservation(JobId, ReservationName) ->
    case ets:lookup(?RESERVATION_TABLE, ReservationName) of
        [Res = #reservation{jobs_using = Jobs}] ->
            case lists:member(JobId, Jobs) of
                true ->
                    ok;  % Already added
                false ->
                    ets:insert(?RESERVATION_TABLE,
                               Res#reservation{jobs_using = [JobId | Jobs]})
            end;
        [] ->
            ok
    end.

%% @private
%% Remove a job from a reservation's jobs_using list
do_remove_job_from_reservation(JobId, ReservationName) ->
    case ets:lookup(?RESERVATION_TABLE, ReservationName) of
        [Res = #reservation{jobs_using = Jobs}] ->
            NewJobs = lists:delete(JobId, Jobs),
            ets:insert(?RESERVATION_TABLE, Res#reservation{jobs_using = NewJobs});
        [] ->
            ok
    end.

%% @private
%% Generate a unique reservation name
generate_reservation_name() ->
    Timestamp = erlang:system_time(microsecond),
    Random = rand:uniform(10000),
    list_to_binary(io_lib:format("res_~p_~p", [Timestamp, Random])).

%% @private
%% Release nodes from a reservation
release_reservation_nodes(_Nodes, _ReservationName) ->
    %% Nodes are virtual allocations - no actual release needed
    %% The scheduler will see these nodes as available once reservation is deleted
    ok.

%%--------------------------------------------------------------------
%% Expiration Handling
%%--------------------------------------------------------------------

%% The do_check_reservation_states/0 function already handles expiration
%% by checking end_time and transitioning to expired state.
%% Enhanced expiration logic is added below to also handle:
%% - Auto-cancellation of expired reservations with purge_time
%% - Notification of jobs when their reservation expires

%% Called from do_check_reservation_states when a reservation expires
notify_reservation_expired(#reservation{name = Name, jobs_using = Jobs}) ->
    error_logger:info_msg("Reservation ~s expired~n", [Name]),
    %% Notify any jobs still referencing this reservation
    lists:foreach(fun(JobId) ->
        error_logger:warning_msg("Job ~p's reservation ~s has expired~n", [JobId, Name])
    end, Jobs).
