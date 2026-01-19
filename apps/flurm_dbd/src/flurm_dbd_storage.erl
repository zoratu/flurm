%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Storage Backend
%%%
%%% Provides persistence for accounting data. Supports multiple
%%% backends: ETS (in-memory), Mnesia (distributed), or external
%%% databases via callbacks.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_storage).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    store/3,
    fetch/2,
    delete/2,
    list/1,
    list/2,
    sync/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-export([init_mnesia_tables/0]).
-endif.

-record(state, {
    backend :: ets | mnesia,
    tables :: map()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Store a value
-spec store(atom(), term(), term()) -> ok | {error, term()}.
store(Table, Key, Value) ->
    gen_server:call(?MODULE, {store, Table, Key, Value}).

%% @doc Fetch a value
-spec fetch(atom(), term()) -> {ok, term()} | {error, not_found}.
fetch(Table, Key) ->
    gen_server:call(?MODULE, {fetch, Table, Key}).

%% @doc Delete a value
-spec delete(atom(), term()) -> ok.
delete(Table, Key) ->
    gen_server:call(?MODULE, {delete, Table, Key}).

%% @doc List all values in a table
-spec list(atom()) -> [term()].
list(Table) ->
    gen_server:call(?MODULE, {list, Table}).

%% @doc List values matching a pattern
-spec list(atom(), term()) -> [term()].
list(Table, Pattern) ->
    gen_server:call(?MODULE, {list, Table, Pattern}).

%% @doc Sync data to disk
-spec sync() -> ok.
sync() ->
    gen_server:call(?MODULE, sync).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    Backend = application:get_env(flurm_dbd, storage_backend, ets),
    lager:info("DBD Storage starting with backend: ~p", [Backend]),

    Tables = case Backend of
        ets ->
            #{
                jobs => ets:new(dbd_jobs, [set, protected]),
                steps => ets:new(dbd_steps, [bag, protected]),
                events => ets:new(dbd_events, [ordered_set, protected]),
                accounts => ets:new(dbd_accounts, [set, protected]),
                users => ets:new(dbd_users, [set, protected]),
                associations => ets:new(dbd_associations, [set, protected]),
                qos => ets:new(dbd_qos, [set, protected]),
                usage => ets:new(dbd_usage, [set, protected])
            };
        mnesia ->
            %% Initialize Mnesia tables
            init_mnesia_tables(),
            #{}
    end,

    {ok, #state{backend = Backend, tables = Tables}}.

handle_call({store, Table, Key, Value}, _From, #state{backend = ets, tables = Tables} = State) ->
    case maps:get(Table, Tables, undefined) of
        undefined ->
            {reply, {error, unknown_table}, State};
        Tab ->
            ets:insert(Tab, {Key, Value}),
            {reply, ok, State}
    end;

handle_call({store, Table, Key, Value}, _From, #state{backend = mnesia} = State) ->
    Result = mnesia:dirty_write(Table, {Table, Key, Value}),
    {reply, Result, State};

handle_call({fetch, Table, Key}, _From, #state{backend = ets, tables = Tables} = State) ->
    case maps:get(Table, Tables, undefined) of
        undefined ->
            {reply, {error, unknown_table}, State};
        Tab ->
            case ets:lookup(Tab, Key) of
                [{_, Value}] -> {reply, {ok, Value}, State};
                [] -> {reply, {error, not_found}, State}
            end
    end;

handle_call({fetch, Table, Key}, _From, #state{backend = mnesia} = State) ->
    case mnesia:dirty_read(Table, Key) of
        [{_, _, Value}] -> {reply, {ok, Value}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call({delete, Table, Key}, _From, #state{backend = ets, tables = Tables} = State) ->
    case maps:get(Table, Tables, undefined) of
        undefined ->
            {reply, ok, State};
        Tab ->
            ets:delete(Tab, Key),
            {reply, ok, State}
    end;

handle_call({delete, Table, Key}, _From, #state{backend = mnesia} = State) ->
    mnesia:dirty_delete(Table, Key),
    {reply, ok, State};

handle_call({list, Table}, _From, #state{backend = ets, tables = Tables} = State) ->
    case maps:get(Table, Tables, undefined) of
        undefined ->
            {reply, [], State};
        Tab ->
            Values = [V || {_, V} <- ets:tab2list(Tab)],
            {reply, Values, State}
    end;

handle_call({list, Table}, _From, #state{backend = mnesia} = State) ->
    Values = mnesia:dirty_all_keys(Table),
    {reply, Values, State};

handle_call({list, Table, Pattern}, _From, #state{backend = ets, tables = Tables} = State) ->
    case maps:get(Table, Tables, undefined) of
        undefined ->
            {reply, [], State};
        Tab ->
            Values = ets:match_object(Tab, Pattern),
            {reply, [V || {_, V} <- Values], State}
    end;

handle_call({list, Table, Pattern}, _From, #state{backend = mnesia} = State) ->
    Values = mnesia:dirty_match_object({Table, Pattern, '_'}),
    {reply, [V || {_, _, V} <- Values], State};

handle_call(sync, _From, #state{backend = ets} = State) ->
    %% ETS is in-memory only, no sync needed
    {reply, ok, State};

handle_call(sync, _From, #state{backend = mnesia} = State) ->
    mnesia:sync_log(),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

init_mnesia_tables() ->
    Tables = [
        {dbd_jobs, [{attributes, [key, value]}, {disc_copies, [node()]}]},
        {dbd_steps, [{attributes, [key, value]}, {disc_copies, [node()]}, {type, bag}]},
        {dbd_events, [{attributes, [key, value]}, {disc_copies, [node()]}, {type, ordered_set}]},
        {dbd_accounts, [{attributes, [key, value]}, {disc_copies, [node()]}]},
        {dbd_users, [{attributes, [key, value]}, {disc_copies, [node()]}]},
        {dbd_associations, [{attributes, [key, value]}, {disc_copies, [node()]}]},
        {dbd_qos, [{attributes, [key, value]}, {disc_copies, [node()]}]},
        {dbd_usage, [{attributes, [key, value]}, {disc_copies, [node()]}]}
    ],
    lists:foreach(fun({Name, Opts}) ->
        case mnesia:create_table(Name, Opts) of
            {atomic, ok} -> ok;
            {aborted, {already_exists, _}} -> ok;
            {aborted, Reason} ->
                lager:error("Failed to create Mnesia table ~p: ~p", [Name, Reason])
        end
    end, Tables).
