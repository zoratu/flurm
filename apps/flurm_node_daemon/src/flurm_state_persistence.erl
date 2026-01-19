%%%-------------------------------------------------------------------
%%% @doc FLURM State Persistence
%%%
%%% Handles saving and restoring node daemon state across restarts.
%%% This includes:
%%% - Running jobs information
%%% - GPU allocations
%%% - Node configuration state
%%%
%%% State is persisted to disk so the node can recover from crashes
%%% and handle orphaned jobs appropriately on restart.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_state_persistence).

-export([
    save_state/1,
    load_state/0,
    get_state_file/0,
    clear_state/0
]).

-ifdef(TEST).
-export([validate_state/1]).
-endif.

-define(STATE_FILE_ENV, state_file).
-define(DEFAULT_STATE_FILE, "/var/lib/flurm/node_state.dat").

%%====================================================================
%% API
%%====================================================================

%% @doc Get the path to the state file
-spec get_state_file() -> string().
get_state_file() ->
    case application:get_env(flurm_node_daemon, ?STATE_FILE_ENV) of
        {ok, Path} -> Path;
        undefined -> ?DEFAULT_STATE_FILE
    end.

%% @doc Save node state to disk
-spec save_state(map()) -> ok | {error, term()}.
save_state(State) ->
    StateFile = get_state_file(),
    try
        %% Ensure directory exists
        ok = filelib:ensure_dir(StateFile),

        %% Add metadata
        StateWithMeta = State#{
            version => 1,
            saved_at => erlang:system_time(millisecond),
            node => node()
        },

        %% Serialize state
        Binary = term_to_binary(StateWithMeta, [compressed]),

        %% Write to temp file first, then rename (atomic)
        TempFile = StateFile ++ ".tmp",
        case file:write_file(TempFile, Binary) of
            ok ->
                case file:rename(TempFile, StateFile) of
                    ok ->
                        lager:debug("Saved node state to ~s", [StateFile]),
                        ok;
                    {error, Reason} ->
                        lager:error("Failed to rename state file: ~p", [Reason]),
                        file:delete(TempFile),
                        {error, Reason}
                end;
            {error, Reason} ->
                lager:error("Failed to write state file: ~p", [Reason]),
                {error, Reason}
        end
    catch
        Error:Reason2:Stack ->
            lager:error("Failed to save state: ~p:~p~n~p", [Error, Reason2, Stack]),
            {error, {Error, Reason2}}
    end.

%% @doc Load node state from disk
-spec load_state() -> {ok, map()} | {error, term()}.
load_state() ->
    StateFile = get_state_file(),
    case file:read_file(StateFile) of
        {ok, Binary} ->
            try
                State = binary_to_term(Binary),
                %% Validate state
                case validate_state(State) of
                    ok ->
                        lager:info("Loaded node state from ~s (saved at ~p)",
                                  [StateFile, maps:get(saved_at, State, unknown)]),
                        {ok, State};
                    {error, Reason} ->
                        lager:warning("Invalid state file, ignoring: ~p", [Reason]),
                        {error, invalid_state}
                end
            catch
                _:_ ->
                    lager:warning("Corrupted state file ~s, ignoring", [StateFile]),
                    {error, corrupted}
            end;
        {error, enoent} ->
            lager:info("No previous state file found at ~s", [StateFile]),
            {error, not_found};
        {error, Reason} ->
            lager:warning("Failed to read state file ~s: ~p", [StateFile, Reason]),
            {error, Reason}
    end.

%% @doc Clear the saved state file
-spec clear_state() -> ok | {error, term()}.
clear_state() ->
    StateFile = get_state_file(),
    case file:delete(StateFile) of
        ok -> ok;
        {error, enoent} -> ok;  % File doesn't exist, that's fine
        {error, Reason} -> {error, Reason}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% Validate loaded state
validate_state(State) when is_map(State) ->
    case maps:get(version, State, undefined) of
        1 -> ok;
        undefined -> {error, missing_version};
        V -> {error, {unknown_version, V}}
    end;
validate_state(_) ->
    {error, not_a_map}.
