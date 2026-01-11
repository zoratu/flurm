%%%-------------------------------------------------------------------
%%% @doc FLURM MUNGE Authentication Helper
%%%
%%% Provides MUNGE credential generation by calling the munge command.
%%% Used for signing responses sent to SLURM clients.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_munge).

-export([
    encode/0,
    encode/1,
    is_available/0
]).

%% @doc Check if munge is available on this system.
-spec is_available() -> boolean().
is_available() ->
    case os:find_executable("munge") of
        false -> false;
        _ -> true
    end.

%% @doc Generate a MUNGE credential with empty payload.
-spec encode() -> {ok, binary()} | {error, term()}.
encode() ->
    encode(<<>>).

%% @doc Generate a MUNGE credential with the given payload.
-spec encode(binary()) -> {ok, binary()} | {error, term()}.
encode(Payload) when is_binary(Payload) ->
    %% Use munge command to generate credential
    %% -n = no output (credential only to stdout)
    Cmd = case Payload of
        <<>> ->
            "echo -n '' | munge";
        _ ->
            %% Base64 encode the payload to avoid shell issues
            B64 = base64:encode(Payload),
            "echo -n '" ++ binary_to_list(B64) ++ "' | base64 -d | munge"
    end,
    case os:cmd(Cmd) of
        [] ->
            {error, munge_failed};
        Result when is_list(Result) ->
            %% Remove trailing newline if present
            Credential = string:trim(Result, trailing, "\n"),
            {ok, list_to_binary(Credential)}
    end.
