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
    decode/1,
    verify/1,
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

%% @doc Decode a MUNGE credential, extracting uid, gid, and payload.
%% Calls the unmunge CLI tool to decode the credential and extract UID, GID, and payload.
%% Returns {ok, Info} with a map containing uid, gid, and payload fields,
%% or {error, Reason} if decoding fails.
%%
%% Error reasons include:
%%   - munge_unavailable: MUNGE is not installed on this system
%%   - unmunge_failed: unmunge command returned empty output
%%   - credential_expired: credential has exceeded its TTL
%%   - credential_replayed: credential has already been used (replay attack)
%%   - credential_rewound: credential timestamp is in the future
%%   - {credential_invalid, Status}: credential format is invalid
-spec decode(Credential :: binary()) ->
    {ok, #{uid := non_neg_integer(), gid := non_neg_integer(), payload := binary()}} |
    {error, term()}.
decode(Credential) when is_binary(Credential) ->
    case is_available() of
        false ->
            {error, munge_unavailable};
        true ->
            decode_with_unmunge(Credential)
    end.

%% @doc Internal function to decode using unmunge command.
-spec decode_with_unmunge(binary()) ->
    {ok, #{uid := non_neg_integer(), gid := non_neg_integer(), payload := binary()}} |
    {error, term()}.
decode_with_unmunge(Credential) ->
    %% Use unmunge command to decode credential
    %% The verbose mode (default) provides UID/GID information in the output
    %% Write credential to temp file to avoid shell escaping issues with os:cmd
    TempFile = "/tmp/flurm_munge_" ++ integer_to_list(erlang:unique_integer([positive])),
    try
        ok = file:write_file(TempFile, Credential),
        Cmd = "unmunge < " ++ TempFile ++ " 2>&1",
        Result = os:cmd(Cmd),
        file:delete(TempFile),
        case Result of
            [] ->
                {error, unmunge_failed};
            _ ->
                parse_unmunge_output(Result)
        end
    catch
        error:Reason ->
            file:delete(TempFile),
            {error, {unmunge_error, Reason}}
    end.

%% @doc Verify a MUNGE credential is valid (not expired, not replayed).
%% This function validates the credential without returning the extracted data.
%% Use decode/1 if you need access to the uid, gid, and payload.
%%
%% Returns ok if the credential is valid, or {error, Reason} if invalid.
%% See decode/1 for a list of possible error reasons.
-spec verify(Credential :: binary()) -> ok | {error, term()}.
verify(Credential) when is_binary(Credential) ->
    case decode(Credential) of
        {ok, _Info} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Parse the output from unmunge command.
%% unmunge outputs something like:
%%   STATUS:           Success (0)
%%   ENCODE_HOST:      hostname (1.2.3.4)
%%   ENCODE_TIME:      2024-01-01 12:00:00 -0500 (1704132000)
%%   DECODE_TIME:      2024-01-01 12:00:01 -0500 (1704132001)
%%   TTL:              300
%%   CIPHER:           aes128 (4)
%%   MAC:              sha256 (5)
%%   ZIP:              none (0)
%%   UID:              1000 (username)
%%   GID:              1000 (groupname)
%%   LENGTH:           0
%%
%%   PAYLOAD:          (empty)
%%
%% Status codes from MUNGE:
%%   0 = Success
%%   15 = Expired credential
%%   16 = Rewound (timestamp in future)
%%   17 = Replayed (credential already used)
-spec parse_unmunge_output(string()) -> {ok, #{uid := non_neg_integer(), gid := non_neg_integer(), payload := binary()}} | {error, term()}.
parse_unmunge_output(Output) ->
    Lines = string:split(Output, "\n", all),
    case extract_status(Lines) of
        {ok, success} ->
            UID = extract_field(Lines, "UID"),
            GID = extract_field(Lines, "GID"),
            Payload = extract_payload(Lines),
            %% Return with required keys (using :=) as documented in spec
            {ok, #{uid => UID, gid => GID, payload => Payload}};
        {ok, expired} ->
            {error, credential_expired};
        {ok, rewound} ->
            {error, credential_rewound};
        {ok, replayed} ->
            {error, credential_replayed};
        {ok, {invalid, StatusStr}} ->
            {error, {credential_invalid, StatusStr}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Extract status from unmunge output.
%% Parses the STATUS line from unmunge output and returns the decoded status.
%% MUNGE status codes:
%%   0 = Success
%%   8 = Invalid credential format
%%   15 = Expired credential (TTL exceeded)
%%   16 = Rewound (encode time is in the future)
%%   17 = Replayed (credential has been used before)
-spec extract_status([string()]) -> {ok, atom()} | {ok, {invalid, string()}} | {error, term()}.
extract_status([]) ->
    {error, invalid_unmunge_output};
extract_status([Line | Rest]) ->
    case string:prefix(string:trim(Line, leading), "STATUS:") of
        nomatch ->
            extract_status(Rest);
        StatusPart ->
            StatusStr = string:trim(StatusPart),
            parse_status_string(StatusStr)
    end.

%% @doc Parse the status string from unmunge output.
-spec parse_status_string(string()) -> {ok, atom()} | {ok, {invalid, string()}}.
parse_status_string(StatusStr) ->
    %% Check for various status conditions in order of likelihood
    case string:find(StatusStr, "Success") of
        nomatch ->
            case string:find(StatusStr, "Expired") of
                nomatch ->
                    case string:find(StatusStr, "Rewound") of
                        nomatch ->
                            case string:find(StatusStr, "Replayed") of
                                nomatch ->
                                    %% Unknown or invalid status
                                    {ok, {invalid, StatusStr}};
                                _ ->
                                    {ok, replayed}
                            end;
                        _ ->
                            {ok, rewound}
                    end;
                _ ->
                    {ok, expired}
            end;
        _ ->
            {ok, success}
    end.

%% @doc Extract a numeric field (like UID or GID) from unmunge output.
-spec extract_field([string()], string()) -> non_neg_integer().
extract_field([], _FieldName) ->
    0;
extract_field([Line | Rest], FieldName) ->
    Prefix = FieldName ++ ":",
    case string:prefix(string:trim(Line, leading), Prefix) of
        nomatch ->
            extract_field(Rest, FieldName);
        ValuePart ->
            %% Value looks like "1000 (username)" - extract just the number
            Trimmed = string:trim(ValuePart),
            case string:split(Trimmed, " ") of
                [NumStr | _] ->
                    case string:to_integer(NumStr) of
                        {Int, _Rest} when is_integer(Int) -> Int;
                        _ -> 0
                    end;
                _ ->
                    0
            end
    end.

%% @doc Extract payload from unmunge output (text after "LENGTH:" line).
-spec extract_payload([string()]) -> binary().
extract_payload(Lines) ->
    extract_payload(Lines, false, []).

extract_payload([], _AfterLength, Acc) ->
    list_to_binary(lists:flatten(lists:reverse(Acc)));
extract_payload([Line | Rest], false, Acc) ->
    %% Look for LENGTH: line, payload comes after blank line
    case string:prefix(string:trim(Line, leading), "LENGTH:") of
        nomatch ->
            extract_payload(Rest, false, Acc);
        _ ->
            %% Found LENGTH, skip to after blank line
            extract_payload(Rest, true, Acc)
    end;
extract_payload(["" | Rest], true, Acc) ->
    %% Skip blank line after LENGTH
    extract_payload(Rest, true, Acc);
extract_payload([Line | Rest], true, Acc) ->
    %% After LENGTH line, look for PAYLOAD: line
    case string:prefix(string:trim(Line, leading), "PAYLOAD:") of
        nomatch ->
            %% Regular payload content line
            extract_payload(Rest, true, [Line | Acc]);
        PayloadPart ->
            %% PAYLOAD: line, extract content after colon
            Content = string:trim(PayloadPart),
            case Content of
                "(empty)" -> <<>>;
                _ -> list_to_binary(Content)
            end
    end.
