#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/*/ebin

%% update_checkpoint.escript - Update FLURM checkpoint file
%% Usage: ./scripts/update_checkpoint.escript <task_id> <new_status>
%% Example: ./scripts/update_checkpoint.escript create_umbrella completed

-mode(compile).

main([TaskId, NewStatus]) ->
    CheckpointPath = filename:join([script_dir(), "..", ".claude", "checkpoint.json"]),
    case file:read_file(CheckpointPath) of
        {ok, JsonBin} ->
            case decode_json(JsonBin) of
                {ok, Checkpoint} ->
                    UpdatedCheckpoint = update_task(Checkpoint, TaskId, NewStatus),
                    FinalCheckpoint = update_timestamp(UpdatedCheckpoint),
                    case write_checkpoint(CheckpointPath, FinalCheckpoint) of
                        ok ->
                            io:format("Updated task '~s' to status '~s'~n", [TaskId, NewStatus]),
                            halt(0);
                        {error, WriteErr} ->
                            io:format(standard_error, "Error writing checkpoint: ~p~n", [WriteErr]),
                            halt(1)
                    end;
                {error, DecodeErr} ->
                    io:format(standard_error, "Error decoding JSON: ~p~n", [DecodeErr]),
                    halt(1)
            end;
        {error, ReadErr} ->
            io:format(standard_error, "Error reading checkpoint file: ~p~n", [ReadErr]),
            halt(1)
    end;

main(["--help"]) ->
    usage(),
    halt(0);

main(_) ->
    usage(),
    halt(1).

usage() ->
    io:format("Usage: update_checkpoint.escript <task_id> <new_status>~n"),
    io:format("~n"),
    io:format("Arguments:~n"),
    io:format("  task_id     - The ID of the task to update~n"),
    io:format("  new_status  - New status: pending | in_progress | completed | blocked~n"),
    io:format("~n"),
    io:format("Example:~n"),
    io:format("  ./scripts/update_checkpoint.escript create_umbrella completed~n").

script_dir() ->
    ScriptName = escript:script_name(),
    filename:dirname(filename:absname(ScriptName)).

%% Simple JSON decoder (no external deps)
decode_json(Bin) when is_binary(Bin) ->
    try
        {ok, Tokens, _} = json_tokenize(binary_to_list(Bin)),
        {ok, Value, _} = json_parse_value(Tokens),
        {ok, Value}
    catch
        _:Reason -> {error, Reason}
    end.

json_tokenize(Str) ->
    json_tokenize(Str, []).

json_tokenize([], Acc) ->
    {ok, lists:reverse(Acc), []};
json_tokenize([C|Rest], Acc) when C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r ->
    json_tokenize(Rest, Acc);
json_tokenize([$:|Rest], Acc) ->
    json_tokenize(Rest, [colon|Acc]);
json_tokenize([$,|Rest], Acc) ->
    json_tokenize(Rest, [comma|Acc]);
json_tokenize([$[|Rest], Acc) ->
    json_tokenize(Rest, [array_start|Acc]);
json_tokenize([$]|Rest], Acc) ->
    json_tokenize(Rest, [array_end|Acc]);
json_tokenize([${|Rest], Acc) ->
    json_tokenize(Rest, [object_start|Acc]);
json_tokenize([$}|Rest], Acc) ->
    json_tokenize(Rest, [object_end|Acc]);
json_tokenize([$"|Rest], Acc) ->
    {String, Rest2} = json_parse_string(Rest, []),
    json_tokenize(Rest2, [{string, String}|Acc]);
json_tokenize([$t,$r,$u,$e|Rest], Acc) ->
    json_tokenize(Rest, [true|Acc]);
json_tokenize([$f,$a,$l,$s,$e|Rest], Acc) ->
    json_tokenize(Rest, [false|Acc]);
json_tokenize([$n,$u,$l,$l|Rest], Acc) ->
    json_tokenize(Rest, [null|Acc]);
json_tokenize([C|_] = Str, Acc) when C =:= $- orelse (C >= $0 andalso C =< $9) ->
    {Num, Rest} = json_parse_number(Str),
    json_tokenize(Rest, [{number, Num}|Acc]).

json_parse_string([$"|Rest], Acc) ->
    {lists:reverse(Acc), Rest};
json_parse_string([$\\, $"|Rest], Acc) ->
    json_parse_string(Rest, [$"|Acc]);
json_parse_string([$\\, $\\|Rest], Acc) ->
    json_parse_string(Rest, [$\\|Acc]);
json_parse_string([$\\, $/|Rest], Acc) ->
    json_parse_string(Rest, [$/|Acc]);
json_parse_string([$\\, $n|Rest], Acc) ->
    json_parse_string(Rest, [$\n|Acc]);
json_parse_string([$\\, $r|Rest], Acc) ->
    json_parse_string(Rest, [$\r|Acc]);
json_parse_string([$\\, $t|Rest], Acc) ->
    json_parse_string(Rest, [$\t|Acc]);
json_parse_string([C|Rest], Acc) ->
    json_parse_string(Rest, [C|Acc]).

json_parse_number(Str) ->
    json_parse_number(Str, []).

json_parse_number([C|Rest], Acc) when C =:= $- orelse (C >= $0 andalso C =< $9) orelse C =:= $. orelse C =:= $e orelse C =:= $E orelse C =:= $+ ->
    json_parse_number(Rest, [C|Acc]);
json_parse_number(Rest, Acc) ->
    NumStr = lists:reverse(Acc),
    Num = case lists:member($., NumStr) orelse lists:member($e, NumStr) orelse lists:member($E, NumStr) of
        true -> list_to_float(NumStr);
        false -> list_to_integer(NumStr)
    end,
    {Num, Rest}.

json_parse_value([object_start|Rest]) ->
    json_parse_object(Rest, #{});
json_parse_value([array_start|Rest]) ->
    json_parse_array(Rest, []);
json_parse_value([{string, S}|Rest]) ->
    {ok, list_to_binary(S), Rest};
json_parse_value([{number, N}|Rest]) ->
    {ok, N, Rest};
json_parse_value([true|Rest]) ->
    {ok, true, Rest};
json_parse_value([false|Rest]) ->
    {ok, false, Rest};
json_parse_value([null|Rest]) ->
    {ok, null, Rest}.

json_parse_object([object_end|Rest], Acc) ->
    {ok, Acc, Rest};
json_parse_object([{string, Key}, colon|Rest], Acc) ->
    {ok, Value, Rest2} = json_parse_value(Rest),
    case Rest2 of
        [comma|Rest3] ->
            json_parse_object(Rest3, maps:put(list_to_binary(Key), Value, Acc));
        [object_end|Rest3] ->
            {ok, maps:put(list_to_binary(Key), Value, Acc), Rest3}
    end.

json_parse_array([array_end|Rest], Acc) ->
    {ok, lists:reverse(Acc), Rest};
json_parse_array(Tokens, Acc) ->
    {ok, Value, Rest} = json_parse_value(Tokens),
    case Rest of
        [comma|Rest2] ->
            json_parse_array(Rest2, [Value|Acc]);
        [array_end|Rest2] ->
            {ok, lists:reverse([Value|Acc]), Rest2}
    end.

%% JSON encoder
encode_json(Map) when is_map(Map) ->
    Pairs = maps:fold(fun(K, V, Acc) ->
        KeyStr = encode_json_string(K),
        ValStr = encode_json(V),
        [[KeyStr, ": ", ValStr]|Acc]
    end, [], Map),
    ["{", lists:join(", ", lists:reverse(Pairs)), "}"];
encode_json(List) when is_list(List) ->
    Items = [encode_json(Item) || Item <- List],
    ["[", lists:join(", ", Items), "]"];
encode_json(Bin) when is_binary(Bin) ->
    encode_json_string(Bin);
encode_json(true) -> "true";
encode_json(false) -> "false";
encode_json(null) -> "null";
encode_json(Num) when is_integer(Num) ->
    integer_to_list(Num);
encode_json(Num) when is_float(Num) ->
    io_lib:format("~p", [Num]).

encode_json_string(Bin) when is_binary(Bin) ->
    encode_json_string(binary_to_list(Bin));
encode_json_string(Str) when is_list(Str) ->
    [$", escape_json_string(Str), $"].

escape_json_string([]) -> [];
escape_json_string([$"|Rest]) -> [$\\, $"|escape_json_string(Rest)];
escape_json_string([$\\|Rest]) -> [$\\, $\\|escape_json_string(Rest)];
escape_json_string([$\n|Rest]) -> [$\\, $n|escape_json_string(Rest)];
escape_json_string([$\r|Rest]) -> [$\\, $r|escape_json_string(Rest)];
escape_json_string([$\t|Rest]) -> [$\\, $t|escape_json_string(Rest)];
escape_json_string([C|Rest]) -> [C|escape_json_string(Rest)].

%% Update task in checkpoint
update_task(Checkpoint, TaskId, NewStatus) ->
    TaskIdBin = list_to_binary(TaskId),
    NewStatusBin = list_to_binary(NewStatus),
    Phases = maps:get(<<"phases">>, Checkpoint),
    UpdatedPhases = maps:map(fun(_PhaseName, Phase) ->
        Tasks = maps:get(<<"tasks">>, Phase, []),
        UpdatedTasks = lists:map(fun(Task) ->
            case maps:get(<<"id">>, Task) of
                TaskIdBin ->
                    maps:put(<<"status">>, NewStatusBin, Task);
                _ ->
                    Task
            end
        end, Tasks),
        maps:put(<<"tasks">>, UpdatedTasks, Phase)
    end, Phases),
    maps:put(<<"phases">>, UpdatedPhases, Checkpoint).

%% Update last_updated timestamp
update_timestamp(Checkpoint) ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:universal_time(),
    Timestamp = io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ",
                              [Y, Mo, D, H, Mi, S]),
    maps:put(<<"last_updated">>, list_to_binary(lists:flatten(Timestamp)), Checkpoint).

%% Write checkpoint file with pretty printing
write_checkpoint(Path, Checkpoint) ->
    Json = pretty_print_json(Checkpoint, 0),
    file:write_file(Path, Json).

%% Pretty print JSON with indentation
pretty_print_json(Map, Indent) when is_map(Map) ->
    Pairs = maps:to_list(Map),
    case Pairs of
        [] -> "{}";
        _ ->
            IndentStr = lists:duplicate(Indent + 2, $ ),
            CloseIndent = lists:duplicate(Indent, $ ),
            FormattedPairs = lists:map(fun({K, V}) ->
                KeyStr = encode_json_string(K),
                ValStr = pretty_print_json(V, Indent + 2),
                [IndentStr, KeyStr, ": ", ValStr]
            end, Pairs),
            ["{\n", lists:join(",\n", FormattedPairs), "\n", CloseIndent, "}"]
    end;
pretty_print_json(List, Indent) when is_list(List) ->
    case List of
        [] -> "[]";
        _ ->
            IndentStr = lists:duplicate(Indent + 2, $ ),
            CloseIndent = lists:duplicate(Indent, $ ),
            FormattedItems = lists:map(fun(Item) ->
                [IndentStr, pretty_print_json(Item, Indent + 2)]
            end, List),
            ["[\n", lists:join(",\n", FormattedItems), "\n", CloseIndent, "]"]
    end;
pretty_print_json(Value, _Indent) ->
    encode_json(Value).
