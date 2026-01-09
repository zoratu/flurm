%%%-------------------------------------------------------------------
%%% @doc FLURM slurm.conf Parser
%%%
%%% Parses SLURM-style configuration files. Supports:
%%% - Key=Value pairs
%%% - Comments (# style)
%%% - Includes (Include=path)
%%% - Multi-line values (trailing \)
%%% - Node definitions (NodeName=...)
%%% - Partition definitions (PartitionName=...)
%%%
%%% Example slurm.conf:
%%%   ClusterName=mycluster
%%%   SlurmctldPort=6817
%%%   NodeName=node[001-100] CPUs=64 RealMemory=128000
%%%   PartitionName=batch Nodes=node[001-050] Default=YES
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_slurm).

-export([
    parse_file/1,
    parse_string/1,
    parse_line/1,
    expand_hostlist/1
]).

-include("flurm_config.hrl").

-type parse_result() :: {ok, #{atom() => term()}} | {error, term()}.
-type line_result() :: {key_value, atom(), term()} |
                       {node_def, map()} |
                       {partition_def, map()} |
                       comment | empty | {error, term()}.

%%====================================================================
%% API
%%====================================================================

%% @doc Parse a slurm.conf file
-spec parse_file(string()) -> parse_result().
parse_file(Filename) ->
    case file:read_file(Filename) of
        {ok, Content} ->
            BaseDir = filename:dirname(Filename),
            parse_content(Content, BaseDir, #{});
        {error, Reason} ->
            {error, {file_error, Filename, Reason}}
    end.

%% @doc Parse a slurm.conf string
-spec parse_string(binary() | string()) -> parse_result().
parse_string(Content) when is_list(Content) ->
    parse_string(list_to_binary(Content));
parse_string(Content) when is_binary(Content) ->
    parse_content(Content, ".", #{}).

%% @doc Parse a single configuration line
-spec parse_line(binary()) -> line_result().
parse_line(Line) ->
    Trimmed = string:trim(binary_to_list(Line)),
    parse_trimmed_line(Trimmed).

%% @doc Expand a SLURM hostlist pattern
%% e.g., "node[001-003,005]" -> ["node001", "node002", "node003", "node005"]
-spec expand_hostlist(binary() | string()) -> [binary()].
expand_hostlist(Pattern) when is_binary(Pattern) ->
    expand_hostlist(binary_to_list(Pattern));
expand_hostlist(Pattern) when is_list(Pattern) ->
    case string:find(Pattern, "[") of
        nomatch ->
            [list_to_binary(Pattern)];
        _ ->
            expand_hostlist_pattern(Pattern)
    end.

%%====================================================================
%% Internal functions - Parsing
%%====================================================================

parse_content(Content, BaseDir, Acc) ->
    Lines = binary:split(Content, [<<"\n">>, <<"\r\n">>], [global]),
    parse_lines(Lines, BaseDir, Acc, []).

parse_lines([], _BaseDir, Acc, _Continuation) ->
    %% Process accumulated nodes and partitions
    FinalConfig = finalize_config(Acc),
    {ok, FinalConfig};
parse_lines([Line | Rest], BaseDir, Acc, Continuation) ->
    %% Handle line continuation
    FullLine = case Continuation of
        [] -> Line;
        _ -> iolist_to_binary([Continuation, Line])
    end,

    %% Check for line continuation (handle empty lines first)
    TrimmedLine = string:trim(FullLine, trailing),
    case byte_size(TrimmedLine) of
        0 ->
            %% Empty line - skip to process
            parse_lines(Rest, BaseDir, Acc, []);
        _ ->
            case binary:last(TrimmedLine) of
                $\\ ->
                    %% Remove trailing backslash and continue
                    WithoutBackslash = binary:part(TrimmedLine, 0, byte_size(TrimmedLine) - 1),
                    parse_lines(Rest, BaseDir, Acc, [WithoutBackslash]);
                _ ->
            %% Process complete line
            case parse_line(FullLine) of
                {key_value, include, Path} ->
                    %% Handle Include directive
                    FullPath = filename:join(BaseDir, binary_to_list(Path)),
                    case parse_file(FullPath) of
                        {ok, IncludedConfig} ->
                            NewAcc = maps:merge(Acc, IncludedConfig),
                            parse_lines(Rest, BaseDir, NewAcc, []);
                        {error, Reason} ->
                            {error, {include_error, Path, Reason}}
                    end;
                {key_value, Key, Value} ->
                    NewAcc = Acc#{Key => Value},
                    parse_lines(Rest, BaseDir, NewAcc, []);
                {node_def, NodeDef} ->
                    Nodes = maps:get(nodes, Acc, []),
                    NewAcc = Acc#{nodes => [NodeDef | Nodes]},
                    parse_lines(Rest, BaseDir, NewAcc, []);
                {partition_def, PartDef} ->
                    Partitions = maps:get(partitions, Acc, []),
                    NewAcc = Acc#{partitions => [PartDef | Partitions]},
                    parse_lines(Rest, BaseDir, NewAcc, []);
                comment ->
                    parse_lines(Rest, BaseDir, Acc, []);
                empty ->
                    parse_lines(Rest, BaseDir, Acc, []);
                {error, Reason} ->
                    {error, {parse_error, Line, Reason}}
            end
        end
    end.

parse_trimmed_line([]) ->
    empty;
parse_trimmed_line("#" ++ _) ->
    comment;
parse_trimmed_line(Line) ->
    case string:split(Line, "=", leading) of
        [KeyStr, ValueStr] ->
            Key = normalize_key(string:trim(KeyStr)),
            Value = parse_value(string:trim(ValueStr)),
            categorize_key_value(Key, Value, Line);
        _ ->
            {error, {invalid_line, Line}}
    end.

categorize_key_value(nodename, _Value, Line) ->
    %% NodeName line - parse as node definition
    parse_node_definition(Line);
categorize_key_value(partitionname, _Value, Line) ->
    %% PartitionName line - parse as partition definition
    parse_partition_definition(Line);
categorize_key_value(Key, Value, _Line) ->
    {key_value, Key, Value}.

%% Parse a NodeName definition line
%% NodeName=node[001-010] CPUs=64 RealMemory=128000 State=IDLE
parse_node_definition(Line) ->
    Parts = string:split(Line, " ", all),
    NodeDef = lists:foldl(fun(Part, Acc) ->
        case string:split(Part, "=", leading) of
            [K, V] ->
                Key = normalize_key(string:trim(K)),
                Value = parse_value(string:trim(V)),
                Acc#{Key => Value};
            _ ->
                Acc
        end
    end, #{}, Parts),
    {node_def, NodeDef}.

%% Parse a PartitionName definition line
%% PartitionName=batch Nodes=node[001-050] Default=YES MaxTime=INFINITE
parse_partition_definition(Line) ->
    Parts = string:split(Line, " ", all),
    PartDef = lists:foldl(fun(Part, Acc) ->
        case string:split(Part, "=", leading) of
            [K, V] ->
                Key = normalize_key(string:trim(K)),
                Value = parse_value(string:trim(V)),
                Acc#{Key => Value};
            _ ->
                Acc
        end
    end, #{}, Parts),
    {partition_def, PartDef}.

normalize_key(KeyStr) ->
    %% Convert to lowercase atom for consistency
    list_to_atom(string:lowercase(KeyStr)).

parse_value("YES") -> true;
parse_value("Yes") -> true;
parse_value("yes") -> true;
parse_value("NO") -> false;
parse_value("No") -> false;
parse_value("no") -> false;
parse_value("TRUE") -> true;
parse_value("FALSE") -> false;
parse_value("INFINITE") -> infinity;
parse_value("UNLIMITED") -> unlimited;
parse_value(Value) ->
    %% Try to parse as number
    case catch list_to_integer(Value) of
        Int when is_integer(Int) ->
            Int;
        _ ->
            %% Check for memory suffixes (K, M, G, T)
            case parse_memory_value(Value) of
                {ok, Bytes} -> Bytes;
                error ->
                    %% Check for time format (D-HH:MM:SS)
                    case parse_time_value(Value) of
                        {ok, Seconds} -> Seconds;
                        error -> list_to_binary(Value)
                    end
            end
    end.

%% Parse memory values like "128G", "64000M", "1T"
parse_memory_value(Value) ->
    case re:run(Value, "^([0-9]+)([KMGT]?)$", [{capture, all_but_first, list}]) of
        {match, [NumStr, ""]} ->
            {ok, list_to_integer(NumStr)};
        {match, [NumStr, "K"]} ->
            {ok, list_to_integer(NumStr) * 1024};
        {match, [NumStr, "M"]} ->
            {ok, list_to_integer(NumStr) * 1024 * 1024};
        {match, [NumStr, "G"]} ->
            {ok, list_to_integer(NumStr) * 1024 * 1024 * 1024};
        {match, [NumStr, "T"]} ->
            {ok, list_to_integer(NumStr) * 1024 * 1024 * 1024 * 1024};
        _ ->
            error
    end.

%% Parse time values like "1-00:00:00" (1 day), "2:00:00" (2 hours)
parse_time_value(Value) ->
    case string:split(Value, "-", leading) of
        [DaysStr, Rest] ->
            %% D-HH:MM:SS format
            case {catch list_to_integer(DaysStr), parse_hms(Rest)} of
                {Days, {ok, HMS}} when is_integer(Days) ->
                    {ok, Days * 86400 + HMS};
                _ ->
                    error
            end;
        [TimeOnly] ->
            %% HH:MM:SS or MM:SS format
            parse_hms(TimeOnly)
    end.

parse_hms(Value) ->
    Parts = string:split(Value, ":", all),
    case Parts of
        [HStr, MStr, SStr] ->
            try
                H = list_to_integer(HStr),
                M = list_to_integer(MStr),
                S = list_to_integer(SStr),
                {ok, H * 3600 + M * 60 + S}
            catch
                _:_ -> error
            end;
        [MStr, SStr] ->
            try
                M = list_to_integer(MStr),
                S = list_to_integer(SStr),
                {ok, M * 60 + S}
            catch
                _:_ -> error
            end;
        _ ->
            error
    end.

finalize_config(Config) ->
    %% Reverse node and partition lists (they were accumulated in reverse)
    Config1 = case maps:get(nodes, Config, undefined) of
        undefined -> Config;
        Nodes -> Config#{nodes => lists:reverse(Nodes)}
    end,
    case maps:get(partitions, Config1, undefined) of
        undefined -> Config1;
        Parts -> Config1#{partitions => lists:reverse(Parts)}
    end.

%%====================================================================
%% Internal functions - Hostlist Expansion
%%====================================================================

%% Expand patterns like "node[001-003,005,010-012]"
expand_hostlist_pattern(Pattern) ->
    case re:run(Pattern, "^(.*)\\[([^\\]]+)\\](.*)$", [{capture, all_but_first, list}]) of
        {match, [Prefix, RangeSpec, Suffix]} ->
            Ranges = string:split(RangeSpec, ",", all),
            Expanded = lists:flatmap(fun(Range) ->
                expand_range(Prefix, Range, Suffix)
            end, Ranges),
            Expanded;
        nomatch ->
            [list_to_binary(Pattern)]
    end.

expand_range(Prefix, Range, Suffix) ->
    case string:split(Range, "-", leading) of
        [Start, End] ->
            %% Range like "001-010"
            StartNum = list_to_integer(Start),
            EndNum = list_to_integer(End),
            Width = length(Start),
            [format_host(Prefix, N, Width, Suffix) || N <- lists:seq(StartNum, EndNum)];
        [Single] ->
            %% Single value like "005"
            [list_to_binary(Prefix ++ Single ++ Suffix)]
    end.

format_host(Prefix, Num, Width, Suffix) ->
    NumStr = integer_to_list(Num),
    Padded = string:pad(NumStr, Width, leading, $0),
    list_to_binary(Prefix ++ Padded ++ Suffix).
