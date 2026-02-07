%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Protocol Codec
%%%
%%% Handles encoding and decoding of PMI-1 wire protocol messages.
%%% PMI-1 uses newline-delimited key-value pairs over sockets.
%%%
%%% Example message formats:
%%%   Request:  cmd=init pmi_version=1 pmi_subversion=1\n
%%%   Response: cmd=response_to_init rc=0 pmi_version=1 pmi_subversion=1\n
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_protocol).

-export([
    decode/1,
    encode/2,
    parse_attrs/1,
    format_attrs/1
]).

%% PMI command types
-type pmi_cmd() :: init | init_ack | get_maxes | maxes
                 | get_appnum | appnum | get_my_kvsname | my_kvsname
                 | barrier_in | barrier_out | put | put_ack
                 | get | get_ack | getbyidx | getbyidx_ack
                 | finalize | finalize_ack | abort | unknown.

-type pmi_attrs() :: #{atom() => binary() | integer()}.
-type pmi_message() :: {pmi_cmd(), pmi_attrs()}.

-export_type([pmi_cmd/0, pmi_attrs/0, pmi_message/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Decode a PMI message from binary
-spec decode(binary()) -> {ok, pmi_message()} | {error, term()}.
decode(<<>>) ->
    {error, empty_message};
decode(Data) ->
    %% Remove trailing newline if present
    Line = case binary:last(Data) of
        $\n -> binary:part(Data, 0, byte_size(Data) - 1);
        _ -> Data
    end,

    %% Split into command and attributes
    Parts = binary:split(Line, <<" ">>, [global]),
    case Parts of
        [] ->
            {error, no_command};
        [First | Rest] ->
            %% First part should be cmd=<command>
            case binary:split(First, <<"=">>) of
                [<<"cmd">>, CmdBin] ->
                    Cmd = parse_command(CmdBin),
                    Attrs = parse_attrs_list(Rest),
                    {ok, {Cmd, Attrs}};
                [<<"mcmd">>, CmdBin] ->
                    %% Multi-command format
                    Cmd = parse_command(CmdBin),
                    Attrs = parse_attrs_list(Rest),
                    {ok, {Cmd, Attrs}};
                _ ->
                    %% Try parsing the whole thing as attrs
                    Attrs = parse_attrs_list(Parts),
                    case maps:get(cmd, Attrs, undefined) of
                        undefined -> {error, {invalid_format, Data}};
                        CmdVal -> {ok, {parse_command(CmdVal), maps:remove(cmd, Attrs)}}
                    end
            end
    end.

%% @doc Encode a PMI response
-spec encode(pmi_cmd(), pmi_attrs()) -> binary().
encode(Cmd, Attrs) ->
    CmdBin = command_to_binary(Cmd),
    AttrsBin = format_attrs(Attrs),
    case AttrsBin of
        <<>> -> <<"cmd=", CmdBin/binary, "\n">>;
        _ -> <<"cmd=", CmdBin/binary, " ", AttrsBin/binary, "\n">>
    end.

%% @doc Parse attributes from a binary string "key1=val1 key2=val2"
-spec parse_attrs(binary()) -> pmi_attrs().
parse_attrs(Bin) ->
    Parts = binary:split(Bin, <<" ">>, [global]),
    parse_attrs_list(Parts).

%% @doc Format attributes map to binary string
-spec format_attrs(pmi_attrs()) -> binary().
format_attrs(Attrs) when map_size(Attrs) == 0 ->
    <<>>;
format_attrs(Attrs) ->
    Parts = maps:fold(fun(Key, Value, Acc) ->
        KeyBin = atom_to_binary(Key, utf8),
        ValBin = value_to_binary(Value),
        [<<KeyBin/binary, "=", ValBin/binary>> | Acc]
    end, [], Attrs),
    iolist_to_binary(lists:join(<<" ">>, lists:reverse(Parts))).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

parse_attrs_list(Parts) ->
    lists:foldl(fun(Part, Acc) ->
        case binary:split(Part, <<"=">>) of
            [Key, Value] ->
                KeyAtom = safe_binary_to_atom(Key),
                maps:put(KeyAtom, parse_value(Value), Acc);
            [_SinglePart] ->
                %% Skip parts without =
                Acc
        end
    end, #{}, Parts).

parse_value(<<"">>) -> <<>>;
parse_value(Bin) ->
    %% Try to parse as integer
    try binary_to_integer(Bin)
    catch _:_ -> Bin
    end.

value_to_binary(V) when is_binary(V) -> V;
value_to_binary(V) when is_integer(V) -> integer_to_binary(V);
value_to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8);
value_to_binary(V) when is_list(V) -> list_to_binary(V).

safe_binary_to_atom(Bin) ->
    try binary_to_existing_atom(Bin, utf8)
    catch _:_ -> binary_to_atom(Bin, utf8)
    end.

parse_command(<<"init">>) -> init;
parse_command(<<"response_to_init">>) -> init_ack;
parse_command(<<"init_ack">>) -> init_ack;
parse_command(<<"get_maxes">>) -> get_maxes;
parse_command(<<"maxes">>) -> maxes;
parse_command(<<"get_appnum">>) -> get_appnum;
parse_command(<<"appnum">>) -> appnum;
parse_command(<<"get_my_kvsname">>) -> get_my_kvsname;
parse_command(<<"my_kvsname">>) -> my_kvsname;
parse_command(<<"barrier_in">>) -> barrier_in;
parse_command(<<"barrier_out">>) -> barrier_out;
parse_command(<<"put">>) -> put;
parse_command(<<"put_ack">>) -> put_ack;
parse_command(<<"get">>) -> get;
parse_command(<<"get_ack">>) -> get_ack;
parse_command(<<"getbyidx">>) -> getbyidx;
parse_command(<<"getbyidx_ack">>) -> getbyidx_ack;
parse_command(<<"finalize">>) -> finalize;
parse_command(<<"finalize_ack">>) -> finalize_ack;
parse_command(<<"abort">>) -> abort;
parse_command(_) -> unknown.

command_to_binary(init) -> <<"init">>;
command_to_binary(init_ack) -> <<"response_to_init">>;
command_to_binary(get_maxes) -> <<"get_maxes">>;
command_to_binary(maxes) -> <<"maxes">>;
command_to_binary(get_appnum) -> <<"get_appnum">>;
command_to_binary(appnum) -> <<"appnum">>;
command_to_binary(get_my_kvsname) -> <<"get_my_kvsname">>;
command_to_binary(my_kvsname) -> <<"my_kvsname">>;
command_to_binary(barrier_in) -> <<"barrier_in">>;
command_to_binary(barrier_out) -> <<"barrier_out">>;
command_to_binary(put) -> <<"put">>;
command_to_binary(put_ack) -> <<"put_ack">>;
command_to_binary(get) -> <<"get">>;
command_to_binary(get_ack) -> <<"get_ack">>;
command_to_binary(getbyidx) -> <<"getbyidx">>;
command_to_binary(getbyidx_ack) -> <<"getbyidx_ack">>;
command_to_binary(finalize) -> <<"finalize">>;
command_to_binary(finalize_ack) -> <<"finalize_ack">>;
command_to_binary(abort) -> <<"abort">>;
command_to_binary(Cmd) -> atom_to_binary(Cmd, utf8).
