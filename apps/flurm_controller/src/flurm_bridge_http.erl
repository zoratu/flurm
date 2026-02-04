%%%-------------------------------------------------------------------
%%% @doc FLURM Bridge HTTP Handler - REST API for Bridge Management
%%%
%%% This module implements a Cowboy REST handler providing HTTP API
%%% endpoints for managing the FLURM bridge and federated clusters.
%%%
%%% Endpoints:
%%% - GET    /api/v1/bridge/status           - Get bridge status
%%% - GET    /api/v1/bridge/mode             - Get current mode
%%% - PUT    /api/v1/bridge/mode             - Set migration mode
%%% - GET    /api/v1/bridge/clusters         - List federated clusters
%%% - POST   /api/v1/bridge/clusters         - Add cluster
%%% - DELETE /api/v1/bridge/clusters/:name   - Remove cluster
%%%
%%% Authentication is via JWT tokens (see flurm_jwt module).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_bridge_http).

-export([init/2]).
-export([
    allowed_methods/2,
    content_types_provided/2,
    content_types_accepted/2,
    delete_resource/2,
    is_authorized/2,
    resource_exists/2,
    to_json/2,
    from_json/2
]).

%% For routing setup
-export([routes/0]).

%% For testing
-ifdef(TEST).
-export([
    parse_path/1,
    route_request/3,
    encode_response/1,
    decode_request/1,
    binary_to_mode/1
]).
-endif.

-define(API_PREFIX, "/api/v1/bridge").

%%%===================================================================
%%% Cowboy REST callbacks
%%%===================================================================

init(Req, State) ->
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    Path = cowboy_req:path(Req),
    Methods = get_allowed_methods(Path),
    {Methods, Req, State}.

content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, to_json}
    ], Req, State}.

content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, from_json}
    ], Req, State}.

is_authorized(Req, State) ->
    %% Check for valid JWT token in Authorization header
    case cowboy_req:header(<<"authorization">>, Req) of
        undefined ->
            %% Allow requests without auth if auth is disabled
            case application:get_env(flurm_controller, bridge_api_auth, enabled) of
                disabled ->
                    {true, Req, State};
                _ ->
                    {{false, <<"Bearer realm=\"flurm\"">>}, Req, State}
            end;
        <<"Bearer ", Token/binary>> ->
            case flurm_jwt:verify(Token) of
                {ok, Claims} ->
                    {true, Req, State#{claims => Claims}};
                {error, _Reason} ->
                    {{false, <<"Bearer realm=\"flurm\", error=\"invalid_token\"">>}, Req, State}
            end;
        _ ->
            {{false, <<"Bearer realm=\"flurm\"">>}, Req, State}
    end.

resource_exists(Req, State) ->
    Path = cowboy_req:path(Req),
    Method = cowboy_req:method(Req),
    Exists = check_resource_exists(Path, Method),
    {Exists, Req, State}.

to_json(Req, State) ->
    Path = cowboy_req:path(Req),
    Method = cowboy_req:method(Req),
    Response = route_request(Method, Path, Req),
    Body = encode_response(Response),
    {Body, Req, State}.

from_json(Req, State) ->
    Path = cowboy_req:path(Req),
    Method = cowboy_req:method(Req),
    {ok, ReqBody, Req2} = cowboy_req:read_body(Req),
    Data = decode_request(ReqBody),
    Response = route_request(Method, Path, {Req2, Data}),
    Body = encode_response(Response),
    Req3 = cowboy_req:set_resp_body(Body, Req2),
    {true, Req3, State}.

delete_resource(Req, State) ->
    Path = cowboy_req:path(Req),
    Response = route_request(<<"DELETE">>, Path, Req),
    Body = encode_response(Response),
    Req2 = cowboy_req:set_resp_body(Body, Req),
    {true, Req2, State}.

%%%===================================================================
%%% Internal Functions - Routing
%%%===================================================================

get_allowed_methods(Path) ->
    case parse_path(Path) of
        {status, _} -> [<<"GET">>, <<"OPTIONS">>];
        {mode, _} -> [<<"GET">>, <<"PUT">>, <<"OPTIONS">>];
        {clusters, undefined} -> [<<"GET">>, <<"POST">>, <<"OPTIONS">>];
        {clusters, _Name} -> [<<"GET">>, <<"DELETE">>, <<"OPTIONS">>];
        _ -> [<<"GET">>, <<"OPTIONS">>]
    end.

check_resource_exists(Path, Method) ->
    case parse_path(Path) of
        {status, _} -> true;
        {mode, _} -> true;
        {clusters, undefined} -> true;
        {clusters, Name} when Name =/= undefined ->
            %% Check if cluster exists for DELETE/GET
            case Method of
                <<"DELETE">> ->
                    case flurm_federation:get_cluster_status(Name) of
                        {ok, _} -> true;
                        {error, not_found} -> false
                    end;
                <<"GET">> ->
                    case flurm_federation:get_cluster_status(Name) of
                        {ok, _} -> true;
                        {error, not_found} -> false
                    end;
                _ ->
                    true
            end;
        _ -> false
    end.

parse_path(<<?API_PREFIX, "/status", _/binary>>) ->
    {status, undefined};
parse_path(<<?API_PREFIX, "/mode", _/binary>>) ->
    {mode, undefined};
parse_path(<<?API_PREFIX, "/clusters/", Rest/binary>>) ->
    %% Extract cluster name, removing any trailing slash or path
    Name = case binary:split(Rest, <<"/">>) of
        [ClusterName | _] -> ClusterName;
        _ -> Rest
    end,
    {clusters, Name};
parse_path(<<?API_PREFIX, "/clusters", _/binary>>) ->
    {clusters, undefined};
parse_path(_) ->
    {unknown, undefined}.

route_request(<<"GET">>, Path, _Req) ->
    case parse_path(Path) of
        {status, _} ->
            handle_get_status();
        {mode, _} ->
            handle_get_mode();
        {clusters, undefined} ->
            handle_list_clusters();
        {clusters, Name} when Name =/= undefined ->
            handle_get_cluster(Name);
        _ ->
            {error, not_found}
    end;

route_request(<<"PUT">>, Path, {_Req, Data}) ->
    case parse_path(Path) of
        {mode, _} ->
            handle_set_mode(Data);
        _ ->
            {error, not_found}
    end;

route_request(<<"POST">>, Path, {_Req, Data}) ->
    case parse_path(Path) of
        {clusters, undefined} ->
            handle_add_cluster(Data);
        _ ->
            {error, not_found}
    end;

route_request(<<"DELETE">>, Path, _Req) ->
    case parse_path(Path) of
        {clusters, Name} when Name =/= undefined ->
            handle_remove_cluster(Name);
        _ ->
            {error, not_found}
    end;

route_request(_, _, _) ->
    {error, method_not_allowed}.

%%%===================================================================
%%% Internal Functions - Handlers
%%%===================================================================

handle_get_status() ->
    %% Get bridge/federation status
    try
        FedStats = flurm_federation:get_federation_stats(),
        Resources = flurm_federation:get_federation_resources(),
        IsFederated = flurm_federation:is_federated(),
        LocalCluster = flurm_federation:get_local_cluster(),
        Mode = get_bridge_mode(),

        Status = #{
            mode => Mode,
            is_federated => IsFederated,
            local_cluster => LocalCluster,
            federation_stats => FedStats,
            resources => Resources
        },
        {ok, Status}
    catch
        _:Reason ->
            {error, #{reason => format_error(Reason)}}
    end.

handle_get_mode() ->
    Mode = get_bridge_mode(),
    {ok, #{mode => Mode}}.

handle_set_mode(Data) ->
    case maps:get(<<"mode">>, Data, undefined) of
        undefined ->
            {error, #{reason => <<"missing 'mode' field">>}};
        ModeBin ->
            Mode = binary_to_mode(ModeBin),
            case Mode of
                invalid ->
                    {error, #{reason => <<"invalid mode, must be: shadow, active, primary, or standalone">>}};
                _ ->
                    case set_bridge_mode(Mode) of
                        ok ->
                            {ok, #{mode => Mode, status => <<"updated">>}};
                        {error, Reason} ->
                            {error, #{reason => format_error(Reason)}}
                    end
            end
    end.

handle_list_clusters() ->
    Clusters = flurm_federation:list_clusters(),
    {ok, #{clusters => Clusters, count => length(Clusters)}}.

handle_get_cluster(Name) ->
    case flurm_federation:get_cluster_status(Name) of
        {ok, ClusterInfo} ->
            {ok, ClusterInfo};
        {error, not_found} ->
            {error, #{reason => <<"cluster not found">>}}
    end.

handle_add_cluster(Data) ->
    Name = maps:get(<<"name">>, Data, undefined),
    Host = maps:get(<<"host">>, Data, undefined),
    Port = maps:get(<<"port">>, Data, 6817),
    Auth = maps:get(<<"auth">>, Data, #{}),
    Weight = maps:get(<<"weight">>, Data, 1),
    Features = maps:get(<<"features">>, Data, []),
    Partitions = maps:get(<<"partitions">>, Data, []),

    case {Name, Host} of
        {undefined, _} ->
            {error, #{reason => <<"missing 'name' field">>}};
        {_, undefined} ->
            {error, #{reason => <<"missing 'host' field">>}};
        {_, _} ->
            Config = #{
                host => Host,
                port => Port,
                auth => Auth,
                weight => Weight,
                features => Features,
                partitions => Partitions
            },
            case flurm_federation:add_cluster(Name, Config) of
                ok ->
                    {ok, #{name => Name, status => <<"added">>}};
                {error, Reason} ->
                    {error, #{reason => format_error(Reason)}}
            end
    end.

handle_remove_cluster(Name) ->
    case flurm_federation:remove_cluster(Name) of
        ok ->
            {ok, #{name => Name, status => <<"removed">>}};
        {error, not_found} ->
            {error, #{reason => <<"cluster not found">>}};
        {error, cannot_remove_local} ->
            {error, #{reason => <<"cannot remove local cluster">>}};
        {error, Reason} ->
            {error, #{reason => format_error(Reason)}}
    end.

%%%===================================================================
%%% Internal Functions - Mode Management
%%%===================================================================

%% Bridge modes:
%% - shadow: FLURM observes but does not act (read-only)
%% - active: FLURM handles new jobs, existing on SLURM continue
%% - primary: FLURM is the primary scheduler
%% - standalone: FLURM operates independently (no bridge)

get_bridge_mode() ->
    application:get_env(flurm_controller, bridge_mode, standalone).

set_bridge_mode(Mode) when Mode =:= shadow; Mode =:= active;
                           Mode =:= primary; Mode =:= standalone ->
    application:set_env(flurm_controller, bridge_mode, Mode),
    ok;
set_bridge_mode(_) ->
    {error, invalid_mode}.

binary_to_mode(<<"shadow">>) -> shadow;
binary_to_mode(<<"active">>) -> active;
binary_to_mode(<<"primary">>) -> primary;
binary_to_mode(<<"standalone">>) -> standalone;
binary_to_mode(_) -> invalid.

%%%===================================================================
%%% Internal Functions - Utilities
%%%===================================================================

format_error(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
format_error(Reason) when is_binary(Reason) ->
    Reason;
format_error(Reason) when is_list(Reason) ->
    list_to_binary(Reason);
format_error(Reason) ->
    list_to_binary(io_lib:format("~p", [Reason])).

encode_response({ok, Data}) ->
    jsx:encode(#{success => true, data => Data});
encode_response({error, Reason}) when is_map(Reason) ->
    jsx:encode(#{success => false, error => Reason});
encode_response({error, Reason}) ->
    jsx:encode(#{success => false, error => #{reason => format_error(Reason)}}).

decode_request(<<>>) ->
    #{};
decode_request(Body) ->
    try
        jsx:decode(Body, [return_maps])
    catch
        _:_ -> #{}
    end.

%%%===================================================================
%%% Cowboy Router Configuration
%%%===================================================================

%% @doc Returns the Cowboy dispatch routes for the bridge API.
%% Call this from flurm_controller_sup when starting Cowboy.
-spec routes() -> cowboy_router:routes().
routes() ->
    [
        {'_', [
            {"/api/v1/bridge/status", ?MODULE, #{}},
            {"/api/v1/bridge/mode", ?MODULE, #{}},
            {"/api/v1/bridge/clusters", ?MODULE, #{}},
            {"/api/v1/bridge/clusters/:name", ?MODULE, #{}}
        ]}
    ].
