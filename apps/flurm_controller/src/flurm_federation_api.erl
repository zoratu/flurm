%%%-------------------------------------------------------------------
%%% @doc FLURM Federation REST API
%%%
%%% Provides HTTP REST endpoints for federation management.
%%%
%%% Endpoints:
%%%   GET  /api/v1/federation              - Get federation info
%%%   GET  /api/v1/federation/clusters     - List clusters
%%%   POST /api/v1/federation/clusters     - Add cluster
%%%   DELETE /api/v1/federation/clusters/:name - Remove cluster
%%%   GET  /api/v1/federation/resources    - Aggregate resources
%%%   POST /api/v1/federation/jobs         - Submit federated job
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_api).

-export([
    handle/3,
    handle/4
]).

-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% API
%%====================================================================

%% @doc Handle API request with method, path, and body
-spec handle(binary(), binary(), binary()) -> {integer(), binary()}.
handle(Method, Path, Body) ->
    handle(Method, Path, Body, #{}).

-spec handle(binary(), binary(), binary(), map()) -> {integer(), binary()}.
handle(<<"GET">>, <<"/api/v1/federation">>, _Body, _Opts) ->
    get_federation_info();

handle(<<"GET">>, <<"/api/v1/federation/clusters">>, _Body, _Opts) ->
    list_clusters();

handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body, _Opts) ->
    add_cluster(Body);

handle(<<"DELETE">>, <<"/api/v1/federation/clusters/", Name/binary>>, _Body, _Opts) ->
    remove_cluster(Name);

handle(<<"GET">>, <<"/api/v1/federation/resources">>, _Body, _Opts) ->
    get_federation_resources();

handle(<<"POST">>, <<"/api/v1/federation/jobs">>, Body, _Opts) ->
    submit_federated_job(Body);

handle(_Method, _Path, _Body, _Opts) ->
    {404, jsx:encode(#{error => <<"Not found">>})}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Get federation info
get_federation_info() ->
    case catch flurm_federation:get_federation_info() of
        {ok, Info} ->
            Response = #{
                status => <<"ok">>,
                federation_name => maps:get(name, Info, <<>>),
                local_cluster => maps:get(local_cluster, Info, <<"local">>),
                cluster_count => length(maps:get(clusters, Info, []))
            },
            {200, jsx:encode(Response)};
        {error, not_federated} ->
            Response = #{
                status => <<"not_federated">>,
                federation_name => <<>>,
                local_cluster => <<"local">>,
                cluster_count => 0
            },
            {200, jsx:encode(Response)};
        {'EXIT', {noproc, _}} ->
            {503, jsx:encode(#{error => <<"Federation service not available">>})};
        Error ->
            {500, jsx:encode(#{error => format_error(Error)})}
    end.

%% @doc List all federated clusters
list_clusters() ->
    case catch flurm_federation:list_clusters() of
        Clusters when is_list(Clusters) ->
            Response = #{
                status => <<"ok">>,
                clusters => format_clusters(Clusters),
                count => length(Clusters)
            },
            {200, jsx:encode(Response)};
        {'EXIT', {noproc, _}} ->
            {503, jsx:encode(#{error => <<"Federation service not available">>})};
        Error ->
            {500, jsx:encode(#{error => format_error(Error)})}
    end.

%% @doc Add a cluster to the federation
add_cluster(Body) ->
    try
        Params = jsx:decode(Body, [return_maps]),
        Name = maps:get(<<"name">>, Params),
        Host = maps:get(<<"host">>, Params, <<"localhost">>),
        Port = maps:get(<<"port">>, Params, 6817),
        Config = #{
            host => Host,
            port => Port,
            weight => maps:get(<<"weight">>, Params, 1),
            features => maps:get(<<"features">>, Params, []),
            partitions => maps:get(<<"partitions">>, Params, [])
        },
        case flurm_federation:add_cluster(Name, Config) of
            ok ->
                Response = #{
                    status => <<"ok">>,
                    message => <<"Cluster added">>,
                    name => Name
                },
                {201, jsx:encode(Response)};
            {error, Reason} ->
                {400, jsx:encode(#{error => format_error(Reason)})}
        end
    catch
        error:badarg ->
            {400, jsx:encode(#{error => <<"Invalid JSON">>})};
        _:Error ->
            {500, jsx:encode(#{error => format_error(Error)})}
    end.

%% @doc Remove a cluster from the federation
remove_cluster(Name) ->
    case catch flurm_federation:remove_cluster(Name) of
        ok ->
            Response = #{
                status => <<"ok">>,
                message => <<"Cluster removed">>,
                name => Name
            },
            {200, jsx:encode(Response)};
        {error, cannot_remove_local} ->
            {400, jsx:encode(#{error => <<"Cannot remove local cluster">>})};
        {error, not_found} ->
            {404, jsx:encode(#{error => <<"Cluster not found">>})};
        {'EXIT', {noproc, _}} ->
            {503, jsx:encode(#{error => <<"Federation service not available">>})};
        Error ->
            {500, jsx:encode(#{error => format_error(Error)})}
    end.

%% @doc Get aggregated federation resources
get_federation_resources() ->
    case catch flurm_federation:get_federation_resources() of
        Resources when is_map(Resources) ->
            Response = #{
                status => <<"ok">>,
                resources => Resources
            },
            {200, jsx:encode(Response)};
        {'EXIT', {noproc, _}} ->
            {503, jsx:encode(#{error => <<"Federation service not available">>})};
        Error ->
            {500, jsx:encode(#{error => format_error(Error)})}
    end.

%% @doc Submit a job to the federation
submit_federated_job(Body) ->
    try
        Params = jsx:decode(Body, [return_maps]),
        JobSpec = #{
            name => maps:get(<<"name">>, Params, <<"federated_job">>),
            script => maps:get(<<"script">>, Params),
            partition => maps:get(<<"partition">>, Params, <<"default">>),
            num_cpus => maps:get(<<"num_cpus">>, Params, 1),
            user_id => maps:get(<<"user_id">>, Params, 1000),
            group_id => maps:get(<<"group_id">>, Params, 1000)
        },
        RoutingPolicy = maps:get(<<"routing">>, Params, <<"round-robin">>),

        %% Route the job to best cluster
        case catch flurm_federation:route_job(JobSpec) of
            {ok, Cluster} ->
                %% Submit to the selected cluster
                case flurm_federation:submit_job(Cluster, JobSpec) of
                    {ok, JobId} ->
                        Response = #{
                            status => <<"ok">>,
                            job_id => JobId,
                            cluster => Cluster,
                            routing => RoutingPolicy
                        },
                        {201, jsx:encode(Response)};
                    {error, Reason} ->
                        {500, jsx:encode(#{error => format_error(Reason)})}
                end;
            {error, no_eligible_clusters} ->
                {503, jsx:encode(#{error => <<"No eligible clusters for job">>})};
            {'EXIT', {noproc, _}} ->
                {503, jsx:encode(#{error => <<"Federation service not available">>})};
            RouteError ->
                {500, jsx:encode(#{error => format_error(RouteError)})}
        end
    catch
        error:badarg ->
            {400, jsx:encode(#{error => <<"Invalid JSON">>})};
        _:CatchError ->
            {500, jsx:encode(#{error => format_error(CatchError)})}
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @doc Format clusters for JSON response
format_clusters(Clusters) ->
    lists:map(fun(Cluster) ->
        case Cluster of
            #{name := Name} ->
                #{
                    name => Name,
                    host => maps:get(host, Cluster, <<>>),
                    port => maps:get(port, Cluster, 6817),
                    state => maps:get(state, Cluster, <<"unknown">>)
                };
            _ when is_tuple(Cluster) ->
                %% Handle record format
                #{cluster => iolist_to_binary(io_lib:format("~p", [Cluster]))};
            _ ->
                #{cluster => <<"unknown">>}
        end
    end, Clusters).

%% @doc Format error for JSON
format_error(Error) when is_binary(Error) -> Error;
format_error(Error) when is_atom(Error) -> atom_to_binary(Error, utf8);
format_error(Error) -> iolist_to_binary(io_lib:format("~p", [Error])).
