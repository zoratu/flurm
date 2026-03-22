%%%-------------------------------------------------------------------
%%% @doc TLA+ Distributed Model Checker Plugin for FLURM
%%%
%%% Integrates the tlaplusplus distributed model checker with FLURM's
%%% job scheduling system. When a job is submitted with the tla plugin
%%% tag, this plugin:
%%%
%%% 1. Allocates N nodes from the FLURM cluster
%%% 2. Distributes the tlaplusplus binary and spec files to each node
%%% 3. Launches each node with --cluster-peers derived from allocated set
%%% 4. Monitors for spot reclamation
%%%
%%% Usage (from sbatch):
%%%   sbatch --comment="plugin:tla" \
%%%          --nodes=4 \
%%%          --wrap="tlaplusplus run-tla --module Spec.tla"
%%%
%%% Or programmatically:
%%%   flurm_plugin_tla:submit_distributed_check(#{
%%%       spec_file => "/path/to/Spec.tla",
%%%       config_file => "/path/to/Spec.cfg",
%%%       nodes => 4,
%%%       binary_path => "/opt/tlaplusplus/tlaplusplus"
%%%   })
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_plugin_tla).

-behaviour(flurm_plugin).

%% Plugin callbacks
-export([
    job_submit/1,
    cluster_alloc/2,
    job_launch/2,
    job_epilog/2
]).

%% Direct API
-export([
    submit_distributed_check/1
]).

-define(DEFAULT_BINARY, "/opt/tlaplusplus/tlaplusplus").
-define(DEFAULT_CLUSTER_PORT, 7878).
-define(DEFAULT_WORKERS, auto).

%%%===================================================================
%%% Plugin callbacks
%%%===================================================================

%% @doc Called on job submission. Detects TLA+ jobs by comment tag.
job_submit(#{comment := Comment} = JobSpec) when is_binary(Comment) ->
    case binary:match(Comment, <<"plugin:tla">>) of
        nomatch ->
            {ok, JobSpec};
        _ ->
            %% Tag this as a TLA+ distributed job
            lager:info("[tla_plugin] TLA+ distributed job detected"),
            {ok, JobSpec#{
                tla_plugin => true,
                cluster_port => ?DEFAULT_CLUSTER_PORT
            }}
    end;
job_submit(JobSpec) ->
    {ok, JobSpec}.

%% @doc Called after nodes are allocated. Sets up peer list for each node.
cluster_alloc(JobId, Nodes) ->
    lager:info("[tla_plugin] Setting up cluster for job ~p across ~p nodes",
               [JobId, length(Nodes)]),

    %% Build peer list: each node needs to know all other nodes
    NodeAddrs = lists:map(fun(#{hostname := Host} = N) ->
        IP = maps:get(ip, N, Host),
        Port = maps:get(cluster_port, N, ?DEFAULT_CLUSTER_PORT),
        {IP, Port}
    end, Nodes),

    %% Add peer info and node IDs to each node
    {_, AnnotatedNodes} = lists:foldl(fun(Node, {Idx, Acc}) ->
        #{hostname := Host} = Node,
        IP = maps:get(ip, Node, Host),
        %% Build peer list excluding this node
        Peers = [io_lib:format("~s:~p", [PeerIP, PeerPort])
                 || {PeerIP, PeerPort} <- NodeAddrs,
                    PeerIP =/= IP],
        PeerStr = lists:join(",", Peers),

        AnnotatedNode = Node#{
            tla_node_id => Idx,
            tla_peers => iolist_to_binary(PeerStr),
            tla_cluster_listen => iolist_to_binary(
                io_lib:format("0.0.0.0:~p", [?DEFAULT_CLUSTER_PORT]))
        },
        {Idx + 1, [AnnotatedNode | Acc]}
    end, {0, []}, Nodes),

    lager:info("[tla_plugin] Cluster peer map configured for ~p nodes", [length(Nodes)]),
    {ok, lists:reverse(AnnotatedNodes)}.

%% @doc Customizes the launch command for TLA+ distributed jobs.
job_launch(JobId, #{tla_plugin := true} = JobInfo) ->
    NodeId = maps:get(tla_node_id, JobInfo, 0),
    Peers = maps:get(tla_peers, JobInfo, <<>>),
    Listen = maps:get(tla_cluster_listen, JobInfo, <<"0.0.0.0:7878">>),
    Script = maps:get(script, JobInfo, <<>>),
    Binary = maps:get(tla_binary, JobInfo, ?DEFAULT_BINARY),

    %% Extract the spec file from the script
    %% Convention: script contains "tlaplusplus run-tla --module Spec.tla ..."
    {SpecArgs, ExtraArgs} = parse_tla_args(Script),

    Command = binary_to_list(Binary),
    Args = [
        "run-tla" | SpecArgs
    ] ++ [
        "--cluster-listen", binary_to_list(Listen),
        "--cluster-peers", binary_to_list(Peers),
        "--node-id", integer_to_list(NodeId)
    ] ++ ExtraArgs,

    Env = [
        {"FLURM_JOB_ID", integer_to_list(JobId)},
        {"FLURM_NODE_ID", integer_to_list(NodeId)}
    ],

    lager:info("[tla_plugin] Job ~p node ~p: ~s ~s",
               [JobId, NodeId, Command, lists:join(" ", Args)]),
    {ok, Command, Args, Env};
job_launch(_JobId, _JobInfo) ->
    default.

%% @doc Called after job completion. Collects results.
job_epilog(JobId, #{exit_code := ExitCode}) ->
    lager:info("[tla_plugin] Job ~p completed with exit code ~p", [JobId, ExitCode]),
    case ExitCode of
        0 ->
            lager:info("[tla_plugin] Model check PASSED (no invariant violations)");
        1 ->
            lager:warning("[tla_plugin] Model check FAILED (invariant violation found)");
        _ ->
            lager:error("[tla_plugin] Model check ERROR (exit code ~p)", [ExitCode])
    end,
    ok;
job_epilog(_JobId, _Result) ->
    ok.

%%%===================================================================
%%% Direct API
%%%===================================================================

%% @doc Submit a distributed TLA+ model check as a FLURM job.
%%
%% Options:
%%   spec_file    - Path to .tla spec file (required)
%%   config_file  - Path to .cfg config file (optional)
%%   nodes        - Number of nodes (default: 2)
%%   binary_path  - Path to tlaplusplus binary (default: /opt/tlaplusplus/tlaplusplus)
%%   workers      - Workers per node (default: auto-detect)
%%   s3_bucket    - S3 bucket for checkpoints and file distribution (optional)
%%   s3_prefix    - S3 prefix for spec files (default: "specs/<basename>")
%%   extra_args   - Additional CLI args (optional list of strings)
%%
-spec submit_distributed_check(map()) -> {ok, integer()} | {error, term()}.
submit_distributed_check(Opts) ->
    SpecFile = maps:get(spec_file, Opts),
    ConfigFile = maps:get(config_file, Opts, <<>>),
    NumNodes = maps:get(nodes, Opts, 2),
    Binary = maps:get(binary_path, Opts, <<?DEFAULT_BINARY>>),
    Workers = maps:get(workers, Opts, auto),
    S3Bucket = maps:get(s3_bucket, Opts, <<>>),
    ExtraArgs = maps:get(extra_args, Opts, []),

    %% When S3 is configured, upload spec/config and use --fetch-module/--fetch-config
    %% so nodes don't need a shared filesystem.
    {SpecArg, ConfigArg, S3Args} = case S3Bucket of
        <<>> ->
            %% No S3 — assume shared filesystem
            {["--module ", SpecFile],
             case ConfigFile of
                 <<>> -> "";
                 _ -> [" --config ", ConfigFile]
             end,
             ""};
        Bucket ->
            BaseName = filename:basename(SpecFile),
            S3Prefix = maps:get(s3_prefix, Opts,
                iolist_to_binary(["specs/", filename:basename(SpecFile, ".tla")])),
            ModuleUri = iolist_to_binary([
                "s3://", Bucket, "/", S3Prefix, "/", BaseName]),
            %% Upload spec file to S3
            upload_to_s3(Bucket, [S3Prefix, "/", BaseName], SpecFile),
            SpecA = ["--fetch-module ", ModuleUri],
            CfgA = case ConfigFile of
                <<>> -> "";
                _ ->
                    CfgBase = filename:basename(ConfigFile),
                    CfgUri = iolist_to_binary([
                        "s3://", Bucket, "/", S3Prefix, "/", CfgBase]),
                    upload_to_s3(Bucket, [S3Prefix, "/", CfgBase], ConfigFile),
                    [" --fetch-config ", CfgUri]
            end,
            S3A = [" --s3-bucket ", Bucket, " --checkpoint-interval-secs 60"],
            {SpecA, CfgA, S3A}
    end,

    %% Build the script command
    Script = iolist_to_binary([
        Binary, " run-tla ", SpecArg, ConfigArg,
        case Workers of
            auto -> "";
            N when is_integer(N) -> [" --workers ", integer_to_binary(N)]
        end,
        S3Args,
        lists:map(fun(A) -> [" ", A] end, ExtraArgs)
    ]),

    JobSpec = #{
        name => iolist_to_binary(["tla_check_", filename:basename(SpecFile, ".tla")]),
        script => Script,
        partition => <<"default">>,
        num_nodes => NumNodes,
        num_cpus => 1,  % Per-node, will use all cores via --workers
        time_limit => 86400,  % 24 hours
        comment => <<"plugin:tla">>,
        tla_plugin => true,
        tla_binary => Binary
    },

    flurm_job_manager:submit_job(JobSpec).

%%%===================================================================
%%% Internal
%%%===================================================================

%% Parse TLA+ args from a script line
parse_tla_args(Script) when is_binary(Script) ->
    parse_tla_args(binary_to_list(Script));
parse_tla_args(Script) when is_list(Script) ->
    %% Simple parser: extract --module and other args
    Tokens = string:tokens(Script, " \t\n"),
    parse_tokens(Tokens, [], []).

parse_tokens([], SpecArgs, ExtraArgs) ->
    {lists:reverse(SpecArgs), lists:reverse(ExtraArgs)};
parse_tokens(["--module", File | Rest], SpecArgs, ExtraArgs) ->
    parse_tokens(Rest, ["--module", File | SpecArgs], ExtraArgs);
parse_tokens(["--config", File | Rest], SpecArgs, ExtraArgs) ->
    parse_tokens(Rest, ["--config", File | SpecArgs], ExtraArgs);
parse_tokens(["--fetch-module", Uri | Rest], SpecArgs, ExtraArgs) ->
    parse_tokens(Rest, ["--fetch-module", Uri | SpecArgs], ExtraArgs);
parse_tokens(["--fetch-config", Uri | Rest], SpecArgs, ExtraArgs) ->
    parse_tokens(Rest, ["--fetch-config", Uri | SpecArgs], ExtraArgs);
parse_tokens(["run-tla" | Rest], SpecArgs, ExtraArgs) ->
    parse_tokens(Rest, SpecArgs, ExtraArgs);  % Skip subcommand (we add it)
parse_tokens([Arg | Rest], SpecArgs, ExtraArgs) ->
    %% Skip the binary name, keep other args
    case lists:prefix("/", Arg) orelse lists:prefix("tlaplusplus", Arg) of
        true -> parse_tokens(Rest, SpecArgs, ExtraArgs);
        false -> parse_tokens(Rest, SpecArgs, [Arg | ExtraArgs])
    end.

%% Upload a local file to S3.
%% Uses the AWS CLI since Erlang doesn't have a native S3 client in OTP.
upload_to_s3(Bucket, Key, LocalPath) ->
    S3Uri = iolist_to_binary(["s3://", Bucket, "/", Key]),
    Cmd = io_lib:format("aws s3 cp ~s ~s", [LocalPath, S3Uri]),
    case os:cmd(lists:flatten(Cmd)) of
        [] -> ok;
        Output ->
            lager:info("[tla_plugin] S3 upload ~s: ~s", [S3Uri, Output]),
            ok
    end.
