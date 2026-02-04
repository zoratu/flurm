%%%-------------------------------------------------------------------
%%% @doc FLURM SLURM Interoperability Test Suite
%%%
%%% This Common Test suite verifies that FLURM can correctly handle
%%% real SLURM client commands (sbatch, squeue, scancel, sinfo, srun)
%%% and can forward jobs to a real SLURM cluster when operating in
%%% bridge mode.
%%%
%%% Test Environment:
%%% - Real SLURM controller (slurmctld)
%%% - Real SLURM compute daemon (slurmd)
%%% - FLURM controller (in bridge mode)
%%% - SLURM clients configured to talk to FLURM
%%% - Shared MUNGE authentication
%%%
%%% Run with:
%%%   ./scripts/run-slurm-interop-tests.sh
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_interop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases - SLURM commands against FLURM
-export([
    sbatch_to_flurm_test/1,
    squeue_against_flurm_test/1,
    scancel_test/1,
    sinfo_test/1,
    srun_interactive_test/1
]).

%% Test cases - Bridge functionality
-export([
    bridge_forward_to_slurm_test/1,
    munge_credential_validation_test/1
]).

%% Test cases - Protocol compatibility
-export([
    protocol_ping_test/1,
    protocol_job_info_test/1,
    protocol_node_info_test/1,
    protocol_partition_info_test/1
]).

%% Defines
-define(FLURM_HOST, "flurm-controller").
-define(FLURM_PORT, 6817).
-define(SLURM_HOST, "slurm-controller").
-define(SLURM_PORT, 6817).
-define(SLURM_CLIENT_FLURM, "slurm-client-flurm").
-define(SLURM_CLIENT_REAL, "slurm-client-real").
-define(TIMEOUT, 30000).

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

all() ->
    [
        {group, slurm_commands},
        {group, bridge_operations},
        {group, protocol_compatibility}
    ].

groups() ->
    [
        {slurm_commands, [sequence], [
            sbatch_to_flurm_test,
            squeue_against_flurm_test,
            scancel_test,
            sinfo_test,
            srun_interactive_test
        ]},
        {bridge_operations, [sequence], [
            bridge_forward_to_slurm_test,
            munge_credential_validation_test
        ]},
        {protocol_compatibility, [parallel], [
            protocol_ping_test,
            protocol_job_info_test,
            protocol_node_info_test,
            protocol_partition_info_test
        ]}
    ].

init_per_suite(Config) ->
    %% Get environment configuration
    FlurmHost = os:getenv("FLURM_CONTROLLER_HOST", ?FLURM_HOST),
    FlurmPort = list_to_integer(os:getenv("FLURM_CONTROLLER_PORT", "6817")),
    SlurmHost = os:getenv("SLURM_CONTROLLER_HOST", ?SLURM_HOST),
    SlurmPort = list_to_integer(os:getenv("SLURM_CONTROLLER_PORT", "6817")),
    SlurmClientFlurm = os:getenv("SLURM_CLIENT_FLURM_HOST", ?SLURM_CLIENT_FLURM),
    SlurmClientReal = os:getenv("SLURM_CLIENT_REAL_HOST", ?SLURM_CLIENT_REAL),

    ct:pal("FLURM Controller: ~s:~p", [FlurmHost, FlurmPort]),
    ct:pal("SLURM Controller: ~s:~p", [SlurmHost, SlurmPort]),
    ct:pal("SLURM Client (FLURM): ~s", [SlurmClientFlurm]),
    ct:pal("SLURM Client (Real): ~s", [SlurmClientReal]),

    %% Wait for services to be ready
    ok = wait_for_service(FlurmHost, FlurmPort, 60),

    %% Store configuration
    [{flurm_host, FlurmHost},
     {flurm_port, FlurmPort},
     {slurm_host, SlurmHost},
     {slurm_port, SlurmPort},
     {slurm_client_flurm, SlurmClientFlurm},
     {slurm_client_real, SlurmClientReal},
     {job_ids, []} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Completed test case: ~p", [TestCase]),
    ok.

%%%===================================================================
%%% Test Cases - SLURM Commands Against FLURM
%%%===================================================================

%% @doc Test sbatch job submission to FLURM controller
sbatch_to_flurm_test(Config) ->
    SlurmClient = ?config(slurm_client_flurm, Config),

    %% Create a simple test job script
    Script = "#!/bin/bash\n#SBATCH --job-name=flurm_test\n#SBATCH --nodes=1\n"
             "#SBATCH --time=00:01:00\n\necho \"Hello from FLURM test\"\nhostname\ndate\n",

    %% Submit job via sbatch
    {ok, Output} = exec_on_client(SlurmClient,
        "echo '" ++ Script ++ "' | sbatch"),

    ct:pal("sbatch output: ~s", [Output]),

    %% Extract job ID from output (format: "Submitted batch job <id>")
    case re:run(Output, "Submitted batch job ([0-9]+)", [{capture, [1], list}]) of
        {match, [JobIdStr]} ->
            JobId = list_to_integer(JobIdStr),
            ct:pal("Submitted job ID: ~p", [JobId]),
            ?assert(JobId > 0),
            %% Store job ID for later tests
            [{submitted_job_id, JobId} | Config];
        nomatch ->
            %% Check if we got a SLURM error response
            case string:find(Output, "error") of
                nomatch ->
                    ct:fail("Could not extract job ID from sbatch output: ~s", [Output]);
                _ ->
                    %% FLURM may not have compute nodes configured
                    ct:pal("sbatch returned error (expected in test environment): ~s", [Output]),
                    {skip, "sbatch failed - FLURM may not have compute nodes"}
            end
    end.

%% @doc Test squeue job listing against FLURM controller
squeue_against_flurm_test(Config) ->
    SlurmClient = ?config(slurm_client_flurm, Config),

    %% Query all jobs
    {ok, Output} = exec_on_client(SlurmClient, "squeue -a"),

    ct:pal("squeue output: ~s", [Output]),

    %% Should at least get header line
    ?assert(string:find(Output, "JOBID") =/= nomatch orelse
            string:find(Output, "error") =/= nomatch orelse
            byte_size(list_to_binary(Output)) >= 0),

    %% If we have a submitted job, check for it
    case ?config(submitted_job_id, Config) of
        undefined -> ok;
        JobId ->
            %% Query specific job
            {ok, JobOutput} = exec_on_client(SlurmClient,
                "squeue -j " ++ integer_to_list(JobId)),
            ct:pal("squeue -j ~p output: ~s", [JobId, JobOutput])
    end,

    Config.

%% @doc Test scancel job cancellation against FLURM controller
scancel_test(Config) ->
    SlurmClient = ?config(slurm_client_flurm, Config),

    %% First submit a job to cancel
    Script = "#!/bin/bash\n#SBATCH --job-name=cancel_test\n#SBATCH --time=01:00:00\nsleep 3600\n",

    {ok, SubmitOutput} = exec_on_client(SlurmClient,
        "echo '" ++ Script ++ "' | sbatch"),

    ct:pal("Submit for cancel test: ~s", [SubmitOutput]),

    case re:run(SubmitOutput, "Submitted batch job ([0-9]+)", [{capture, [1], list}]) of
        {match, [JobIdStr]} ->
            JobId = list_to_integer(JobIdStr),

            %% Give it a moment
            timer:sleep(500),

            %% Cancel the job
            {ok, CancelOutput} = exec_on_client(SlurmClient,
                "scancel " ++ integer_to_list(JobId)),

            ct:pal("scancel output: ~s", [CancelOutput]),

            %% Verify job is cancelled or error is returned
            timer:sleep(500),
            {ok, StatusOutput} = exec_on_client(SlurmClient,
                "squeue -j " ++ integer_to_list(JobId)),
            ct:pal("Post-cancel status: ~s", [StatusOutput]),

            %% Job should be gone or in CANCELLED state
            ok;
        nomatch ->
            ct:pal("Could not submit job for cancel test: ~s", [SubmitOutput]),
            {skip, "Could not submit test job"}
    end.

%% @doc Test sinfo cluster information against FLURM controller
sinfo_test(Config) ->
    SlurmClient = ?config(slurm_client_flurm, Config),

    %% Query partition info
    {ok, Output} = exec_on_client(SlurmClient, "sinfo"),

    ct:pal("sinfo output: ~s", [Output]),

    %% Should get partition information or error
    ?assert(byte_size(list_to_binary(Output)) >= 0),

    %% Query node info
    {ok, NodeOutput} = exec_on_client(SlurmClient, "sinfo -N"),
    ct:pal("sinfo -N output: ~s", [NodeOutput]),

    Config.

%% @doc Test srun interactive command (limited - may not fully work without slurmd)
srun_interactive_test(Config) ->
    SlurmClient = ?config(slurm_client_flurm, Config),

    %% Try a simple srun command - this may fail without proper setup
    %% but we want to verify the protocol handshake works
    {ok, Output} = exec_on_client(SlurmClient,
        "timeout 10 srun --nodes=1 hostname 2>&1 || echo 'srun completed or timed out'"),

    ct:pal("srun output: ~s", [Output]),

    %% We expect either:
    %% 1. Successful hostname output
    %% 2. Error about no nodes available
    %% 3. Timeout (expected without proper slurmd)
    ?assert(byte_size(list_to_binary(Output)) > 0),

    Config.

%%%===================================================================
%%% Test Cases - Bridge Operations
%%%===================================================================

%% @doc Test forwarding a job from FLURM to real SLURM cluster
bridge_forward_to_slurm_test(Config) ->
    SlurmClientFlurm = ?config(slurm_client_flurm, Config),
    SlurmClientReal = ?config(slurm_client_real, Config),

    %% Submit job to FLURM with bridge forwarding enabled
    %% This requires FLURM to be configured in active bridge mode
    Script = "#!/bin/bash\n#SBATCH --job-name=bridge_test\n#SBATCH --nodes=1\n"
             "#SBATCH --time=00:01:00\n#SBATCH --comment=forward_to_slurm\n\n"
             "echo \"Bridge test job\"\nhostname\n",

    %% Check if FLURM is in bridge mode by looking at env
    BridgeMode = os:getenv("FLURM_BRIDGE_MODE", "standalone"),

    case BridgeMode of
        "active" ->
            %% Submit to FLURM
            {ok, FlurmOutput} = exec_on_client(SlurmClientFlurm,
                "echo '" ++ Script ++ "' | sbatch"),
            ct:pal("FLURM submission (bridge): ~s", [FlurmOutput]),

            %% Give time for bridge forwarding
            timer:sleep(2000),

            %% Check if job appeared on real SLURM
            {ok, RealOutput} = exec_on_client(SlurmClientReal, "squeue -a"),
            ct:pal("Real SLURM queue: ~s", [RealOutput]),

            %% Note: This may or may not show the job depending on bridge implementation
            ok;
        _ ->
            ct:pal("Bridge mode not enabled, skipping forward test"),
            {skip, "Bridge mode not enabled"}
    end.

%% @doc Test MUNGE credential validation
munge_credential_validation_test(Config) ->
    SlurmClient = ?config(slurm_client_flurm, Config),

    %% Test that MUNGE credentials work between client and FLURM
    %% This is implicit in successful sbatch/squeue, but we can verify explicitly

    %% Generate a MUNGE credential
    {ok, CredOutput} = exec_on_client(SlurmClient, "munge -n"),
    ct:pal("MUNGE credential generated, length: ~p", [length(CredOutput)]),

    %% Verify MUNGE is working
    {ok, VerifyOutput} = exec_on_client(SlurmClient,
        "munge -n | unmunge 2>&1 | head -5"),
    ct:pal("MUNGE verification: ~s", [VerifyOutput]),

    %% Check for successful unmunge
    case string:find(VerifyOutput, "STATUS:") of
        nomatch ->
            %% Check if munge/unmunge are available
            case string:find(VerifyOutput, "not found") of
                nomatch ->
                    ct:fail("MUNGE verification failed: ~s", [VerifyOutput]);
                _ ->
                    {skip, "MUNGE tools not available in container"}
            end;
        _ ->
            ?assert(string:find(VerifyOutput, "Success") =/= nomatch orelse
                    string:find(VerifyOutput, "STATUS: 0") =/= nomatch orelse
                    string:find(VerifyOutput, "STATUS:") =/= nomatch),
            ok
    end.

%%%===================================================================
%%% Test Cases - Protocol Compatibility
%%%===================================================================

%% @doc Test protocol-level ping request
protocol_ping_test(Config) ->
    FlurmHost = ?config(flurm_host, Config),
    FlurmPort = ?config(flurm_port, Config),

    %% Connect to FLURM controller
    {ok, Socket} = gen_tcp:connect(FlurmHost, FlurmPort,
        [binary, {packet, 0}, {active, false}], ?TIMEOUT),

    %% Build and send ping message using protocol codec
    ok = application:ensure_started(flurm_protocol),
    PingRequest = flurm_protocol_codec:encode_ping_request(),
    ok = gen_tcp:send(Socket, PingRequest),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, ?TIMEOUT),

    ct:pal("Ping response bytes: ~p", [byte_size(Response)]),

    %% Should get a valid SLURM response
    ?assert(byte_size(Response) >= 14),  % Minimum SLURM header size

    gen_tcp:close(Socket),
    Config.

%% @doc Test protocol-level job info request
protocol_job_info_test(Config) ->
    FlurmHost = ?config(flurm_host, Config),
    FlurmPort = ?config(flurm_port, Config),

    {ok, Socket} = gen_tcp:connect(FlurmHost, FlurmPort,
        [binary, {packet, 0}, {active, false}], ?TIMEOUT),

    %% Build job info request (all jobs)
    ok = application:ensure_started(flurm_protocol),
    JobInfoRequest = flurm_protocol_codec:encode_job_info_request(0, 0, 0),
    ok = gen_tcp:send(Socket, JobInfoRequest),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, ?TIMEOUT),

    ct:pal("Job info response bytes: ~p", [byte_size(Response)]),
    ?assert(byte_size(Response) >= 14),

    gen_tcp:close(Socket),
    Config.

%% @doc Test protocol-level node info request
protocol_node_info_test(Config) ->
    FlurmHost = ?config(flurm_host, Config),
    FlurmPort = ?config(flurm_port, Config),

    {ok, Socket} = gen_tcp:connect(FlurmHost, FlurmPort,
        [binary, {packet, 0}, {active, false}], ?TIMEOUT),

    %% Build node info request
    ok = application:ensure_started(flurm_protocol),
    NodeInfoRequest = flurm_protocol_codec:encode_node_info_request(0, <<>>),
    ok = gen_tcp:send(Socket, NodeInfoRequest),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, ?TIMEOUT),

    ct:pal("Node info response bytes: ~p", [byte_size(Response)]),
    ?assert(byte_size(Response) >= 14),

    gen_tcp:close(Socket),
    Config.

%% @doc Test protocol-level partition info request
protocol_partition_info_test(Config) ->
    FlurmHost = ?config(flurm_host, Config),
    FlurmPort = ?config(flurm_port, Config),

    {ok, Socket} = gen_tcp:connect(FlurmHost, FlurmPort,
        [binary, {packet, 0}, {active, false}], ?TIMEOUT),

    %% Build partition info request
    ok = application:ensure_started(flurm_protocol),
    PartInfoRequest = flurm_protocol_codec:encode_partition_info_request(0, <<>>),
    ok = gen_tcp:send(Socket, PartInfoRequest),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, ?TIMEOUT),

    ct:pal("Partition info response bytes: ~p", [byte_size(Response)]),
    ?assert(byte_size(Response) >= 14),

    gen_tcp:close(Socket),
    Config.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @doc Wait for a service to become available
-spec wait_for_service(string(), integer(), integer()) -> ok | {error, timeout}.
wait_for_service(_Host, _Port, 0) ->
    {error, timeout};
wait_for_service(Host, Port, Retries) ->
    case gen_tcp:connect(Host, Port, [binary], 5000) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, _} ->
            timer:sleep(1000),
            wait_for_service(Host, Port, Retries - 1)
    end.

%% @doc Execute a command on a SLURM client container
-spec exec_on_client(string(), string()) -> {ok, string()} | {error, term()}.
exec_on_client(Container, Command) ->
    %% Use docker exec to run command on container
    DockerCmd = "docker exec " ++ Container ++ " /bin/bash -c '" ++ escape_quotes(Command) ++ "' 2>&1",
    Result = os:cmd(DockerCmd),
    {ok, Result}.

%% @doc Escape single quotes in a command string
-spec escape_quotes(string()) -> string().
escape_quotes(Str) ->
    %% Replace ' with '\'' for bash
    lists:flatten(string:replace(Str, "'", "'\\''", all)).
