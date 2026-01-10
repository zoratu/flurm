%%%-------------------------------------------------------------------
%%% @doc Integration tests simulating real SLURM client behavior
%%%
%%% These tests start the FLURM controller and connect to it as a
%%% SLURM client would, sending properly formatted protocol messages.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_client_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Setup/Teardown
%%%===================================================================

-define(TEST_PORT, 16817).  % Non-standard port to avoid conflicts

setup() ->
    %% Start required applications
    application:ensure_all_started(ranch),

    %% Start a simple TCP listener using ranch
    {ok, _} = ranch:start_listener(
        test_flurm_listener,
        ranch_tcp,
        #{socket_opts => [{port, ?TEST_PORT}]},
        flurm_test_protocol_handler,
        []
    ),
    ok.

cleanup(_) ->
    ranch:stop_listener(test_flurm_listener),
    ok.

%%%===================================================================
%%% Client Connection Tests
%%%===================================================================

client_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"ping request/response", fun test_ping/0},
         {"job info request", fun test_job_info_request/0},
         {"submit batch job", fun test_submit_batch_job/0},
         {"cancel job request", fun test_cancel_job/0}
     ]}.

test_ping() ->
    {ok, Socket} = gen_tcp:connect("localhost", ?TEST_PORT,
                                    [binary, {packet, 0}, {active, false}]),

    %% Send a ping request
    {ok, PingMsg} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    ok = gen_tcp:send(Socket, PingMsg),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),

    %% Decode response
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode(Response),

    %% Verify it's a return code response
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket).

test_job_info_request() ->
    {ok, Socket} = gen_tcp:connect("localhost", ?TEST_PORT,
                                    [binary, {packet, 0}, {active, false}]),

    %% Send job info request
    Request = #job_info_request{show_flags = 0, job_id = 0, user_id = 0},
    {ok, Msg} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Request),
    ok = gen_tcp:send(Socket, Msg),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),

    %% Decode response
    {ok, #slurm_msg{header = Header, body = _Body}, _Rest} =
        flurm_protocol_codec:decode(Response),

    %% Should get either job info response or return code
    ?assert(Header#slurm_header.msg_type =:= ?RESPONSE_JOB_INFO orelse
            Header#slurm_header.msg_type =:= ?RESPONSE_SLURM_RC),

    gen_tcp:close(Socket).

test_submit_batch_job() ->
    {ok, Socket} = gen_tcp:connect("localhost", ?TEST_PORT,
                                    [binary, {packet, 0}, {active, false}]),

    %% Create a batch job request
    JobReq = #batch_job_request{
        name = <<"test_job">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho hello">>,
        work_dir = <<"/tmp">>,
        min_nodes = 1,
        max_nodes = 1,
        min_cpus = 1,
        num_tasks = 1,
        cpus_per_task = 1,
        time_limit = 60,
        priority = 100,
        user_id = 1000,
        group_id = 1000
    },

    {ok, Msg} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobReq),
    ok = gen_tcp:send(Socket, Msg),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),

    %% Decode response
    {ok, #slurm_msg{header = Header, body = _Body}, _Rest} =
        flurm_protocol_codec:decode(Response),

    %% Should get batch job response or return code
    ?assert(Header#slurm_header.msg_type =:= ?RESPONSE_SUBMIT_BATCH_JOB orelse
            Header#slurm_header.msg_type =:= ?RESPONSE_SLURM_RC),

    gen_tcp:close(Socket).

test_cancel_job() ->
    {ok, Socket} = gen_tcp:connect("localhost", ?TEST_PORT,
                                    [binary, {packet, 0}, {active, false}]),

    %% Send cancel job request
    Request = #cancel_job_request{job_id = 1, signal = 9},
    {ok, Msg} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Request),
    ok = gen_tcp:send(Socket, Msg),

    %% Receive response
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),

    %% Decode response
    {ok, #slurm_msg{header = Header, body = _Body}, _Rest} =
        flurm_protocol_codec:decode(Response),

    %% Should get return code
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),

    gen_tcp:close(Socket).
