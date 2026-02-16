%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_srun_callback module
%%% Tests for srun callback handler
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_srun_callback_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Tests that exercise the gen_server callbacks with real function calls

%%====================================================================
%% gen_server Lifecycle Tests
%%====================================================================

start_stop_test_() ->
    {setup,
     fun() ->
         %% Stop any existing server
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 catch gen_server:stop(Pid, shutdown, 1000)
         end,
         timer:sleep(50)
     end,
     fun(_) ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, shutdown, 1000)
         end
     end,
     fun(_) ->
         [
             {"start_link returns pid", fun() ->
                 {ok, Pid} = flurm_srun_callback:start_link(),
                 ?assert(is_pid(Pid)),
                 ?assert(is_process_alive(Pid)),
                 gen_server:stop(Pid)
             end}
         ]
     end}.

%%====================================================================
%% API Tests with Running Server
%%====================================================================

srun_callback_api_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(_Pid) ->
         [
             {"get_callback returns not_found for unknown job", fun() ->
                 Result = flurm_srun_callback:get_callback(99999),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"send_task_output fails without callback", fun() ->
                 Result = flurm_srun_callback:send_task_output(99999, <<"output">>, 0),
                 ?assertEqual({error, no_callback}, Result)
             end},
             {"notify_job_complete fails without callback", fun() ->
                 Result = flurm_srun_callback:notify_job_complete(99999, 0, <<>>),
                 ?assertEqual({error, no_callback}, Result)
             end},
             {"notify_job_ready fails without callback", fun() ->
                 Result = flurm_srun_callback:notify_job_ready(99999, <<"node1">>),
                 ?assertEqual({error, no_callback}, Result)
             end}
         ]
     end}.

%%====================================================================
%% register_callback Tests
%%====================================================================

register_callback_invalid_port_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(_) ->
         [
             {"register_callback fails with port 0", fun() ->
                 Result = flurm_srun_callback:register_callback(1, <<"localhost">>, 0, 1234),
                 ?assertEqual({error, invalid_port}, Result)
             end},
             {"register_callback fails with invalid host", fun() ->
                 %% Connection to non-existent host should fail
                 Result = flurm_srun_callback:register_callback(2, <<"nonexistent.invalid.host">>, 12345, 5678),
                 ?assertMatch({error, _}, Result)
             end}
         ]
     end}.

%%====================================================================
%% register_existing_socket Tests
%%====================================================================

register_existing_socket_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(_) ->
         [
             {"register_existing_socket stores callback info", fun() ->
                 %% Create a fake socket (just need any port reference)
                 {ok, ListenSock} = gen_tcp:listen(0, [binary]),
                 {ok, Port} = inet:port(ListenSock),

                 %% Connect to ourselves to get a socket
                 spawn(fun() ->
                     {ok, _} = gen_tcp:accept(ListenSock)
                 end),
                 timer:sleep(50),
                 {ok, Socket} = gen_tcp:connect("localhost", Port, [binary]),

                 %% Register the socket
                 ok = flurm_srun_callback:register_existing_socket(100, Socket),

                 %% Should be able to get the callback
                 {ok, Info} = flurm_srun_callback:get_callback(100),
                 ?assert(is_map(Info)),
                 ?assertEqual(Socket, maps:get(socket, Info)),

                 %% Cleanup
                 gen_tcp:close(Socket),
                 gen_tcp:close(ListenSock)
             end}
         ]
     end}.

%%====================================================================
%% TCP Message Handling Tests
%%====================================================================

tcp_closed_handling_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(Pid) ->
         [
             {"tcp_closed removes callback", fun() ->
                 %% Create a socket pair
                 {ok, ListenSock} = gen_tcp:listen(0, [binary]),
                 {ok, Port} = inet:port(ListenSock),
                 spawn(fun() -> {ok, _} = gen_tcp:accept(ListenSock) end),
                 timer:sleep(50),
                 {ok, Socket} = gen_tcp:connect("localhost", Port, [binary]),

                 %% Register
                 ok = flurm_srun_callback:register_existing_socket(200, Socket),
                 {ok, _} = flurm_srun_callback:get_callback(200),

                 %% Simulate tcp_closed message
                 Pid ! {tcp_closed, Socket},
                 timer:sleep(50),

                 %% Callback should be removed
                 ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(200)),

                 %% Cleanup
                 catch gen_tcp:close(Socket),
                 catch gen_tcp:close(ListenSock)
             end}
         ]
     end}.

tcp_error_handling_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(Pid) ->
         [
             {"tcp_error removes callback", fun() ->
                 %% Create a socket pair
                 {ok, ListenSock} = gen_tcp:listen(0, [binary]),
                 {ok, Port} = inet:port(ListenSock),
                 spawn(fun() -> {ok, _} = gen_tcp:accept(ListenSock) end),
                 timer:sleep(50),
                 {ok, Socket} = gen_tcp:connect("localhost", Port, [binary]),

                 %% Register
                 ok = flurm_srun_callback:register_existing_socket(300, Socket),

                 %% Simulate tcp_error message
                 Pid ! {tcp_error, Socket, econnreset},
                 timer:sleep(50),

                 %% Callback should be removed
                 ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(300)),

                 %% Cleanup
                 catch gen_tcp:close(Socket),
                 catch gen_tcp:close(ListenSock)
             end}
         ]
     end}.

%%====================================================================
%% Unknown Request Handling Tests
%%====================================================================

unknown_request_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(Pid) ->
         [
             {"unknown call returns error", fun() ->
                 Result = gen_server:call(Pid, unknown_request),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"unknown cast is handled", fun() ->
                 %% Cast should not crash the server
                 gen_server:cast(Pid, unknown_cast),
                 timer:sleep(50),
                 ?assert(is_process_alive(Pid))
             end},
             {"unknown info is handled", fun() ->
                 Pid ! unknown_message,
                 timer:sleep(50),
                 ?assert(is_process_alive(Pid))
             end}
         ]
     end}.

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_closes_sockets_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50)
     end,
     fun(_) -> ok end,
     fun(_) ->
         [
             {"terminate closes all sockets", fun() ->
                 {ok, Pid} = flurm_srun_callback:start_link(),

                 %% Create and register a socket
                 {ok, ListenSock} = gen_tcp:listen(0, [binary]),
                 {ok, Port} = inet:port(ListenSock),
                 spawn(fun() -> {ok, _} = gen_tcp:accept(ListenSock) end),
                 timer:sleep(50),
                 {ok, Socket} = gen_tcp:connect("localhost", Port, [binary]),
                 ok = flurm_srun_callback:register_existing_socket(400, Socket),

                 %% Stop the server
                 gen_server:stop(Pid),

                 %% Socket should be closed (send will fail)
                 timer:sleep(50),
                 Result = gen_tcp:send(Socket, <<"test">>),
                 ?assertMatch({error, _}, Result),

                 %% Cleanup
                 catch gen_tcp:close(ListenSock)
             end}
         ]
     end}.

%%====================================================================
%% Edge Cases
%%====================================================================

multiple_jobs_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_srun_callback) of
             undefined -> ok;
             P -> catch gen_server:stop(P, shutdown, 1000)
         end,
         timer:sleep(50),
         {ok, Pid} = flurm_srun_callback:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, shutdown, 1000)
     end,
     fun(_) ->
         [
             {"multiple jobs can be registered", fun() ->
                 %% Create sockets for two jobs
                 {ok, Listen1} = gen_tcp:listen(0, [binary]),
                 {ok, Port1} = inet:port(Listen1),
                 spawn(fun() -> {ok, _} = gen_tcp:accept(Listen1) end),
                 timer:sleep(50),
                 {ok, Sock1} = gen_tcp:connect("localhost", Port1, [binary]),

                 {ok, Listen2} = gen_tcp:listen(0, [binary]),
                 {ok, Port2} = inet:port(Listen2),
                 spawn(fun() -> {ok, _} = gen_tcp:accept(Listen2) end),
                 timer:sleep(50),
                 {ok, Sock2} = gen_tcp:connect("localhost", Port2, [binary]),

                 %% Register both
                 ok = flurm_srun_callback:register_existing_socket(500, Sock1),
                 ok = flurm_srun_callback:register_existing_socket(501, Sock2),

                 %% Both should be retrievable
                 {ok, Info1} = flurm_srun_callback:get_callback(500),
                 {ok, Info2} = flurm_srun_callback:get_callback(501),
                 ?assertEqual(Sock1, maps:get(socket, Info1)),
                 ?assertEqual(Sock2, maps:get(socket, Info2)),

                 %% Cleanup
                 gen_tcp:close(Sock1),
                 gen_tcp:close(Sock2),
                 gen_tcp:close(Listen1),
                 gen_tcp:close(Listen2)
             end}
         ]
     end}.
