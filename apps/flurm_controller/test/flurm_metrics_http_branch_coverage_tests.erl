%%%-------------------------------------------------------------------
%%% @doc Branch-targeted coverage tests for flurm_metrics_http.
%%%-------------------------------------------------------------------
-module(flurm_metrics_http_branch_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

-record(state, {
    socket :: gen_tcp:socket() | undefined,
    port :: pos_integer()
}).

init_error_reason_branch_test() ->
    catch meck:unload(application),
    catch meck:unload(gen_tcp),
    meck:new(application, [unstick, passthrough, no_passthrough_cover]),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(application, get_env, fun
        (flurm_controller, metrics_port, _) -> 19191;
        (_, _, Default) -> Default
    end),
    meck:expect(gen_tcp, listen, fun(_, _) -> {error, eperm} end),
    {ok, State} = flurm_metrics_http:init([]),
    ?assertEqual(undefined, State#state.socket),
    ?assertEqual(19191, State#state.port),
    meck:unload(gen_tcp),
    meck:unload(application).

handle_info_accept_error_branch_test() ->
    catch meck:unload(gen_tcp),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(gen_tcp, accept, fun(_, _) -> {error, econnreset} end),
    State = #state{socket = make_ref(), port = 9090},
    {noreply, NewState} = flurm_metrics_http:handle_info(accept, State),
    ?assertEqual(State, NewState),
    receive
        accept -> ok
    after 1000 ->
        ?assert(false)
    end,
    meck:unload(gen_tcp).

drain_headers_default_branch_test() ->
    catch meck:unload(gen_tcp),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(gen_tcp, recv, fun(_, _, _) -> {error, timeout} end),
    ?assertEqual(ok, flurm_metrics_http:drain_headers(fake_socket)),
    meck:unload(gen_tcp).

drain_headers_with_length_branches_test() ->
    catch meck:unload(gen_tcp),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    C = atomics:new(1, [{signed, false}]),
    meck:expect(gen_tcp, recv, fun(_, _, _) ->
        case atomics:add_get(C, 1, 1) of
            1 -> {ok, {http_header, 0, 'Content-Length', 0, <<"abc">>}};
            2 -> {ok, {http_header, 0, 'X-Test', 0, <<"v">>}};
            _ -> {ok, http_eoh}
        end
    end),
    {Len, Headers} = flurm_metrics_http:drain_headers_with_length(fake_socket),
    ?assertEqual(0, Len),
    ?assert(lists:keymember('Content-Length', 1, Headers)),
    ?assert(lists:keymember('X-Test', 1, Headers)),
    meck:unload(gen_tcp).

drain_headers_with_length_fallback_test() ->
    catch meck:unload(gen_tcp),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(gen_tcp, recv, fun(_, _, _) -> {error, timeout} end),
    ?assertEqual({0, []}, flurm_metrics_http:drain_headers_with_length(fake_socket)),
    meck:unload(gen_tcp).

read_body_success_test() ->
    catch meck:unload(inet),
    catch meck:unload(gen_tcp),
    meck:new(inet, [unstick, passthrough, no_passthrough_cover]),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    meck:expect(gen_tcp, recv, fun(_, 3, 5000) -> {ok, <<"abc">>} end),
    ?assertEqual(<<"abc">>, flurm_metrics_http:read_body(fake_socket, 3)),
    meck:unload(gen_tcp),
    meck:unload(inet).

read_body_error_test() ->
    catch meck:unload(inet),
    catch meck:unload(gen_tcp),
    meck:new(inet, [unstick, passthrough, no_passthrough_cover]),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    meck:expect(gen_tcp, recv, fun(_, 4, 5000) -> {error, closed} end),
    ?assertEqual(<<>>, flurm_metrics_http:read_body(fake_socket, 4)),
    meck:unload(gen_tcp),
    meck:unload(inet).

send_json_response_status_lines_test() ->
    catch meck:unload(inet),
    catch meck:unload(gen_tcp),
    meck:new(inet, [unstick, passthrough, no_passthrough_cover]),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    meck:expect(gen_tcp, send, fun(_, Data) ->
        put(last_send, iolist_to_binary(Data)),
        ok
    end),
    Body = <<"{}">>,
    Expected = [
        {200, <<"HTTP/1.1 200 OK">>},
        {201, <<"HTTP/1.1 201 Created">>},
        {400, <<"HTTP/1.1 400 Bad Request">>},
        {404, <<"HTTP/1.1 404 Not Found">>},
        {500, <<"HTTP/1.1 500 Internal Server Error">>},
        {503, <<"HTTP/1.1 503 Service Unavailable">>},
        {999, <<"HTTP/1.1 200 OK">>}
    ],
    lists:foreach(fun({Code, Prefix}) ->
        ok = flurm_metrics_http:send_json_response(fake_socket, Code, Body),
        Sent = get(last_send),
        ?assert(binary:match(Sent, Prefix) =/= nomatch)
    end, Expected),
    meck:unload(gen_tcp),
    meck:unload(inet).

handle_request_federation_api_path_test() ->
    catch meck:unload(inet),
    catch meck:unload(gen_tcp),
    catch meck:unload(flurm_federation_api),
    meck:new(inet, [unstick, passthrough, no_passthrough_cover]),
    meck:new(gen_tcp, [unstick, passthrough, no_passthrough_cover]),
    meck:new(flurm_federation_api, [non_strict]),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    meck:expect(flurm_federation_api, handle, fun(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>) ->
        {201, <<"{\"ok\":true}">>}
    end),
    C = atomics:new(1, [{signed, false}]),
    meck:expect(gen_tcp, recv, fun(_, _, _) ->
        case atomics:add_get(C, 1, 1) of
            1 -> {ok, {http_request, 'POST', {abs_path, <<"/api/v1/federation/jobs">>}, {1, 1}}};
            2 -> {ok, {http_header, 0, 'Content-Length', 0, <<"2">>}};
            3 -> {ok, http_eoh};
            4 -> {ok, <<"{}">>};
            _ -> {error, closed}
        end
    end),
    meck:expect(gen_tcp, send, fun(_, Data) ->
        put(last_send, iolist_to_binary(Data)),
        ok
    end),
    meck:expect(gen_tcp, close, fun(_) -> ok end),
    ok = flurm_metrics_http:handle_request(fake_socket),
    Sent = get(last_send),
    ?assert(binary:match(Sent, <<"HTTP/1.1 201 Created">>) =/= nomatch),
    ?assert(meck:called(flurm_federation_api, handle, [<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>])),
    meck:unload(flurm_federation_api),
    meck:unload(gen_tcp),
    meck:unload(inet).
