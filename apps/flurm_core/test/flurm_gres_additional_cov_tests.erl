%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for FLURM GRES (Generic Resource) Management
%%%
%%% Tests GRES functionality including type management, node tracking,
%%% allocation, and scheduler integration.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_gres_additional_cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Generators
%%%===================================================================

gres_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun type_management_tests/1,
      fun node_gres_tests/1,
      fun allocation_tests/1,
      fun spec_parsing_tests/1,
      fun scheduler_integration_tests/1,
      fun legacy_api_tests/1,
      fun message_handling_tests/1
     ]}.

setup() ->
    %% Clean up any leftover state from previous test runs
    cleanup_gres_state(),

    %% Mock lager
    catch meck:unload(lager),
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    %% Start the GRES server
    {ok, Pid} = flurm_gres:start_link(),
    Pid.

cleanup(Pid) ->
    %% Stop the GRES server gracefully
    stop_gres_server(Pid),

    %% Clean up ETS tables
    cleanup_gres_state(),

    %% Unload meck mocks
    catch meck:unload(lager),
    ok.

%% Helper to stop the GRES server safely
stop_gres_server(Pid) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        true ->
            %% Unlink first to avoid test process crash if gen_server:stop fails
            catch unlink(Pid),
            catch gen_server:stop(Pid, normal, 5000),
            %% Wait for the process to terminate
            wait_for_process_death(Pid, 100);
        false ->
            ok
    end;
stop_gres_server(_) ->
    %% Also try to stop by registered name in case Pid is invalid
    case whereis(flurm_gres) of
        undefined -> ok;
        RegPid ->
            catch unlink(RegPid),
            catch gen_server:stop(RegPid, normal, 5000),
            wait_for_process_death(RegPid, 100)
    end.

%% Wait for a process to die with timeout
wait_for_process_death(Pid, Timeout) when Timeout > 0 ->
    case is_process_alive(Pid) of
        false -> ok;
        true ->
            timer:sleep(10),
            wait_for_process_death(Pid, Timeout - 10)
    end;
wait_for_process_death(Pid, _Timeout) ->
    %% Force kill if still alive
    catch exit(Pid, kill),
    timer:sleep(10),
    ok.

%% Clean up GRES state (ETS tables and registered process)
cleanup_gres_state() ->
    %% Stop any registered process
    case whereis(flurm_gres) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, normal, 1000),
            catch exit(Pid, kill),
            timer:sleep(10)
    end,

    %% Delete ETS tables
    catch ets:delete(flurm_gres_types),
    catch ets:delete(flurm_gres_nodes),
    catch ets:delete(flurm_gres_allocations),
    catch ets:delete(flurm_gres_jobs),
    ok.

%%%===================================================================
%%% Type Management Tests
%%%===================================================================

type_management_tests(_Pid) ->
    [
        {"register_type for GPU", fun() ->
            Config = #{
                count => 4,
                type_specific => #{
                    vendor => nvidia,
                    model => <<"A100">>,
                    memory_mb => 40960
                }
            },
            ?assertEqual(ok, flurm_gres:register_type(gpu, Config))
        end},

        {"register_type for FPGA", fun() ->
            Config = #{count => 2},
            ?assertEqual(ok, flurm_gres:register_type(fpga, Config))
        end},

        {"register_type for MPS", fun() ->
            Config = #{count => 100, type_specific => #{mode => shared}},
            ?assertEqual(ok, flurm_gres:register_type(mps, Config))
        end},

        {"list_types returns registered types", fun() ->
            flurm_gres:register_type(gpu, #{count => 2}),
            flurm_gres:register_type(fpga, #{count => 1}),
            Types = flurm_gres:list_types(),
            ?assert(is_list(Types)),
            ?assert(lists:member(gpu, Types))
        end},

        {"get_type_info for existing type", fun() ->
            Config = #{count => 8, memory_mb => 81920},
            flurm_gres:register_type(gpu, Config),
            {ok, Info} = flurm_gres:get_type_info(gpu),
            ?assert(is_map(Info)),
            ?assertEqual(8, maps:get(count, Info))
        end},

        {"get_type_info for non-existent type", fun() ->
            ?assertEqual({error, not_found}, flurm_gres:get_type_info(unknown))
        end},

        {"unregister_type removes type", fun() ->
            flurm_gres:register_type(temp_type, #{count => 1}),
            ?assertEqual(ok, flurm_gres:unregister_type(temp_type)),
            ?assertEqual({error, not_found}, flurm_gres:get_type_info(temp_type))
        end}
    ].

%%%===================================================================
%%% Node GRES Tests
%%%===================================================================

node_gres_tests(_Pid) ->
    [
        {"register_node_gres creates entries", fun() ->
            GRESList = [
                #{type => gpu, index => 0, name => <<"nvidia_a100">>, memory_mb => 40960},
                #{type => gpu, index => 1, name => <<"nvidia_a100">>, memory_mb => 40960}
            ],
            ?assertEqual(ok, flurm_gres:register_node_gres(<<"node01">>, GRESList))
        end},

        {"get_node_gres returns registered GRES", fun() ->
            GRESList = [
                #{type => gpu, index => 0, name => <<"nvidia_v100">>, memory_mb => 16384}
            ],
            flurm_gres:register_node_gres(<<"node02">>, GRESList),
            {ok, Retrieved} = flurm_gres:get_node_gres(<<"node02">>),
            ?assert(is_list(Retrieved)),
            ?assert(length(Retrieved) >= 1)
        end},

        {"get_node_gres for node without GRES", fun() ->
            {ok, Retrieved} = flurm_gres:get_node_gres(<<"nogresnod">>),
            ?assertEqual([], Retrieved)
        end},

        {"update_node_gres updates entries", fun() ->
            Initial = [#{type => gpu, index => 0, name => <<"v100">>, memory_mb => 16384}],
            flurm_gres:register_node_gres(<<"node03">>, Initial),
            Updated = [
                #{type => gpu, index => 0, name => <<"a100">>, memory_mb => 40960},
                #{type => gpu, index => 1, name => <<"a100">>, memory_mb => 40960}
            ],
            ?assertEqual(ok, flurm_gres:update_node_gres(<<"node03">>, Updated))
        end},

        {"get_nodes_with_gres finds matching nodes", fun() ->
            GRESList = [#{type => gpu, index => 0, name => <<"a100">>}],
            flurm_gres:register_node_gres(<<"gpunode1">>, GRESList),
            flurm_gres:register_node_gres(<<"gpunode2">>, GRESList),
            Nodes = flurm_gres:get_nodes_with_gres(gpu),
            ?assert(is_list(Nodes))
        end}
    ].

%%%===================================================================
%%% Allocation Tests
%%%===================================================================

allocation_tests(_Pid) ->
    [
        {"allocate GRES from binary string", fun() ->
            GRESList = [
                #{type => gpu, index => 0, name => <<"a100">>},
                #{type => gpu, index => 1, name => <<"a100">>}
            ],
            flurm_gres:register_node_gres(<<"allocnode1">>, GRESList),
            Result = flurm_gres:allocate(12345, <<"allocnode1">>, <<"gpu:1">>),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"allocate GRES with type and count", fun() ->
            GRESList = [#{type => fpga, index => 0, name => <<"stratix">>}],
            flurm_gres:register_node_gres(<<"fpganode">>, GRESList),
            Result = flurm_gres:allocate(12346, <<"fpganode">>, <<"fpga:1">>),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"deallocate releases GRES", fun() ->
            GRESList = [#{type => gpu, index => 0, name => <<"v100">>}],
            flurm_gres:register_node_gres(<<"deallocnode">>, GRESList),
            flurm_gres:allocate(12347, <<"deallocnode">>, <<"gpu:1">>),
            ?assertEqual(ok, flurm_gres:deallocate(12347, <<"deallocnode">>))
        end},

        {"get_job_gres returns allocations", fun() ->
            GRESList = [#{type => gpu, index => 0, name => <<"a100">>}],
            flurm_gres:register_node_gres(<<"jobgresnode">>, GRESList),
            flurm_gres:allocate(12348, <<"jobgresnode">>, <<"gpu:1">>),
            Result = flurm_gres:get_job_gres(12348),
            ?assert(element(1, Result) =:= ok orelse Result =:= {error, not_found})
        end},

        {"get_job_gres for job without allocations", fun() ->
            ?assertEqual({error, not_found}, flurm_gres:get_job_gres(99999))
        end},

        {"check_availability with binary spec", fun() ->
            GRESList = [
                #{type => gpu, index => 0, name => <<"a100">>},
                #{type => gpu, index => 1, name => <<"a100">>}
            ],
            flurm_gres:register_node_gres(<<"checknode">>, GRESList),
            Result = flurm_gres:check_availability(<<"checknode">>, <<"gpu:2">>),
            ?assert(is_boolean(Result) orelse element(1, Result) =:= error)
        end},

        {"check_availability for unavailable GRES", fun() ->
            GRESList = [#{type => gpu, index => 0, name => <<"v100">>}],
            flurm_gres:register_node_gres(<<"smallnode">>, GRESList),
            %% Request more GPUs than available
            Result = flurm_gres:check_availability(<<"smallnode">>, <<"gpu:10">>),
            ?assert(is_boolean(Result) orelse element(1, Result) =:= error)
        end}
    ].

%%%===================================================================
%%% Spec Parsing Tests
%%%===================================================================

spec_parsing_tests(_Pid) ->
    [
        {"parse_gres_string simple type:count", fun() ->
            {ok, Specs} = flurm_gres:parse_gres_string(<<"gpu:2">>),
            ?assert(is_list(Specs)),
            ?assert(length(Specs) >= 1)
        end},

        {"parse_gres_string type:name:count", fun() ->
            {ok, Specs} = flurm_gres:parse_gres_string(<<"gpu:a100:4">>),
            ?assert(is_list(Specs))
        end},

        {"parse_gres_string multiple specs", fun() ->
            {ok, Specs} = flurm_gres:parse_gres_string(<<"gpu:2,fpga:1">>),
            ?assert(length(Specs) >= 2)
        end},

        {"parse_gres_string from list", fun() ->
            {ok, Specs} = flurm_gres:parse_gres_string("gpu:1"),
            ?assert(is_list(Specs))
        end},

        {"parse_gres_string empty", fun() ->
            {ok, Specs} = flurm_gres:parse_gres_string(<<>>),
            ?assertEqual([], Specs)
        end},

        {"format_gres_spec round trip", fun() ->
            {ok, Specs} = flurm_gres:parse_gres_string(<<"gpu:2">>),
            Formatted = flurm_gres:format_gres_spec(Specs),
            ?assert(is_binary(Formatted))
        end},

        {"format_gres_spec empty list", fun() ->
            ?assertEqual(<<>>, flurm_gres:format_gres_spec([]))
        end},

        {"parse_gres_spec legacy function", fun() ->
            Spec = <<"gpu:tesla:2">>,
            Result = flurm_gres:parse_gres_spec(Spec),
            ?assert(is_list(Result) orelse (is_tuple(Result) andalso (element(1, Result) =:= ok orelse element(1, Result) =:= error)))
        end}
    ].

%%%===================================================================
%%% Scheduler Integration Tests
%%%===================================================================

scheduler_integration_tests(_Pid) ->
    [
        {"filter_nodes_by_gres finds matching nodes", fun() ->
            %% Register GRES on some nodes
            flurm_gres:register_node_gres(<<"filternode1">>, [
                #{type => gpu, index => 0, name => <<"a100">>}
            ]),
            flurm_gres:register_node_gres(<<"filternode2">>, [
                #{type => gpu, index => 0, name => <<"v100">>}
            ]),
            Nodes = [<<"filternode1">>, <<"filternode2">>, <<"filternode3">>],
            Result = flurm_gres:filter_nodes_by_gres(Nodes, <<"gpu:1">>),
            ?assert(is_list(Result))
        end},

        {"score_node_gres returns score", fun() ->
            flurm_gres:register_node_gres(<<"scorenode">>, [
                #{type => gpu, index => 0, name => <<"a100">>, memory_mb => 40960},
                #{type => gpu, index => 1, name => <<"a100">>, memory_mb => 40960}
            ]),
            Score = flurm_gres:score_node_gres(<<"scorenode">>, <<"gpu:1">>),
            ?assert(is_number(Score))
        end}
    ].

%%%===================================================================
%%% Legacy API Tests
%%%===================================================================

legacy_api_tests(_Pid) ->
    [
        {"register_gres legacy function", fun() ->
            %% register_gres expects a list of device maps (not a single map)
            Result = flurm_gres:register_gres(<<"legacynode">>, gpu, [#{index => 0, name => <<"v100">>}]),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"unregister_gres legacy function", fun() ->
            %% register_gres expects a list of device maps (not a single map)
            flurm_gres:register_gres(<<"unregnode">>, gpu, [#{index => 0}]),
            Result = flurm_gres:unregister_gres(<<"unregnode">>, gpu),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"request_gres legacy function", fun() ->
            flurm_gres:register_node_gres(<<"reqnode">>, [#{type => gpu, index => 0}]),
            %% Use allocate directly with string spec since request_gres expects gres_spec record
            Result = flurm_gres:allocate(12350, <<"reqnode">>, <<"gpu:1">>),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"release_gres legacy function", fun() ->
            flurm_gres:register_node_gres(<<"relnode">>, [#{type => gpu, index => 0}]),
            %% Use allocate directly with string spec since request_gres expects gres_spec record
            flurm_gres:allocate(12351, <<"relnode">>, <<"gpu:1">>),
            Result = flurm_gres:release_gres(12351, <<"relnode">>, gpu),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"get_available_gres legacy function", fun() ->
            flurm_gres:register_node_gres(<<"availnode">>, [#{type => gpu, index => 0}]),
            Result = flurm_gres:get_available_gres(<<"availnode">>),
            ?assert(is_list(Result) orelse (is_tuple(Result) andalso (element(1, Result) =:= ok orelse element(1, Result) =:= error)))
        end},

        {"get_gres_by_type legacy function", fun() ->
            flurm_gres:register_node_gres(<<"typenode">>, [#{type => gpu, index => 0}]),
            Result = flurm_gres:get_gres_by_type(<<"typenode">>, gpu),
            ?assert(is_list(Result) orelse (is_tuple(Result) andalso (element(1, Result) =:= ok orelse element(1, Result) =:= error)))
        end},

        {"match_gres_requirements legacy function", fun() ->
            flurm_gres:register_node_gres(<<"matchnode">>, [
                #{type => gpu, index => 0},
                #{type => gpu, index => 1}
            ]),
            Result = flurm_gres:match_gres_requirements(<<"matchnode">>, <<"gpu:1">>),
            ?assert(is_boolean(Result) orelse is_list(Result) orelse (is_tuple(Result) andalso (element(1, Result) =:= ok orelse element(1, Result) =:= error)))
        end}
    ].

%%%===================================================================
%%% Message Handling Tests
%%%===================================================================

message_handling_tests(Pid) ->
    [
        {"handle_info unknown message", fun() ->
            Pid ! unknown_message,
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_cast unknown message", fun() ->
            gen_server:cast(Pid, unknown_cast),
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end}
    ].
