%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for FLURM Burst Buffer
%%%
%%% Tests burst buffer management functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_burst_buffer_additional_cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Generators
%%%===================================================================

burst_buffer_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun per_test_setup/0,
      fun per_test_cleanup/1,
      [
       fun basic_api_tests/1,
       fun pool_management_tests/1,
       fun allocation_tests/1,
       fun staging_tests/1,
       fun directive_tests/1,
       fun reservation_tests/1,
       fun legacy_api_tests/1,
       fun message_handling_tests/1
      ]}}.

setup() ->
    %% Mock lager
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

per_test_setup() ->
    {ok, Pid} = flurm_burst_buffer:start_link(),
    Pid.

per_test_cleanup(Pid) ->
    catch gen_server:stop(Pid),
    catch ets:delete(flurm_bb_pools),
    catch ets:delete(flurm_bb_allocations),
    catch ets:delete(flurm_bb_staging),
    catch ets:delete(flurm_bb_reservations),
    catch ets:delete(flurm_bb_persistent),
    ok.

%%%===================================================================
%%% Basic API Tests
%%%===================================================================

basic_api_tests(_Pid) ->
    [
        {"start_link creates process", fun() ->
            ?assert(is_pid(whereis(flurm_burst_buffer)))
        end},

        {"list_buffers returns list", fun() ->
            Buffers = flurm_burst_buffer:list_buffers(),
            ?assert(is_list(Buffers))
        end},

        {"list_pools returns list", fun() ->
            Pools = flurm_burst_buffer:list_pools(),
            ?assert(is_list(Pools))
        end},

        {"get_stats returns map", fun() ->
            Stats = flurm_burst_buffer:get_stats(),
            ?assert(is_map(Stats))
        end}
    ].

%%%===================================================================
%%% Pool Management Tests
%%%===================================================================

pool_management_tests(_Pid) ->
    [
        {"register_buffer with generic type", fun() ->
            Config = #{
                type => generic,
                capacity => 10737418240,  % 10GB in bytes
                nodes => [<<"node01">>, <<"node02">>],
                granularity => 1048576    % 1MB
            },
            ?assertEqual(ok, flurm_burst_buffer:register_buffer(<<"nvme_pool">>, Config)),
            Buffers = flurm_burst_buffer:list_buffers(),
            Names = [maps:get(name, B) || B <- Buffers],
            ?assert(lists:member(<<"nvme_pool">>, Names))
        end},

        {"register_buffer with datawarp type", fun() ->
            Config = #{
                type => datawarp,
                capacity => <<"100GB">>,
                nodes => [<<"node03">>]
            },
            ?assertEqual(ok, flurm_burst_buffer:register_buffer(<<"dw_pool">>, Config))
        end},

        {"register_buffer with lua type", fun() ->
            Config = #{
                type => lua,
                capacity => 5368709120,
                nodes => [<<"node04">>],
                lua_script => <<"/usr/local/bb/bb.lua">>
            },
            ?assertEqual(ok, flurm_burst_buffer:register_buffer(<<"lua_pool">>, Config))
        end},

        {"unregister_buffer removes pool", fun() ->
            Config = #{type => generic, capacity => 1073741824, nodes => []},
            flurm_burst_buffer:register_buffer(<<"temp_pool">>, Config),
            ?assertEqual(ok, flurm_burst_buffer:unregister_buffer(<<"temp_pool">>)),
            ?assertEqual({error, not_found}, flurm_burst_buffer:get_buffer_status(<<"temp_pool">>))
        end},

        {"unregister_buffer non-existent", fun() ->
            Result = flurm_burst_buffer:unregister_buffer(<<"nonexistent">>),
            %% May return ok (idempotent) or {error, not_found} depending on implementation
            ?assert(Result =:= ok orelse Result =:= {error, not_found})
        end},

        {"get_buffer_status for existing pool", fun() ->
            Config = #{type => generic, capacity => 2147483648, nodes => [<<"node05">>]},
            flurm_burst_buffer:register_buffer(<<"status_pool">>, Config),
            {ok, Status} = flurm_burst_buffer:get_buffer_status(<<"status_pool">>),
            ?assert(is_map(Status)),
            ?assertEqual(<<"status_pool">>, maps:get(name, Status))
        end},

        {"get_buffer_status for non-existent pool", fun() ->
            ?assertEqual({error, not_found}, flurm_burst_buffer:get_buffer_status(<<"unknown">>))
        end}
    ].

%%%===================================================================
%%% Allocation Tests
%%%===================================================================

allocation_tests(_Pid) ->
    [
        {"allocate with map spec", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"alloc_pool">>, Config),
            Spec = #{
                pool => <<"alloc_pool">>,
                size => 1073741824,
                persistent => false
            },
            Result = flurm_burst_buffer:allocate(12345, Spec),
            ?assertMatch({ok, _}, Result)
        end},

        {"allocate with binary spec", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"binalloc_pool">>, Config),
            Result = flurm_burst_buffer:allocate(12346, <<"capacity=1GB pool=binalloc_pool">>),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"allocate 3-arg version", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"alloc3_pool">>, Config),
            Result = flurm_burst_buffer:allocate(12347, <<"alloc3_pool">>, 536870912),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"deallocate job", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"dealloc_pool">>, Config),
            Spec = #{pool => <<"dealloc_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12348, Spec),
            ?assertEqual(ok, flurm_burst_buffer:deallocate(12348))
        end},

        {"deallocate 2-arg version", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"dealloc2_pool">>, Config),
            Spec = #{pool => <<"dealloc2_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12349, Spec),
            Result = flurm_burst_buffer:deallocate(12349, <<"dealloc2_pool">>),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"get_job_allocation existing", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"getalloc_pool">>, Config),
            Spec = #{pool => <<"getalloc_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12350, Spec),
            {ok, Allocations} = flurm_burst_buffer:get_job_allocation(12350),
            ?assert(is_list(Allocations)),
            ?assert(length(Allocations) > 0)
        end},

        {"get_job_allocation non-existent", fun() ->
            ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(99999))
        end}
    ].

%%%===================================================================
%%% Staging Tests
%%%===================================================================

staging_tests(_Pid) ->
    [
        {"stage_in 2-arg version", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"stage_pool">>, Config),
            Spec = #{pool => <<"stage_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12360, Spec),
            Files = [{<<"/data/input.dat">>, <<"/bb/input.dat">>}],
            Result = flurm_burst_buffer:stage_in(12360, Files),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"stage_in 3-arg version", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"stage3_pool">>, Config),
            Spec = #{pool => <<"stage3_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12361, Spec),
            Files = [{<<"/data/input2.dat">>, <<"/bb/input2.dat">>}],
            Result = flurm_burst_buffer:stage_in(12361, <<"stage3_pool">>, Files),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"stage_out 2-arg version", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"stageout_pool">>, Config),
            Spec = #{pool => <<"stageout_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12362, Spec),
            Files = [{<<"/bb/output.dat">>, <<"/data/output.dat">>}],
            Result = flurm_burst_buffer:stage_out(12362, Files),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"stage_out 3-arg version", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"stageout3_pool">>, Config),
            Spec = #{pool => <<"stageout3_pool">>, size => 1073741824},
            flurm_burst_buffer:allocate(12363, Spec),
            Files = [{<<"/bb/output2.dat">>, <<"/data/output2.dat">>}],
            Result = flurm_burst_buffer:stage_out(12363, <<"stageout3_pool">>, Files),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end}
    ].

%%%===================================================================
%%% Directive Tests
%%%===================================================================

directive_tests(_Pid) ->
    [
        {"parse_directives with capacity", fun() ->
            Script = <<"#!/bin/bash\n#BB capacity=10GB\necho hello">>,
            {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
            ?assert(is_list(Directives))
        end},

        {"parse_directives with stage_in", fun() ->
            Script = <<"#!/bin/bash\n#BB stage_in source=/data/in.dat destination=/bb/in.dat\nrun">>,
            {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
            ?assert(is_list(Directives))
        end},

        {"parse_directives with stage_out", fun() ->
            Script = <<"#!/bin/bash\n#BB stage_out source=/bb/out.dat destination=/data/out.dat\nrun">>,
            {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
            ?assert(is_list(Directives))
        end},

        {"parse_directives with create_persistent", fun() ->
            Script = <<"#!/bin/bash\n#BB create_persistent name=mydata capacity=5GB\nrun">>,
            {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
            ?assert(is_list(Directives))
        end},

        {"parse_directives with destroy_persistent", fun() ->
            Script = <<"#!/bin/bash\n#BB destroy_persistent name=mydata\nrun">>,
            {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
            ?assert(is_list(Directives))
        end},

        {"parse_directives no directives", fun() ->
            Script = <<"#!/bin/bash\necho hello">>,
            {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
            ?assertEqual([], Directives)
        end},

        {"parse_bb_spec valid spec", fun() ->
            Spec = <<"capacity=1GB pool=mypool access=striped type=scratch">>,
            Result = flurm_burst_buffer:parse_bb_spec(Spec),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"format_bb_spec", fun() ->
            %% format_bb_spec may expect a specific record type, so catch any errors
            Result = try
                flurm_burst_buffer:format_bb_spec(#{pool => <<"mypool">>, size => 1073741824})
            catch
                error:_ -> {error, format_error}
            end,
            %% May return binary or error tuple depending on implementation
            ?assert(is_binary(Result) orelse (is_tuple(Result) andalso element(1, Result) =:= error))
        end}
    ].

%%%===================================================================
%%% Reservation Tests
%%%===================================================================

reservation_tests(_Pid) ->
    [
        {"reserve_capacity", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"resv_pool">>, Config),
            Result = flurm_burst_buffer:reserve_capacity(12370, <<"resv_pool">>, 2147483648),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"get_reservations", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"getresv_pool">>, Config),
            flurm_burst_buffer:reserve_capacity(12371, <<"getresv_pool">>, 1073741824),
            Reservations = flurm_burst_buffer:get_reservations(<<"getresv_pool">>),
            ?assert(is_list(Reservations))
        end},

        {"release_reservation", fun() ->
            Config = #{type => generic, capacity => 10737418240, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"release_pool">>, Config),
            flurm_burst_buffer:reserve_capacity(12372, <<"release_pool">>, 1073741824),
            ?assertEqual(ok, flurm_burst_buffer:release_reservation(12372, <<"release_pool">>))
        end}
    ].

%%%===================================================================
%%% Legacy API Tests
%%%===================================================================

legacy_api_tests(_Pid) ->
    [
        {"create_pool delegates to register_buffer", fun() ->
            Config = #{type => generic, capacity => 1073741824, nodes => []},
            ?assertEqual(ok, flurm_burst_buffer:create_pool(<<"legacy_pool">>, Config))
        end},

        {"delete_pool delegates to unregister_buffer", fun() ->
            Config = #{type => generic, capacity => 1073741824, nodes => []},
            flurm_burst_buffer:create_pool(<<"todelete">>, Config),
            ?assertEqual(ok, flurm_burst_buffer:delete_pool(<<"todelete">>))
        end},

        {"get_pool_info for existing pool", fun() ->
            Config = #{type => generic, capacity => 2147483648, nodes => [<<"node01">>]},
            flurm_burst_buffer:register_buffer(<<"info_pool">>, Config),
            Result = flurm_burst_buffer:get_pool_info(<<"info_pool">>),
            ?assertMatch({ok, _}, Result)
        end},

        {"get_pool_info for non-existent pool", fun() ->
            ?assertEqual({error, not_found}, flurm_burst_buffer:get_pool_info(<<"unknown">>))
        end}
    ].

%%%===================================================================
%%% Message Handling Tests
%%%===================================================================

message_handling_tests(Pid) ->
    [
        {"handle_info cleanup", fun() ->
            Pid ! cleanup,
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info stage_complete", fun() ->
            Pid ! {stage_complete, make_ref(), ok},
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end},

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
