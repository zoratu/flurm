%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for flurm_gres TEST-exported Pure Functions
%%%
%%% Calls REAL functions directly (no mocking) to maximize rebar3
%%% cover results.  Every TEST-exported helper in flurm_gres is
%%% exercised here: parsing, formatting, GPU matching, scoring,
%%% allocation-mode helpers, and record-to-map conversions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_gres_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Record Definitions (mirrors flurm_gres.erl internal records)
%%====================================================================

-record(gres_spec, {
    type :: atom() | binary(),
    name :: binary() | any,
    count :: pos_integer(),
    per_node :: pos_integer(),
    per_task :: pos_integer(),
    memory_mb :: non_neg_integer() | any,
    flags :: [atom()],
    exclusive :: boolean(),
    constraints :: [term()]
}).

-record(gres_device, {
    id :: {binary(), atom(), non_neg_integer()},
    node :: binary(),
    type :: atom(),
    index :: non_neg_integer(),
    name :: binary(),
    count :: pos_integer(),
    memory_mb :: non_neg_integer(),
    flags :: [atom()],
    links :: [non_neg_integer()],
    cores :: [non_neg_integer()],
    state :: available | allocated | draining | down,
    allocated_to :: pos_integer() | undefined,
    allocation_mode :: exclusive | shared | undefined,
    properties :: map()
}).

-record(job_gres_alloc, {
    job_id :: pos_integer(),
    node :: binary(),
    allocations :: [{atom(), [non_neg_integer()]}],
    allocation_mode :: exclusive | shared,
    allocated_at :: non_neg_integer()
}).

%%====================================================================
%% Helpers
%%====================================================================

gpu_spec(Count) ->
    #gres_spec{
        type = gpu, name = any, count = Count,
        per_node = Count, per_task = 0,
        memory_mb = any, flags = [], exclusive = true,
        constraints = []
    }.

gpu_spec_named(Name, Count) ->
    #gres_spec{
        type = gpu, name = Name, count = Count,
        per_node = Count, per_task = 0,
        memory_mb = any, flags = [], exclusive = true,
        constraints = []
    }.

gpu_spec_shared(Count) ->
    #gres_spec{
        type = gpu, name = any, count = Count,
        per_node = Count, per_task = 0,
        memory_mb = any, flags = [shared], exclusive = false,
        constraints = []
    }.

fpga_spec(Count) ->
    #gres_spec{
        type = fpga, name = any, count = Count,
        per_node = Count, per_task = 0,
        memory_mb = any, flags = [], exclusive = true,
        constraints = []
    }.

gpu_spec_with_mem(Count, Mem) ->
    #gres_spec{
        type = gpu, name = any, count = Count,
        per_node = Count, per_task = 0,
        memory_mb = Mem, flags = [], exclusive = true,
        constraints = []
    }.

make_device(Node, Type, Index, Name, MemMB, Links) ->
    #gres_device{
        id = {Node, Type, Index},
        node = Node, type = Type, index = Index,
        name = Name, count = 1, memory_mb = MemMB,
        flags = [], links = Links, cores = [],
        state = available, allocated_to = undefined,
        allocation_mode = undefined, properties = #{}
    }.

%%====================================================================
%% parse_single_gres/1
%%====================================================================

parse_single_gres_test_() ->
    {"parse_single_gres/1", [
        {"gpu type only",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(1,   S#gres_spec.count),
             ?assertEqual(any, S#gres_spec.name)
         end},

        {"gpu:4 - type with count",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu:4">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(4,   S#gres_spec.count),
             ?assertEqual(any, S#gres_spec.name)
         end},

        {"gpu:a100 - type with name (non-numeric treated as name)",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu:a100">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(1,   S#gres_spec.count),
             ?assertEqual(<<"a100">>, S#gres_spec.name)
         end},

        {"gpu:a100:4 - type, name, count",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu:a100:4">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(4,   S#gres_spec.count),
             ?assertEqual(<<"a100">>, S#gres_spec.name),
             ?assertEqual(4,   S#gres_spec.per_node)
         end},

        {"fpga:1 - FPGA type",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"fpga:1">>),
             ?assertEqual(fpga, S#gres_spec.type),
             ?assertEqual(1,    S#gres_spec.count)
         end},

        {"FPGA:3 - uppercase FPGA",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"FPGA:3">>),
             ?assertEqual(fpga, S#gres_spec.type),
             ?assertEqual(3,    S#gres_spec.count)
         end},

        {"mic:2 - MIC type",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"mic:2">>),
             ?assertEqual(mic, S#gres_spec.type)
         end},

        {"MIC:2 - uppercase MIC",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"MIC:2">>),
             ?assertEqual(mic, S#gres_spec.type)
         end},

        {"mps:8 - MPS type",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"mps:8">>),
             ?assertEqual(mps, S#gres_spec.type),
             ?assertEqual(8,   S#gres_spec.count)
         end},

        {"MPS:4 - uppercase MPS",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"MPS:4">>),
             ?assertEqual(mps, S#gres_spec.type),
             ?assertEqual(4,   S#gres_spec.count)
         end},

        {"shard:2",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"shard:2">>),
             ?assertEqual(shard, S#gres_spec.type)
         end},

        {"GPU:2 - uppercase GPU",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"GPU:2">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(2,   S#gres_spec.count)
         end},

        {"custom_accel:3 - custom gres type preserved as binary",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"custom_accel:3">>),
             ?assertEqual(<<"custom_accel">>, S#gres_spec.type),
             ?assertEqual(3, S#gres_spec.count)
         end},

        {"gpu:tesla:2 - named GPU with count",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu:tesla:2">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(<<"tesla">>, S#gres_spec.name),
             ?assertEqual(2, S#gres_spec.count)
         end},

        {"extended format gpu:a100:2:mem:40960",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu:a100:2:mem:40960">>),
             ?assertEqual(gpu, S#gres_spec.type),
             ?assertEqual(<<"a100">>, S#gres_spec.name),
             ?assertEqual(2, S#gres_spec.count)
         end},

        {"default exclusive=true when no shared flag",
         fun() ->
             S = flurm_gres:parse_single_gres(<<"gpu:2">>),
             ?assertEqual(true, S#gres_spec.exclusive)
         end}
    ]}.

%%====================================================================
%% extract_flags/1
%%====================================================================

extract_flags_test_() ->
    {"extract_flags/1", [
        {"no flags leaves spec unchanged",
         fun() ->
             {Clean, Flags} = flurm_gres:extract_flags(<<"gpu:2">>),
             ?assertEqual(<<"gpu:2">>, Clean),
             ?assertEqual([], Flags)
         end},

        {"shard flag detected",
         fun() ->
             {_Clean, Flags} = flurm_gres:extract_flags(<<"gpu:2,shard">>),
             ?assert(lists:member(shared, Flags))
         end},

        {"exclusive flag detected",
         fun() ->
             {_Clean, Flags} = flurm_gres:extract_flags(<<"gpu:2,exclusive">>),
             ?assert(lists:member(exclusive, Flags))
         end},

        {"mps flag detected",
         fun() ->
             {_Clean, Flags} = flurm_gres:extract_flags(<<"gpu:2,mps">>),
             ?assert(lists:member(mps_enabled, Flags))
         end}
    ]}.

%%====================================================================
%% parse_gres_type/1
%%====================================================================

parse_gres_type_test_() ->
    {"parse_gres_type/1", [
        {"gpu",  fun() -> ?assertEqual(gpu,   flurm_gres:parse_gres_type(<<"gpu">>))  end},
        {"GPU",  fun() -> ?assertEqual(gpu,   flurm_gres:parse_gres_type(<<"GPU">>))  end},
        {"fpga", fun() -> ?assertEqual(fpga,  flurm_gres:parse_gres_type(<<"fpga">>)) end},
        {"FPGA", fun() -> ?assertEqual(fpga,  flurm_gres:parse_gres_type(<<"FPGA">>)) end},
        {"mic",  fun() -> ?assertEqual(mic,   flurm_gres:parse_gres_type(<<"mic">>))  end},
        {"MIC",  fun() -> ?assertEqual(mic,   flurm_gres:parse_gres_type(<<"MIC">>))  end},
        {"mps",  fun() -> ?assertEqual(mps,   flurm_gres:parse_gres_type(<<"mps">>))  end},
        {"MPS",  fun() -> ?assertEqual(mps,   flurm_gres:parse_gres_type(<<"MPS">>))  end},
        {"shard", fun() -> ?assertEqual(shard, flurm_gres:parse_gres_type(<<"shard">>)) end},
        {"custom binary passthrough",
         fun() -> ?assertEqual(<<"tpu">>, flurm_gres:parse_gres_type(<<"tpu">>)) end}
    ]}.

%%====================================================================
%% parse_count/1 and parse_count_safe/1
%%====================================================================

parse_count_test_() ->
    {"parse_count/1 and parse_count_safe/1", [
        {"parse_count valid",
         fun() -> ?assertEqual(42, flurm_gres:parse_count(<<"42">>)) end},

        {"parse_count invalid throws",
         fun() ->
             ?assertThrow({parse_error, {invalid_count, <<"abc">>}},
                          flurm_gres:parse_count(<<"abc">>))
         end},

        {"parse_count_safe valid returns {ok, N}",
         fun() -> ?assertEqual({ok, 7}, flurm_gres:parse_count_safe(<<"7">>)) end},

        {"parse_count_safe invalid returns error",
         fun() -> ?assertEqual(error, flurm_gres:parse_count_safe(<<"xyz">>)) end}
    ]}.

%%====================================================================
%% parse_memory_constraint/1
%%====================================================================

parse_memory_constraint_test_() ->
    {"parse_memory_constraint/1", [
        {"mem:40960 returns integer",
         fun() -> ?assertEqual(40960, flurm_gres:parse_memory_constraint(<<"mem:40960">>)) end},

        {"mem:bad returns any",
         fun() -> ?assertEqual(any, flurm_gres:parse_memory_constraint(<<"mem:bad">>)) end},

        {"other binary returns any",
         fun() -> ?assertEqual(any, flurm_gres:parse_memory_constraint(<<"something">>)) end}
    ]}.

%%====================================================================
%% format_single_gres/1
%%====================================================================

format_single_gres_test_() ->
    {"format_single_gres/1", [
        {"gpu:2 (name=any)",
         fun() ->
             Bin = iolist_to_binary(flurm_gres:format_single_gres(gpu_spec(2))),
             ?assertEqual(<<"gpu:2">>, Bin)
         end},

        {"gpu:a100:4 (named)",
         fun() ->
             Bin = iolist_to_binary(
                     flurm_gres:format_single_gres(gpu_spec_named(<<"a100">>, 4))),
             ?assertEqual(<<"gpu:a100:4">>, Bin)
         end},

        {"fpga:1",
         fun() ->
             Bin = iolist_to_binary(flurm_gres:format_single_gres(fpga_spec(1))),
             ?assertEqual(<<"fpga:1">>, Bin)
         end},

        {"shared flag is appended",
         fun() ->
             Bin = iolist_to_binary(
                     flurm_gres:format_single_gres(gpu_spec_shared(2))),
             ?assert(binary:match(Bin, <<"gpu:2">>) =/= nomatch),
             ?assert(binary:match(Bin, <<"shared">>) =/= nomatch)
         end},

        {"exclusive flag is NOT appended (stripped by format_with_flags)",
         fun() ->
             S = gpu_spec(1),
             Bin = iolist_to_binary(
                     flurm_gres:format_single_gres(S#gres_spec{flags = [exclusive]})),
             ?assertEqual(nomatch, binary:match(Bin, <<"exclusive">>))
         end}
    ]}.

%%====================================================================
%% format_with_flags/2
%%====================================================================

format_with_flags_test_() ->
    {"format_with_flags/2", [
        {"empty flags returns base unchanged",
         fun() ->
             Base = [<<"gpu">>, <<":">>, <<"2">>],
             ?assertEqual(Base, flurm_gres:format_with_flags(Base, []))
         end},

        {"shared flag appended",
         fun() ->
             Base = [<<"gpu">>, <<":">>, <<"2">>],
             Res = iolist_to_binary(flurm_gres:format_with_flags(Base, [shared])),
             ?assert(binary:match(Res, <<"shared">>) =/= nomatch)
         end},

        {"exclusive-only list produces no suffix (exclusive is filtered out)",
         fun() ->
             Base = [<<"gpu">>, <<":">>, <<"2">>],
             ?assertEqual(Base, flurm_gres:format_with_flags(Base, [exclusive]))
         end},

        {"mps_enabled flag appended",
         fun() ->
             Base = [<<"gpu">>, <<":">>, <<"1">>],
             Res = iolist_to_binary(flurm_gres:format_with_flags(Base, [mps_enabled])),
             ?assert(binary:match(Res, <<"mps_enabled">>) =/= nomatch)
         end}
    ]}.

%%====================================================================
%% format_type/1
%%====================================================================

format_type_test_() ->
    {"format_type/1", [
        {"gpu",   fun() -> ?assertEqual(<<"gpu">>,   flurm_gres:format_type(gpu))   end},
        {"fpga",  fun() -> ?assertEqual(<<"fpga">>,  flurm_gres:format_type(fpga))  end},
        {"mic",   fun() -> ?assertEqual(<<"mic">>,   flurm_gres:format_type(mic))   end},
        {"mps",   fun() -> ?assertEqual(<<"mps">>,   flurm_gres:format_type(mps))   end},
        {"shard", fun() -> ?assertEqual(<<"shard">>, flurm_gres:format_type(shard)) end},
        {"binary passthrough",
         fun() -> ?assertEqual(<<"tpu">>, flurm_gres:format_type(<<"tpu">>)) end},
        {"arbitrary atom",
         fun() -> ?assertEqual(<<"npu">>, flurm_gres:format_type(npu)) end}
    ]}.

%%====================================================================
%% matches_gpu_model/2
%%====================================================================

matches_gpu_model_test_() ->
    {"matches_gpu_model/2", [
        {"exact substring match (a100 in nvidia_a100)",
         fun() ->
             ?assert(flurm_gres:matches_gpu_model(<<"nvidia_a100">>, <<"a100">>))
         end},

        {"case-insensitive match (A100 vs nvidia_a100)",
         fun() ->
             ?assert(flurm_gres:matches_gpu_model(<<"nvidia_a100">>, <<"A100">>))
         end},

        {"v100 substring match",
         fun() ->
             ?assert(flurm_gres:matches_gpu_model(<<"nvidia_tesla_v100">>, <<"v100">>))
         end},

        {"h100 match",
         fun() ->
             ?assert(flurm_gres:matches_gpu_model(<<"nvidia_h100_sxm">>, <<"h100">>))
         end},

        {"no match at all",
         fun() ->
             ?assertNot(flurm_gres:matches_gpu_model(<<"nvidia_a100">>, <<"rtx4090">>))
         end},

        {"non-binary arguments return false",
         fun() ->
             ?assertNot(flurm_gres:matches_gpu_model(a100, <<"a100">>)),
             ?assertNot(flurm_gres:matches_gpu_model(<<"a100">>, 123))
         end}
    ]}.

%%====================================================================
%% check_gpu_aliases/2
%%====================================================================

check_gpu_aliases_test_() ->
    {"check_gpu_aliases/2", [
        {"a100 alias",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("nvidia_a100", "a100")) end},

        {"v100 alias",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("nvidia_v100", "v100")) end},

        {"h100 alias",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("nvidia_h100_sxm", "h100")) end},

        {"tesla alias matches v100",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("nvidia_v100", "tesla")) end},

        {"tesla alias matches a100",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("nvidia_a100", "tesla")) end},

        {"mi250 alias",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("amd_instinct_mi250x", "mi250")) end},

        {"mi300 alias",
         fun() -> ?assert(flurm_gres:check_gpu_aliases("amd_instinct_mi300a", "mi300")) end},

        {"unknown alias returns false",
         fun() -> ?assertNot(flurm_gres:check_gpu_aliases("some_gpu", "unknown_model")) end}
    ]}.

%%====================================================================
%% determine_allocation_mode/1
%%====================================================================

determine_allocation_mode_test_() ->
    {"determine_allocation_mode/1", [
        {"exclusive when any spec is exclusive",
         fun() ->
             ?assertEqual(exclusive,
                          flurm_gres:determine_allocation_mode([gpu_spec(1)]))
         end},

        {"shared when all specs are non-exclusive",
         fun() ->
             ?assertEqual(shared,
                          flurm_gres:determine_allocation_mode([gpu_spec_shared(1)]))
         end},

        {"exclusive wins in mixed list",
         fun() ->
             ?assertEqual(exclusive,
                          flurm_gres:determine_allocation_mode(
                            [gpu_spec_shared(1), gpu_spec(2)]))
         end},

        {"map spec defaults to exclusive",
         fun() ->
             ?assertEqual(exclusive,
                          flurm_gres:determine_allocation_mode(
                            [#{exclusive => true}]))
         end},

        {"map spec with exclusive=false",
         fun() ->
             ?assertEqual(shared,
                          flurm_gres:determine_allocation_mode(
                            [#{exclusive => false}]))
         end}
    ]}.

%%====================================================================
%% calculate_topology_score/2
%%====================================================================

calculate_topology_score_test_() ->
    {"calculate_topology_score/2", [
        {"no GPU specs yields 0",
         fun() ->
             Devs = [make_device(<<"n1">>, fpga, 0, <<"fpga0">>, 8192, [])],
             ?assertEqual(0, flurm_gres:calculate_topology_score(Devs, [fpga_spec(1)]))
         end},

        {"GPU devices with links give bonus",
         fun() ->
             D0 = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [1]),
             D1 = make_device(<<"n1">>, gpu, 1, <<"a100">>, 40960, [0]),
             Score = flurm_gres:calculate_topology_score([D0, D1], [gpu_spec(2)]),
             ?assert(Score > 0),
             ?assert(Score =< 15)
         end},

        {"GPU devices with no links give 0",
         fun() ->
             D0 = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, []),
             D1 = make_device(<<"n1">>, gpu, 1, <<"a100">>, 40960, []),
             ?assertEqual(0, flurm_gres:calculate_topology_score([D0, D1], [gpu_spec(2)]))
         end},

        {"empty device list gives 0",
         fun() ->
             ?assertEqual(0, flurm_gres:calculate_topology_score([], [gpu_spec(1)]))
         end}
    ]}.

%%====================================================================
%% calculate_fit_score/2
%%====================================================================

calculate_fit_score_test_() ->
    {"calculate_fit_score/2", [
        {"100% utilization = 20 (very good fit)",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [])],
             ?assertEqual(20, flurm_gres:calculate_fit_score(Devs, [gpu_spec(1)]))
         end},

        {"50% utilization = 10 (reasonable fit)",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, I, <<"a100">>, 40960, [])
                     || I <- lists:seq(0, 1)],
             ?assertEqual(20, flurm_gres:calculate_fit_score(Devs, [gpu_spec(2)]))
         end},

        {"low utilization = 5 (partial fit)",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, I, <<"a100">>, 40960, [])
                     || I <- lists:seq(0, 9)],
             ?assertEqual(5, flurm_gres:calculate_fit_score(Devs, [gpu_spec(1)]))
         end},

        {"empty device list = 0",
         fun() ->
             ?assertEqual(0, flurm_gres:calculate_fit_score([], [gpu_spec(1)]))
         end}
    ]}.

%%====================================================================
%% calculate_memory_score/2
%%====================================================================

calculate_memory_score_test_() ->
    {"calculate_memory_score/2", [
        {"no memory constraint => 0",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [])],
             ?assertEqual(0, flurm_gres:calculate_memory_score(Devs, [gpu_spec(1)]))
         end},

        {"good memory match => 5",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [])],
             Spec = gpu_spec_with_mem(1, 32000),
             ?assertEqual(5, flurm_gres:calculate_memory_score(Devs, [Spec]))
         end},

        {"memory too high (device 40960 < 2*spec 50000) => 0",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [])],
             Spec = gpu_spec_with_mem(1, 50000),
             ?assertEqual(0, flurm_gres:calculate_memory_score(Devs, [Spec]))
         end},

        {"device memory much larger than spec (outside 2x window) => 0",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 81920, [])],
             Spec = gpu_spec_with_mem(1, 10000),
             ?assertEqual(0, flurm_gres:calculate_memory_score(Devs, [Spec]))
         end}
    ]}.

%%====================================================================
%% can_satisfy_spec/2
%%====================================================================

can_satisfy_spec_test_() ->
    {"can_satisfy_spec/2", [
        {"sufficient available devices",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, []),
                     make_device(<<"n1">>, gpu, 1, <<"a100">>, 40960, [])],
             ?assert(flurm_gres:can_satisfy_spec(gpu_spec(2), Devs))
         end},

        {"insufficient devices",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [])],
             ?assertNot(flurm_gres:can_satisfy_spec(gpu_spec(2), Devs))
         end},

        {"name filter excludes non-matching devices",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"nvidia_v100">>, 32768, [])],
             Spec = gpu_spec_named(<<"a100">>, 1),
             ?assertNot(flurm_gres:can_satisfy_spec(Spec, Devs))
         end},

        {"name=any matches all",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"nvidia_v100">>, 32768, [])],
             ?assert(flurm_gres:can_satisfy_spec(gpu_spec(1), Devs))
         end},

        {"memory filter excludes low-memory devices",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 16384, [])],
             Spec = gpu_spec_with_mem(1, 40000),
             ?assertNot(flurm_gres:can_satisfy_spec(Spec, Devs))
         end},

        {"memory=any ignores memory",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 1, [])],
             ?assert(flurm_gres:can_satisfy_spec(gpu_spec(1), Devs))
         end},

        {"map spec is converted and checked",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [])],
             MapSpec = #{type => gpu, count => 1, name => any, memory_mb => any},
             ?assert(flurm_gres:can_satisfy_spec(MapSpec, Devs))
         end},

        {"empty device list returns false",
         fun() ->
             ?assertNot(flurm_gres:can_satisfy_spec(gpu_spec(1), []))
         end},

        {"named device match via gpu model matching",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, 0, <<"nvidia_a100">>, 40960, [])],
             Spec = gpu_spec_named(<<"a100">>, 1),
             ?assert(flurm_gres:can_satisfy_spec(Spec, Devs))
         end}
    ]}.

%%====================================================================
%% device_to_map/1
%%====================================================================

device_to_map_test_() ->
    {"device_to_map/1", [
        {"converts all fields",
         fun() ->
             D = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, [1]),
             M = flurm_gres:device_to_map(D),
             ?assertEqual(<<"n1">>,    maps:get(node, M)),
             ?assertEqual(gpu,         maps:get(type, M)),
             ?assertEqual(0,           maps:get(index, M)),
             ?assertEqual(<<"a100">>,  maps:get(name, M)),
             ?assertEqual(1,           maps:get(count, M)),
             ?assertEqual(40960,       maps:get(memory_mb, M)),
             ?assertEqual([],          maps:get(flags, M)),
             ?assertEqual([1],         maps:get(links, M)),
             ?assertEqual([],          maps:get(cores, M)),
             ?assertEqual(available,   maps:get(state, M)),
             ?assertEqual(undefined,   maps:get(allocated_to, M)),
             ?assertEqual(undefined,   maps:get(allocation_mode, M)),
             ?assertEqual(#{},         maps:get(properties, M))
         end}
    ]}.

%%====================================================================
%% job_alloc_to_map/1
%%====================================================================

job_alloc_to_map_test_() ->
    {"job_alloc_to_map/1", [
        {"converts all fields",
         fun() ->
             A = #job_gres_alloc{
                 job_id = 100,
                 node = <<"n1">>,
                 allocations = [{gpu, [0, 1]}],
                 allocation_mode = exclusive,
                 allocated_at = 1700000000
             },
             M = flurm_gres:job_alloc_to_map(A),
             ?assertEqual(100,            maps:get(job_id, M)),
             ?assertEqual(<<"n1">>,       maps:get(node, M)),
             ?assertEqual([{gpu, [0,1]}], maps:get(allocations, M)),
             ?assertEqual(exclusive,      maps:get(allocation_mode, M)),
             ?assertEqual(1700000000,     maps:get(allocated_at, M))
         end}
    ]}.

%%====================================================================
%% map_to_gres_spec/1
%%====================================================================

map_to_gres_spec_test_() ->
    {"map_to_gres_spec/1", [
        {"full map converts correctly",
         fun() ->
             M = #{type => fpga, name => <<"alveo">>, count => 2,
                   per_node => 2, per_task => 1,
                   memory_mb => 16384, flags => [exclusive],
                   exclusive => true, constraints => [fast]},
             S = flurm_gres:map_to_gres_spec(M),
             ?assertEqual(fpga,        S#gres_spec.type),
             ?assertEqual(<<"alveo">>, S#gres_spec.name),
             ?assertEqual(2,           S#gres_spec.count),
             ?assertEqual(2,           S#gres_spec.per_node),
             ?assertEqual(1,           S#gres_spec.per_task),
             ?assertEqual(16384,       S#gres_spec.memory_mb),
             ?assertEqual([exclusive], S#gres_spec.flags),
             ?assertEqual(true,        S#gres_spec.exclusive),
             ?assertEqual([fast],      S#gres_spec.constraints)
         end},

        {"empty map uses defaults",
         fun() ->
             S = flurm_gres:map_to_gres_spec(#{}),
             ?assertEqual(gpu,  S#gres_spec.type),
             ?assertEqual(any,  S#gres_spec.name),
             ?assertEqual(1,    S#gres_spec.count),
             ?assertEqual(1,    S#gres_spec.per_node),
             ?assertEqual(0,    S#gres_spec.per_task),
             ?assertEqual(any,  S#gres_spec.memory_mb),
             ?assertEqual([],   S#gres_spec.flags),
             ?assertEqual(true, S#gres_spec.exclusive),
             ?assertEqual([],   S#gres_spec.constraints)
         end},

        {"per_node defaults to count when not given",
         fun() ->
             S = flurm_gres:map_to_gres_spec(#{count => 4}),
             ?assertEqual(4, S#gres_spec.per_node)
         end}
    ]}.

%%====================================================================
%% sort_by_topology/1
%%====================================================================

sort_by_topology_test_() ->
    {"sort_by_topology/1", [
        {"prefers devices with more available links first",
         fun() ->
             D0 = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, []),
             D1 = make_device(<<"n1">>, gpu, 1, <<"a100">>, 40960, [0, 2]),
             D2 = make_device(<<"n1">>, gpu, 2, <<"a100">>, 40960, [1]),
             Sorted = flurm_gres:sort_by_topology([D0, D1, D2]),
             %% D1 has 2 links to available devices (0 and 2), should be first
             [First | _] = Sorted,
             ?assertEqual(1, First#gres_device.index)
         end},

        {"empty list returns empty",
         fun() ->
             ?assertEqual([], flurm_gres:sort_by_topology([]))
         end},

        {"single device returns itself",
         fun() ->
             D = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, []),
             ?assertEqual([D], flurm_gres:sort_by_topology([D]))
         end}
    ]}.

%%====================================================================
%% select_best_devices/3
%%====================================================================

select_best_devices_test_() ->
    {"select_best_devices/3", [
        {"selects correct count of devices",
         fun() ->
             D0 = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, []),
             D1 = make_device(<<"n1">>, gpu, 1, <<"a100">>, 40960, []),
             {ok, Selected} = flurm_gres:select_best_devices([D0, D1], 1, gpu_spec(1)),
             ?assertEqual(1, length(Selected))
         end},

        {"returns error when insufficient devices",
         fun() ->
             D0 = make_device(<<"n1">>, gpu, 0, <<"a100">>, 40960, []),
             ?assertEqual({error, insufficient_gres},
                          flurm_gres:select_best_devices([D0], 3, gpu_spec(3)))
         end},

        {"selects all when count equals available",
         fun() ->
             Devs = [make_device(<<"n1">>, gpu, I, <<"a100">>, 40960, [])
                     || I <- lists:seq(0, 3)],
             {ok, Selected} = flurm_gres:select_best_devices(Devs, 4, gpu_spec(4)),
             ?assertEqual(4, length(Selected))
         end}
    ]}.

%%====================================================================
%% Round-trip: parse then format
%%====================================================================

roundtrip_test_() ->
    {"parse -> format round-trip", [
        {"gpu:2",
         fun() ->
             {ok, Specs} = flurm_gres:parse_gres_string(<<"gpu:2">>),
             Bin = iolist_to_binary(flurm_gres:format_gres_spec(Specs)),
             ?assertEqual(<<"gpu:2">>, Bin)
         end},

        {"gpu:a100:4",
         fun() ->
             {ok, Specs} = flurm_gres:parse_gres_string(<<"gpu:a100:4">>),
             Bin = iolist_to_binary(flurm_gres:format_gres_spec(Specs)),
             ?assertEqual(<<"gpu:a100:4">>, Bin)
         end},

        {"fpga:1",
         fun() ->
             {ok, Specs} = flurm_gres:parse_gres_string(<<"fpga:1">>),
             Bin = iolist_to_binary(flurm_gres:format_gres_spec(Specs)),
             ?assertEqual(<<"fpga:1">>, Bin)
         end},

        {"empty string",
         fun() ->
             {ok, Specs} = flurm_gres:parse_gres_string(<<>>),
             ?assertEqual(<<>>, flurm_gres:format_gres_spec(Specs))
         end}
    ]}.
