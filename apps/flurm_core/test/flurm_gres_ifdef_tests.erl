%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_gres internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_gres_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Parse Single GRES Tests
%%====================================================================

parse_single_gres_type_only_test() ->
    Spec = flurm_gres:parse_single_gres(<<"gpu">>),
    ?assertEqual(gpu, element(2, Spec)),
    ?assertEqual(1, element(4, Spec)).

parse_single_gres_type_count_test() ->
    Spec = flurm_gres:parse_single_gres(<<"gpu:4">>),
    ?assertEqual(gpu, element(2, Spec)),
    ?assertEqual(4, element(4, Spec)).

parse_single_gres_type_name_count_test() ->
    Spec = flurm_gres:parse_single_gres(<<"gpu:a100:2">>),
    ?assertEqual(gpu, element(2, Spec)),
    ?assertEqual(<<"a100">>, element(3, Spec)),
    ?assertEqual(2, element(4, Spec)).

parse_single_gres_with_name_no_count_test() ->
    Spec = flurm_gres:parse_single_gres(<<"gpu:tesla">>),
    ?assertEqual(gpu, element(2, Spec)),
    ?assertEqual(<<"tesla">>, element(3, Spec)),
    ?assertEqual(1, element(4, Spec)).

%%====================================================================
%% Extract Flags Tests
%%====================================================================

extract_flags_no_flags_test() ->
    {MainPart, Flags} = flurm_gres:extract_flags(<<"gpu:2">>),
    ?assertEqual(<<"gpu:2">>, MainPart),
    ?assertEqual([], Flags).

extract_flags_shared_test() ->
    {_MainPart, Flags} = flurm_gres:extract_flags(<<"gpu:2,shard">>),
    ?assert(lists:member(shared, Flags)).

extract_flags_exclusive_test() ->
    {_MainPart, Flags} = flurm_gres:extract_flags(<<"gpu:2,exclusive">>),
    ?assert(lists:member(exclusive, Flags)).

extract_flags_mps_test() ->
    {_MainPart, Flags} = flurm_gres:extract_flags(<<"gpu:2,mps">>),
    ?assert(lists:member(mps_enabled, Flags)).

%%====================================================================
%% Parse GRES Type Tests
%%====================================================================

parse_gres_type_gpu_test() ->
    ?assertEqual(gpu, flurm_gres:parse_gres_type(<<"gpu">>)),
    ?assertEqual(gpu, flurm_gres:parse_gres_type(<<"GPU">>)).

parse_gres_type_fpga_test() ->
    ?assertEqual(fpga, flurm_gres:parse_gres_type(<<"fpga">>)),
    ?assertEqual(fpga, flurm_gres:parse_gres_type(<<"FPGA">>)).

parse_gres_type_mic_test() ->
    ?assertEqual(mic, flurm_gres:parse_gres_type(<<"mic">>)),
    ?assertEqual(mic, flurm_gres:parse_gres_type(<<"MIC">>)).

parse_gres_type_mps_test() ->
    ?assertEqual(mps, flurm_gres:parse_gres_type(<<"mps">>)),
    ?assertEqual(mps, flurm_gres:parse_gres_type(<<"MPS">>)).

parse_gres_type_custom_test() ->
    ?assertEqual(<<"custom_type">>, flurm_gres:parse_gres_type(<<"custom_type">>)).

%%====================================================================
%% Parse Count Tests
%%====================================================================

parse_count_valid_test() ->
    ?assertEqual(1, flurm_gres:parse_count(<<"1">>)),
    ?assertEqual(10, flurm_gres:parse_count(<<"10">>)),
    ?assertEqual(100, flurm_gres:parse_count(<<"100">>)).

parse_count_invalid_test() ->
    ?assertThrow({parse_error, {invalid_count, <<"abc">>}}, flurm_gres:parse_count(<<"abc">>)).

parse_count_safe_valid_test() ->
    ?assertEqual({ok, 5}, flurm_gres:parse_count_safe(<<"5">>)),
    ?assertEqual({ok, 42}, flurm_gres:parse_count_safe(<<"42">>)).

parse_count_safe_invalid_test() ->
    ?assertEqual(error, flurm_gres:parse_count_safe(<<"invalid">>)).

%%====================================================================
%% Parse Memory Constraint Tests
%%====================================================================

parse_memory_constraint_valid_test() ->
    ?assertEqual(8192, flurm_gres:parse_memory_constraint(<<"mem:8192">>)).

parse_memory_constraint_no_prefix_test() ->
    ?assertEqual(any, flurm_gres:parse_memory_constraint(<<"8192">>)).

parse_memory_constraint_invalid_test() ->
    ?assertEqual(any, flurm_gres:parse_memory_constraint(<<"mem:abc">>)).

%%====================================================================
%% Format Type Tests
%%====================================================================

format_type_atoms_test() ->
    ?assertEqual(<<"gpu">>, flurm_gres:format_type(gpu)),
    ?assertEqual(<<"fpga">>, flurm_gres:format_type(fpga)),
    ?assertEqual(<<"mic">>, flurm_gres:format_type(mic)),
    ?assertEqual(<<"mps">>, flurm_gres:format_type(mps)),
    ?assertEqual(<<"shard">>, flurm_gres:format_type(shard)).

format_type_binary_test() ->
    ?assertEqual(<<"custom">>, flurm_gres:format_type(<<"custom">>)).

format_type_other_atom_test() ->
    ?assertEqual(<<"other">>, flurm_gres:format_type(other)).

%%====================================================================
%% GPU Model Matching Tests
%%====================================================================

matches_gpu_model_exact_test() ->
    ?assert(flurm_gres:matches_gpu_model(<<"nvidia_a100">>, <<"a100">>)),
    ?assert(flurm_gres:matches_gpu_model(<<"nvidia_v100">>, <<"v100">>)).

matches_gpu_model_substring_test() ->
    ?assert(flurm_gres:matches_gpu_model(<<"NVIDIA Tesla V100">>, <<"v100">>)),
    ?assert(flurm_gres:matches_gpu_model(<<"nvidia_a100_80gb">>, <<"a100">>)).

matches_gpu_model_no_match_test() ->
    ?assertNot(flurm_gres:matches_gpu_model(<<"nvidia_a100">>, <<"v100">>)),
    ?assertNot(flurm_gres:matches_gpu_model(<<"amd_mi100">>, <<"nvidia">>)).

matches_gpu_model_non_binary_test() ->
    ?assertNot(flurm_gres:matches_gpu_model(atom, <<"test">>)),
    ?assertNot(flurm_gres:matches_gpu_model(<<"test">>, atom)).

%%====================================================================
%% Check GPU Aliases Tests
%%====================================================================

check_gpu_aliases_a100_test() ->
    ?assert(flurm_gres:check_gpu_aliases("nvidia_a100", "a100")).

check_gpu_aliases_v100_test() ->
    ?assert(flurm_gres:check_gpu_aliases("nvidia_v100", "v100")).

check_gpu_aliases_h100_test() ->
    ?assert(flurm_gres:check_gpu_aliases("nvidia_h100", "h100")).

check_gpu_aliases_tesla_v100_test() ->
    ?assert(flurm_gres:check_gpu_aliases("nvidia_v100", "tesla")).

check_gpu_aliases_tesla_a100_test() ->
    ?assert(flurm_gres:check_gpu_aliases("nvidia_a100", "tesla")).

check_gpu_aliases_mi250_test() ->
    ?assert(flurm_gres:check_gpu_aliases("amd_mi250x", "mi250")).

check_gpu_aliases_mi300_test() ->
    ?assert(flurm_gres:check_gpu_aliases("amd_mi300", "mi300")).

check_gpu_aliases_no_match_test() ->
    ?assertNot(flurm_gres:check_gpu_aliases("nvidia_rtx3090", "a100")).

%%====================================================================
%% Map to GRES Spec Tests
%%====================================================================

map_to_gres_spec_defaults_test() ->
    Spec = flurm_gres:map_to_gres_spec(#{}),
    ?assertEqual(gpu, element(2, Spec)),
    ?assertEqual(any, element(3, Spec)),
    ?assertEqual(1, element(4, Spec)).

map_to_gres_spec_custom_test() ->
    Spec = flurm_gres:map_to_gres_spec(#{
        type => fpga,
        name => <<"xilinx">>,
        count => 2,
        exclusive => false
    }),
    ?assertEqual(fpga, element(2, Spec)),
    ?assertEqual(<<"xilinx">>, element(3, Spec)),
    ?assertEqual(2, element(4, Spec)).

%%====================================================================
%% Determine Allocation Mode Tests
%%====================================================================

determine_allocation_mode_exclusive_test() ->
    Spec1 = flurm_gres:map_to_gres_spec(#{exclusive => true}),
    ?assertEqual(exclusive, flurm_gres:determine_allocation_mode([Spec1])).

determine_allocation_mode_shared_test() ->
    Spec1 = flurm_gres:map_to_gres_spec(#{exclusive => false}),
    ?assertEqual(shared, flurm_gres:determine_allocation_mode([Spec1])).

determine_allocation_mode_mixed_test() ->
    Spec1 = flurm_gres:map_to_gres_spec(#{exclusive => true}),
    Spec2 = flurm_gres:map_to_gres_spec(#{exclusive => false}),
    ?assertEqual(exclusive, flurm_gres:determine_allocation_mode([Spec1, Spec2])).

determine_allocation_mode_map_test() ->
    ?assertEqual(exclusive, flurm_gres:determine_allocation_mode([#{exclusive => true}])),
    ?assertEqual(shared, flurm_gres:determine_allocation_mode([#{exclusive => false}])).

%%====================================================================
%% Sort By Topology Tests
%%====================================================================

sort_by_topology_empty_test() ->
    ?assertEqual([], flurm_gres:sort_by_topology([])).

sort_by_topology_no_links_test() ->
    %% Create mock device records (tuple position matches #gres_device record)
    Device1 = {gres_device, {<<"node1">>, gpu, 0}, <<"node1">>, gpu, 0, <<"a100">>,
               1, 40960, [], [], [], available, undefined, undefined, #{}},
    Device2 = {gres_device, {<<"node1">>, gpu, 1}, <<"node1">>, gpu, 1, <<"a100">>,
               1, 40960, [], [], [], available, undefined, undefined, #{}},
    Devices = [Device1, Device2],
    Sorted = flurm_gres:sort_by_topology(Devices),
    ?assertEqual(2, length(Sorted)).

%%====================================================================
%% Scoring Tests
%%====================================================================

calculate_topology_score_no_gpus_test() ->
    ?assertEqual(0, flurm_gres:calculate_topology_score([], [])).

calculate_fit_score_empty_test() ->
    ?assertEqual(0, flurm_gres:calculate_fit_score([], [])).

calculate_memory_score_no_specs_test() ->
    ?assertEqual(0, flurm_gres:calculate_memory_score([], [])).

%%====================================================================
%% Can Satisfy Spec Tests
%%====================================================================

can_satisfy_spec_empty_available_test() ->
    Spec = flurm_gres:map_to_gres_spec(#{type => gpu, count => 1}),
    ?assertNot(flurm_gres:can_satisfy_spec(Spec, [])).

can_satisfy_spec_map_input_test() ->
    Spec = #{type => gpu, count => 1, name => any, memory_mb => any},
    ?assertNot(flurm_gres:can_satisfy_spec(Spec, [])).

%%====================================================================
%% Select Best Devices Tests
%%====================================================================

select_best_devices_insufficient_test() ->
    ?assertEqual({error, insufficient_gres},
                 flurm_gres:select_best_devices([], 1, #{})).

%%====================================================================
%% Format With Flags Tests
%%====================================================================

format_with_flags_empty_test() ->
    Base = [<<"gpu">>, <<":">>, <<"2">>],
    ?assertEqual(Base, flurm_gres:format_with_flags(Base, [])).

format_with_flags_exclusive_only_test() ->
    Base = [<<"gpu">>, <<":">>, <<"2">>],
    %% exclusive flag is not added to output
    ?assertEqual(Base, flurm_gres:format_with_flags(Base, [exclusive])).

format_with_flags_shared_test() ->
    Base = [<<"gpu">>, <<":">>, <<"2">>],
    Result = flurm_gres:format_with_flags(Base, [shared]),
    ?assert(is_list(Result)).
