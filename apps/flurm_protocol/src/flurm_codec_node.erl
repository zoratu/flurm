%%%-------------------------------------------------------------------
%%% @doc FLURM Codec - Node Message Encoding/Decoding
%%%
%%% This module handles encoding and decoding of node-related SLURM
%%% protocol messages:
%%%   - REQUEST_NODE_REGISTRATION_STATUS (1001) / MESSAGE_NODE_REGISTRATION_STATUS (1002)
%%%   - REQUEST_NODE_INFO (2007) / RESPONSE_NODE_INFO (2008)
%%%   - REQUEST_PARTITION_INFO (2009) / RESPONSE_PARTITION_INFO (2010)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_node).

-export([
    decode_body/2,
    encode_body/2
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Decode Functions
%%%===================================================================

%% REQUEST_NODE_REGISTRATION_STATUS (1001)
decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary) ->
    decode_node_registration_request(Binary);

%% Unsupported message type
decode_body(_MsgType, _Binary) ->
    unsupported.

%%%===================================================================
%%% Encode Functions
%%%===================================================================

%% REQUEST_NODE_REGISTRATION_STATUS (1001)
encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req) ->
    encode_node_registration_request(Req);

%% REQUEST_NODE_INFO (2007)
encode_body(?REQUEST_NODE_INFO, #node_info_request{} = Req) ->
    encode_node_info_request(Req);
encode_body(?REQUEST_NODE_INFO, _) ->
    {ok, <<0:32/big, 0:32>>};

%% RESPONSE_NODE_INFO (2008)
encode_body(?RESPONSE_NODE_INFO, Resp) ->
    encode_node_info_response(Resp);

%% REQUEST_PARTITION_INFO (2009)
encode_body(?REQUEST_PARTITION_INFO, #partition_info_request{} = Req) ->
    encode_partition_info_request(Req);
encode_body(?REQUEST_PARTITION_INFO, _) ->
    {ok, <<0:32/big, 0:32>>};

%% RESPONSE_PARTITION_INFO (2010)
encode_body(?RESPONSE_PARTITION_INFO, Resp) ->
    encode_partition_info_response(Resp);

%% Unsupported message type
encode_body(_MsgType, _Body) ->
    unsupported.

%%%===================================================================
%%% Internal: Decode Helpers
%%%===================================================================

%% Decode REQUEST_NODE_REGISTRATION_STATUS (1001)
decode_node_registration_request(Binary) ->
    case Binary of
        <<StatusOnly:32/big, _Rest/binary>> ->
            {ok, #node_registration_request{status_only = StatusOnly =/= 0}};
        <<>> ->
            {ok, #node_registration_request{}};
        _ ->
            {ok, #node_registration_request{}}
    end.

%%%===================================================================
%%% Internal: Encode Helpers
%%%===================================================================

%% Encode REQUEST_NODE_REGISTRATION_STATUS
encode_node_registration_request(#node_registration_request{status_only = StatusOnly}) ->
    StatusOnlyInt = case StatusOnly of true -> 1; false -> 0; _ -> 0 end,
    {ok, <<StatusOnlyInt:32/big>>};
encode_node_registration_request(_) ->
    {ok, <<0:32/big>>}.

%% Encode REQUEST_NODE_INFO
encode_node_info_request(#node_info_request{show_flags = ShowFlags, node_name = NodeName}) ->
    NodeNameBin = flurm_protocol_pack:pack_string(ensure_binary(NodeName)),
    {ok, <<ShowFlags:32/big, NodeNameBin/binary>>}.

%% Encode RESPONSE_NODE_INFO (2008)
encode_node_info_response(#node_info_response{last_update = LastUpdate, node_count = NodeCount, nodes = Nodes}) ->
    NodesBin = [encode_single_node_info(N) || N <- Nodes],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<NodeCount:32/big>>,
        NodesBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_node_info_response(_) ->
    {ok, <<0:64, 0:32>>}.

%% Encode a single node_info record
%% SLURM 22.05 node_info_t pack format from _pack_node_info_members
encode_single_node_info(#node_info{} = N) ->
    [
        %% Fields 1-2: arch, bcast_address
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.arch)),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.bcast_address)),
        %% Fields 3-6: boards, boot_time, cluster_name, cores
        flurm_protocol_pack:pack_uint16(N#node_info.boards),
        flurm_protocol_pack:pack_time(N#node_info.boot_time),
        flurm_protocol_pack:pack_string(<<>>),  %% cluster_name (empty for single cluster)
        flurm_protocol_pack:pack_uint16(N#node_info.cores),
        %% Fields 7-8: core_spec_cnt, cpu_bind
        flurm_protocol_pack:pack_uint16(N#node_info.core_spec_cnt),
        flurm_protocol_pack:pack_uint32(0),  %% cpu_bind
        %% Fields 9-10: cpu_load, free_mem
        flurm_protocol_pack:pack_uint32(N#node_info.cpu_load),
        flurm_protocol_pack:pack_uint64(N#node_info.free_mem),
        %% Fields 11-12: cpus, last_busy
        flurm_protocol_pack:pack_uint16(N#node_info.cpus),
        flurm_protocol_pack:pack_time(N#node_info.last_busy),
        %% Fields 13-14: cpu_spec_list, energy
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.cpu_spec_list)),
        <<0:32/big>>,  %% acct_gather_energy (NULL)
        %% Fields 15-17: ext_sensors, power, gres
        <<0:32/big>>,  %% ext_sensors (NULL)
        <<0:32/big>>,  %% power (NULL)
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.gres)),
        %% Fields 18-20: gres_drain, gres_used, mcs_label
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.gres_drain)),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.gres_used)),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.mcs_label)),
        %% Fields 21-22: mem_spec_limit, name
        flurm_protocol_pack:pack_uint64(N#node_info.mem_spec_limit),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.name)),
        %% Fields 23-25: next_state, node_addr, node_hostname
        flurm_protocol_pack:pack_uint32(0),  %% next_state
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.node_addr)),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.node_hostname)),
        %% Fields 26-27: node_state, os
        flurm_protocol_pack:pack_uint32(N#node_info.node_state),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.os)),
        %% Fields 28-31: owner, partitions, port, real_memory
        flurm_protocol_pack:pack_uint32(N#node_info.owner),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.partitions)),
        flurm_protocol_pack:pack_uint16(N#node_info.port),
        flurm_protocol_pack:pack_uint64(N#node_info.real_memory),
        %% Fields 32-34: comment, reason, reason_time
        flurm_protocol_pack:pack_string(<<>>),  %% comment
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.reason)),
        flurm_protocol_pack:pack_time(N#node_info.reason_time),
        %% Fields 35-36: reason_uid, resume_after
        flurm_protocol_pack:pack_uint32(N#node_info.reason_uid),
        flurm_protocol_pack:pack_time(0),  %% resume_after
        %% Fields 37: resv_name
        flurm_protocol_pack:pack_string(<<>>),  %% resv_name
        %% Fields 38-42: select_nodeinfo, slurmd_start_time, sockets, threads, tmp_disk
        <<109:32/big>>,  %% select_nodeinfo plugin_id (cons_tres)
        flurm_protocol_pack:pack_time(N#node_info.slurmd_start_time),
        flurm_protocol_pack:pack_uint16(N#node_info.sockets),
        flurm_protocol_pack:pack_uint16(N#node_info.threads),
        flurm_protocol_pack:pack_uint32(N#node_info.tmp_disk),
        %% Fields 43-45: tres_fmt_str, version, weight
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.tres_fmt_str)),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.version)),
        flurm_protocol_pack:pack_uint32(N#node_info.weight),
        %% Fields 46-47: features, features_act
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.features)),
        flurm_protocol_pack:pack_string(ensure_binary(N#node_info.features_act))
    ];
encode_single_node_info(_) ->
    [].

%% Encode REQUEST_PARTITION_INFO
encode_partition_info_request(#partition_info_request{show_flags = ShowFlags, partition_name = PartName}) ->
    PartNameBin = flurm_protocol_pack:pack_string(ensure_binary(PartName)),
    {ok, <<ShowFlags:32/big, PartNameBin/binary>>}.

%% Encode RESPONSE_PARTITION_INFO (2010)
encode_partition_info_response(#partition_info_response{last_update = LastUpdate, partition_count = PartCount, partitions = Partitions}) ->
    PartsBin = [encode_single_partition_info(P) || P <- Partitions],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<PartCount:32/big>>,
        PartsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_partition_info_response(_) ->
    {ok, <<0:64, 0:32>>}.

%% Encode a single partition_info record
%% SLURM 22.05 partition_info_t pack format
encode_single_partition_info(#partition_info{} = P) ->
    [
        %% Field 1: name
        flurm_protocol_pack:pack_string(P#partition_info.name),
        %% Fields 2-9: cpu_bind, grace_time, max_time, default_time, max_nodes, min_nodes, total_nodes, total_cpus
        flurm_protocol_pack:pack_uint32(0),  % cpu_bind
        flurm_protocol_pack:pack_uint32(0),  % grace_time
        flurm_protocol_pack:pack_uint32(P#partition_info.max_time),
        flurm_protocol_pack:pack_uint32(P#partition_info.default_time),
        flurm_protocol_pack:pack_uint32(P#partition_info.max_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.min_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.total_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.total_cpus),
        %% Fields 10-12: def_mem_per_cpu (uint64), max_cpus_per_node, max_mem_per_cpu (uint64)
        flurm_protocol_pack:pack_uint64(0),  % def_mem_per_cpu
        flurm_protocol_pack:pack_uint32(16#FFFFFFFF),  % max_cpus_per_node (INFINITE)
        flurm_protocol_pack:pack_uint64(0),  % max_mem_per_cpu
        %% Fields 13-22: flags, max_share, over_time_limit, preempt_mode, priority_job_factor, priority_tier, state_up, cr_type, resume_timeout, suspend_timeout (all uint16)
        flurm_protocol_pack:pack_uint16(0),  % flags
        flurm_protocol_pack:pack_uint16(1),  % max_share
        flurm_protocol_pack:pack_uint16(0),  % over_time_limit
        flurm_protocol_pack:pack_uint16(0),  % preempt_mode
        flurm_protocol_pack:pack_uint16(P#partition_info.priority_job_factor),
        flurm_protocol_pack:pack_uint16(P#partition_info.priority_tier),
        flurm_protocol_pack:pack_uint16(P#partition_info.state_up),
        flurm_protocol_pack:pack_uint16(0),  % cr_type
        flurm_protocol_pack:pack_uint16(0),  % resume_timeout
        flurm_protocol_pack:pack_uint16(0),  % suspend_timeout
        %% Field 23: suspend_time (uint32)
        flurm_protocol_pack:pack_uint32(0),  % suspend_time
        %% Fields 24-33: allow_accounts, allow_groups, allow_alloc_nodes, allow_qos, qos_char, alternate, deny_accounts, deny_qos, nodes, nodesets
        flurm_protocol_pack:pack_string(<<>>),  % allow_accounts
        flurm_protocol_pack:pack_string(<<>>),  % allow_groups
        flurm_protocol_pack:pack_string(<<>>),  % allow_alloc_nodes
        flurm_protocol_pack:pack_string(<<>>),  % allow_qos
        flurm_protocol_pack:pack_string(<<>>),  % qos_char
        flurm_protocol_pack:pack_string(<<>>),  % alternate
        flurm_protocol_pack:pack_string(<<>>),  % deny_accounts
        flurm_protocol_pack:pack_string(<<>>),  % deny_qos
        flurm_protocol_pack:pack_string(P#partition_info.nodes),
        flurm_protocol_pack:pack_string(<<>>),  % nodesets
        %% Field 34: node_inx (bitstring) - array of int32 range pairs
        encode_node_inx(P#partition_info.node_inx),
        %% Fields 35-36: billing_weights_str, tres_fmt_str
        flurm_protocol_pack:pack_string(<<>>),  % billing_weights_str
        flurm_protocol_pack:pack_string(<<>>),  % tres_fmt_str
        %% Field 37: job_defaults_list - pack count 0 for empty list
        <<0:32/big>>
    ];
encode_single_partition_info(_) ->
    [].

%% Encode node index array
encode_node_inx([]) ->
    %% Empty: pack NO_VAL as count to indicate no ranges
    <<?SLURM_NO_VAL:32/big>>;
encode_node_inx(Ranges) when is_list(Ranges) ->
    %% Pack count + pairs of int32 (start, end)
    Count = length(Ranges),
    Elements = << <<V:32/signed-big>> || V <- Ranges >>,
    <<Count:32/big, Elements/binary>>.

%%%===================================================================
%%% Internal: Utility Functions
%%%===================================================================

%% Ensure value is binary
ensure_binary(undefined) -> <<>>;
ensure_binary(null) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(_) -> <<>>.
