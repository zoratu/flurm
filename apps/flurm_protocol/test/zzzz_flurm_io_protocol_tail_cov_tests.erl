%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_io_protocol edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_io_protocol_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

decode_invalid_io_header_branch_test() ->
    %% Any non-10-byte header that is not "incomplete" hits invalid_io_header.
    ?assertEqual({error, invalid_io_header},
                 flurm_io_protocol:decode_io_hdr(<<1,2,3,4,5,6,7,8,9,10,11>>)).

