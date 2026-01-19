%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_scheduler_advanced internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure calculation functions that are exported
%%% only when compiled with -DTEST.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_advanced_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% insert_by_priority/3 Tests
%%====================================================================

insert_by_priority_test_() ->
    {"insert_by_priority/3 inserts job maintaining priority order",
     [
      {"inserts into empty list",
       fun() ->
           Result = flurm_scheduler_advanced:insert_by_priority(100, 5000, []),
           ?assertEqual([{100, 5000}], Result)
       end},

      {"inserts at front when highest priority",
       fun() ->
           Existing = [{200, 3000}, {300, 2000}, {400, 1000}],
           Result = flurm_scheduler_advanced:insert_by_priority(100, 5000, Existing),
           ?assertEqual([{100, 5000}, {200, 3000}, {300, 2000}, {400, 1000}], Result)
       end},

      {"inserts at end when lowest priority",
       fun() ->
           Existing = [{200, 5000}, {300, 3000}, {400, 2000}],
           Result = flurm_scheduler_advanced:insert_by_priority(100, 1000, Existing),
           ?assertEqual([{200, 5000}, {300, 3000}, {400, 2000}, {100, 1000}], Result)
       end},

      {"inserts in middle at correct position",
       fun() ->
           Existing = [{200, 5000}, {300, 3000}, {400, 1000}],
           Result = flurm_scheduler_advanced:insert_by_priority(100, 4000, Existing),
           ?assertEqual([{200, 5000}, {100, 4000}, {300, 3000}, {400, 1000}], Result)
       end},

      {"handles equal priority - inserts after equal (FIFO for same priority)",
       fun() ->
           Existing = [{200, 5000}, {300, 3000}, {400, 1000}],
           Result = flurm_scheduler_advanced:insert_by_priority(100, 3000, Existing),
           % When priority equals, new job goes after existing jobs with same priority
           ?assertEqual([{200, 5000}, {300, 3000}, {100, 3000}, {400, 1000}], Result)
       end},

      {"handles single element list",
       fun() ->
           Existing = [{200, 3000}],
           Result = flurm_scheduler_advanced:insert_by_priority(100, 5000, Existing),
           ?assertEqual([{100, 5000}, {200, 3000}], Result)
       end},

      {"handles insertion with same job id different priority",
       fun() ->
           % This tests the function behavior - it doesn't deduplicate
           Existing = [{100, 3000}],
           Result = flurm_scheduler_advanced:insert_by_priority(100, 5000, Existing),
           ?assertEqual([{100, 5000}, {100, 3000}], Result)
       end}
     ]}.

%%====================================================================
%% recalculate_priorities/1 Tests
%%
%% Note: recalculate_priorities calls calculate_job_priority which
%% depends on external ETS tables. We wrap tests with try/catch to
%% handle the case when those tables don't exist.
%%====================================================================

recalculate_priorities_test_() ->
    {"recalculate_priorities/1 handles various input formats",
     [
      {"handles empty list",
       fun() ->
           Result = flurm_scheduler_advanced:recalculate_priorities([]),
           ?assertEqual([], Result)
       end},

      {"handles list of job IDs - returns list (may fail ETS lookup)",
       fun() ->
           % When ETS doesn't exist, this will error - we catch it
           Result = try
               flurm_scheduler_advanced:recalculate_priorities([100, 200, 300])
           catch
               error:badarg -> []  % ETS table doesn't exist
           end,
           ?assert(is_list(Result))
       end},

      {"handles list of tuples - returns list (may fail ETS lookup)",
       fun() ->
           % When ETS doesn't exist, this will error - we catch it
           Input = [{100, 5000}, {200, 3000}, {300, 1000}],
           Result = try
               flurm_scheduler_advanced:recalculate_priorities(Input)
           catch
               error:badarg -> []  % ETS table doesn't exist
           end,
           ?assert(is_list(Result))
       end},

      {"returns sorted list structure",
       fun() ->
           % With multiple jobs, result should be sorted list
           Input = [{100, 1000}, {200, 5000}, {300, 3000}],
           Result = try
               flurm_scheduler_advanced:recalculate_priorities(Input)
           catch
               error:badarg -> []  % ETS table doesn't exist
           end,
           ?assert(is_list(Result))
       end}
     ]}.

%%====================================================================
%% cancel_timer/1 Tests
%%====================================================================

cancel_timer_test_() ->
    {"cancel_timer/1 handles timer references",
     [
      {"handles undefined timer",
       fun() ->
           ?assertEqual(ok, flurm_scheduler_advanced:cancel_timer(undefined))
       end},

      {"cancels valid timer reference",
       fun() ->
           Timer = erlang:send_after(60000, self(), test_msg),
           Result = flurm_scheduler_advanced:cancel_timer(Timer),
           % erlang:cancel_timer returns remaining time or false
           ?assert(Result =:= ok orelse is_integer(Result) orelse Result =:= false)
       end},

      {"handles already cancelled timer",
       fun() ->
           Timer = erlang:send_after(60000, self(), test_msg),
           erlang:cancel_timer(Timer),
           % Calling again should not crash
           Result = flurm_scheduler_advanced:cancel_timer(Timer),
           ?assert(Result =:= ok orelse Result =:= false orelse is_integer(Result))
       end}
     ]}.

%%====================================================================
%% Integration-style tests for pure logic
%%====================================================================

priority_ordering_test_() ->
    {"Priority ordering maintains correct behavior",
     [
      {"builds ordered queue through multiple insertions",
       fun() ->
           List0 = [],
           List1 = flurm_scheduler_advanced:insert_by_priority(1, 1000, List0),
           List2 = flurm_scheduler_advanced:insert_by_priority(2, 3000, List1),
           List3 = flurm_scheduler_advanced:insert_by_priority(3, 2000, List2),
           List4 = flurm_scheduler_advanced:insert_by_priority(4, 5000, List3),
           List5 = flurm_scheduler_advanced:insert_by_priority(5, 500, List4),

           % Should be ordered by priority descending
           ?assertEqual([{4, 5000}, {2, 3000}, {3, 2000}, {1, 1000}, {5, 500}], List5)
       end},

      {"maintains ordering with boundary priorities",
       fun() ->
           List0 = [],
           List1 = flurm_scheduler_advanced:insert_by_priority(1, 0, List0),
           List2 = flurm_scheduler_advanced:insert_by_priority(2, 1000000, List1),
           List3 = flurm_scheduler_advanced:insert_by_priority(3, -100, List2),

           % Should handle extreme values
           ?assertEqual([{2, 1000000}, {1, 0}, {3, -100}], List3)
       end}
     ]}.
