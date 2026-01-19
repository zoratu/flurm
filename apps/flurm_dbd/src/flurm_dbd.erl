%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - High-Level API
%%%
%%% Provides a clean API for interacting with the accounting database.
%%% This module is the main interface for:
%%% - Job accounting (sacct-style queries)
%%% - User/account association management
%%% - TRES usage tracking and reporting
%%% - Archive and purge operations
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd).

%% Job accounting API (sacct-style)
-export([
    %% Record job lifecycle events
    record_job_submit/1,
    record_job_start/2,
    record_job_end/3,
    record_job_cancelled/2,

    %% Query jobs
    get_job/1,
    get_jobs/0,
    get_jobs/1,
    get_jobs_by_user/1,
    get_jobs_by_account/1,
    get_jobs_in_range/2,

    %% sacct-style output
    sacct/0,
    sacct/1,
    format_sacct/1
]).

%% TRES tracking API
-export([
    get_user_usage/1,
    get_account_usage/1,
    get_cluster_usage/0,
    reset_usage/0
]).

%% Association management API
-export([
    add_association/3,
    add_association/4,
    get_associations/1,
    get_association/1,
    remove_association/1,
    update_association/2,
    get_user_associations/1,
    get_account_associations/1
]).

%% Archive/purge API
-export([
    archive_old_jobs/1,
    purge_old_jobs/1,
    get_archived_jobs/0,
    get_archived_jobs/1
]).

%% Statistics
-export([
    get_stats/0
]).

%% Test exports for internal function testing
-ifdef(TEST).
-export([]).  % No internal functions - this is a pure delegation module
-endif.

%%====================================================================
%% Job Accounting API
%%====================================================================

%% @doc Record when a job is submitted
%% JobData should be a map containing at minimum:
%%   job_id, user_id, group_id, partition, num_nodes, num_cpus
-spec record_job_submit(map()) -> ok.
record_job_submit(JobData) ->
    flurm_dbd_server:record_job_submit(JobData).

%% @doc Record when a job starts running
-spec record_job_start(pos_integer(), [binary()]) -> ok.
record_job_start(JobId, AllocatedNodes) ->
    flurm_dbd_server:record_job_start(JobId, AllocatedNodes).

%% @doc Record when a job ends
-spec record_job_end(pos_integer(), integer(), atom()) -> ok.
record_job_end(JobId, ExitCode, FinalState) ->
    flurm_dbd_server:record_job_end(JobId, ExitCode, FinalState).

%% @doc Record when a job is cancelled
-spec record_job_cancelled(pos_integer(), atom()) -> ok.
record_job_cancelled(JobId, Reason) ->
    flurm_dbd_server:record_job_cancelled(JobId, Reason).

%% @doc Get a specific job record by ID
-spec get_job(pos_integer()) -> {ok, map()} | {error, not_found}.
get_job(JobId) ->
    flurm_dbd_server:get_job_record(JobId).

%% @doc Get all job records
-spec get_jobs() -> [map()].
get_jobs() ->
    flurm_dbd_server:list_job_records().

%% @doc Get job records matching filters
%% Supported filters:
%%   user - filter by username
%%   account - filter by account
%%   partition - filter by partition
%%   state - filter by job state
%%   start_time_after - jobs started after this timestamp
%%   start_time_before - jobs started before this timestamp
%%   end_time_after - jobs ended after this timestamp
%%   end_time_before - jobs ended before this timestamp
-spec get_jobs(map()) -> [map()].
get_jobs(Filters) ->
    flurm_dbd_server:list_job_records(Filters).

%% @doc Get all jobs for a specific user
-spec get_jobs_by_user(binary()) -> [map()].
get_jobs_by_user(Username) ->
    flurm_dbd_server:get_jobs_by_user(Username).

%% @doc Get all jobs for a specific account
-spec get_jobs_by_account(binary()) -> [map()].
get_jobs_by_account(Account) ->
    flurm_dbd_server:get_jobs_by_account(Account).

%% @doc Get jobs that ran within a time range
-spec get_jobs_in_range(non_neg_integer(), non_neg_integer()) -> [map()].
get_jobs_in_range(StartTime, EndTime) ->
    flurm_dbd_server:get_jobs_in_range(StartTime, EndTime).

%% @doc Display all jobs in sacct format
-spec sacct() -> iolist().
sacct() ->
    Jobs = get_jobs(),
    format_sacct(Jobs).

%% @doc Display jobs matching filters in sacct format
-spec sacct(map()) -> iolist().
sacct(Filters) ->
    Jobs = get_jobs(Filters),
    format_sacct(Jobs).

%% @doc Format a list of job records as sacct output
-spec format_sacct([map()]) -> iolist().
format_sacct(Jobs) ->
    flurm_dbd_server:format_sacct_output(Jobs).

%%====================================================================
%% TRES Tracking API
%%====================================================================

%% @doc Get TRES usage for a specific user
%% Returns a map with:
%%   cpu_seconds - Total CPU seconds used
%%   mem_seconds - Total memory-seconds (MB * seconds)
%%   gpu_seconds - Total GPU seconds used
%%   node_seconds - Total node-seconds used
%%   job_count - Number of jobs run
%%   elapsed_total - Total wall-clock time of all jobs
-spec get_user_usage(binary()) -> map().
get_user_usage(Username) ->
    flurm_dbd_server:get_user_usage(Username).

%% @doc Get TRES usage for a specific account
-spec get_account_usage(binary()) -> map().
get_account_usage(Account) ->
    flurm_dbd_server:get_account_usage(Account).

%% @doc Get cluster-wide TRES usage
-spec get_cluster_usage() -> map().
get_cluster_usage() ->
    flurm_dbd_server:get_cluster_usage().

%% @doc Reset all usage counters (for new billing period)
-spec reset_usage() -> ok.
reset_usage() ->
    flurm_dbd_server:reset_usage().

%%====================================================================
%% Association Management API
%%====================================================================

%% @doc Add a user-account association
%% Creates a link between a user and an account on a cluster.
%% This is required before a user can run jobs under an account.
-spec add_association(binary(), binary(), binary()) -> {ok, pos_integer()} | {error, term()}.
add_association(Cluster, Account, User) ->
    flurm_dbd_server:add_association(Cluster, Account, User).

%% @doc Add a user-account association with options
%% Options can include:
%%   partition - restrict to specific partition
%%   shares - fairshare shares (default: 1)
%%   max_jobs - max concurrent jobs (0 = unlimited)
%%   max_submit - max submitted jobs (0 = unlimited)
%%   max_wall - max wall time per job in minutes (0 = unlimited)
%%   grp_tres - group TRES limits (map)
%%   max_tres_per_job - per-job TRES limits (map)
%%   qos - list of allowed QOS
%%   default_qos - default QOS
-spec add_association(binary(), binary(), binary(), map()) -> {ok, pos_integer()} | {error, term()}.
add_association(Cluster, Account, User, Opts) ->
    flurm_dbd_server:add_association(Cluster, Account, User, Opts).

%% @doc Get all associations for a cluster
-spec get_associations(binary()) -> [map()].
get_associations(Cluster) ->
    flurm_dbd_server:get_associations(Cluster).

%% @doc Get a specific association by ID
-spec get_association(pos_integer()) -> {ok, map()} | {error, not_found}.
get_association(AssocId) ->
    flurm_dbd_server:get_association(AssocId).

%% @doc Remove an association
-spec remove_association(pos_integer()) -> ok | {error, not_found}.
remove_association(AssocId) ->
    flurm_dbd_server:remove_association(AssocId).

%% @doc Update an association's settings
-spec update_association(pos_integer(), map()) -> ok | {error, not_found}.
update_association(AssocId, Updates) ->
    flurm_dbd_server:update_association(AssocId, Updates).

%% @doc Get all associations for a user
-spec get_user_associations(binary()) -> [map()].
get_user_associations(Username) ->
    flurm_dbd_server:get_user_associations(Username).

%% @doc Get all associations for an account
-spec get_account_associations(binary()) -> [map()].
get_account_associations(Account) ->
    flurm_dbd_server:get_account_associations(Account).

%%====================================================================
%% Archive/Purge API
%%====================================================================

%% @doc Archive jobs older than N days
%% Moves completed job records from the main table to the archive table.
%% Running and pending jobs are not archived.
-spec archive_old_jobs(pos_integer()) -> {ok, non_neg_integer()}.
archive_old_jobs(Days) ->
    flurm_dbd_server:archive_old_jobs(Days).

%% @doc Purge archived jobs older than N days
%% Permanently deletes archived job records older than N days.
%% This operation cannot be undone.
-spec purge_old_jobs(pos_integer()) -> {ok, non_neg_integer()}.
purge_old_jobs(Days) ->
    flurm_dbd_server:purge_old_jobs(Days).

%% @doc Get all archived job records
-spec get_archived_jobs() -> [map()].
get_archived_jobs() ->
    flurm_dbd_server:get_archived_jobs().

%% @doc Get archived job records with filters
-spec get_archived_jobs(map()) -> [map()].
get_archived_jobs(Filters) ->
    flurm_dbd_server:get_archived_jobs(Filters).

%%====================================================================
%% Statistics
%%====================================================================

%% @doc Get daemon statistics
%% Returns a map with:
%%   jobs_submitted - total jobs submitted
%%   jobs_started - total jobs started
%%   jobs_completed - total jobs completed successfully
%%   jobs_failed - total jobs that failed
%%   jobs_cancelled - total jobs cancelled
%%   total_job_records - current job records in main table
%%   total_archived - current job records in archive table
%%   total_associations - number of associations
%%   uptime_seconds - daemon uptime
-spec get_stats() -> map().
get_stats() ->
    flurm_dbd_server:get_stats().
