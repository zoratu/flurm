%%%-------------------------------------------------------------------
%%% @doc FLURM Database Header Definitions
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FLURM_DB_HRL).
-define(FLURM_DB_HRL, true).

%% Type definitions
-type table_name() :: jobs | nodes | partitions | atom().
-type key() :: term().
-type value() :: term().

%% Database configuration
-define(DEFAULT_DATA_DIR, "/var/lib/flurm/db").
-define(DEFAULT_CLUSTER_NAME, flurm_cluster).

%% Ra machine state
-record(flurm_db_state, {
    tables = #{} :: #{table_name() => #{key() => value()}}
}).

%% Ra machine commands
-type db_command() ::
    {put, table_name(), key(), value()} |
    {delete, table_name(), key()} |
    {clear, table_name()}.

%% Ra machine queries
-type db_query() ::
    {get, table_name(), key()} |
    {list, table_name()} |
    {list_keys, table_name()}.

-endif.
