-- MySQL initialization script for FLURM accounting
-- Creates tables compatible with slurmdbd schema for migration testing

-- Jobs table
CREATE TABLE IF NOT EXISTS job_table (
    job_db_inx BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    mod_time BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    deleted TINYINT DEFAULT 0 NOT NULL,
    account VARCHAR(255),
    admin_comment TEXT,
    array_job_id INT UNSIGNED DEFAULT 0 NOT NULL,
    array_max_tasks INT UNSIGNED DEFAULT 0 NOT NULL,
    array_task_id INT UNSIGNED DEFAULT 0xfffffffe NOT NULL,
    array_task_pending INT UNSIGNED DEFAULT 0 NOT NULL,
    array_task_str TEXT,
    batch_script LONGTEXT,
    cluster VARCHAR(255),
    constraints TEXT,
    container TEXT,
    cpus_alloc INT UNSIGNED DEFAULT 0 NOT NULL,
    cpus_req INT UNSIGNED DEFAULT 0 NOT NULL,
    derived_ec INT UNSIGNED DEFAULT 0 NOT NULL,
    derived_es TEXT,
    elapsed INT UNSIGNED DEFAULT 0 NOT NULL,
    eligible BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    end BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    env_hash VARCHAR(64),
    env_vars LONGTEXT,
    exit_code INT DEFAULT 0 NOT NULL,
    extra TEXT,
    failed_node TEXT,
    flags INT UNSIGNED DEFAULT 0 NOT NULL,
    gres_alloc TEXT,
    gres_req TEXT,
    gres_used TEXT,
    het_job_id INT UNSIGNED DEFAULT 0 NOT NULL,
    het_job_offset INT UNSIGNED DEFAULT 0 NOT NULL,
    id_array_job INT UNSIGNED DEFAULT 0 NOT NULL,
    id_array_task INT UNSIGNED DEFAULT 0xfffffffe NOT NULL,
    id_assoc INT UNSIGNED NOT NULL,
    id_block TEXT,
    id_group INT UNSIGNED DEFAULT 0 NOT NULL,
    id_job INT UNSIGNED NOT NULL,
    id_qos INT UNSIGNED DEFAULT 0 NOT NULL,
    id_resv INT UNSIGNED DEFAULT 0 NOT NULL,
    id_user INT UNSIGNED NOT NULL,
    id_wckey INT UNSIGNED DEFAULT 0 NOT NULL,
    job_name VARCHAR(200),
    kill_requid INT DEFAULT -1 NOT NULL,
    licensed TEXT,
    mcs_label VARCHAR(64),
    mem_req BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    nodelist TEXT,
    nodes_alloc INT UNSIGNED DEFAULT 0 NOT NULL,
    node_inx TEXT,
    `partition` VARCHAR(200),
    priority INT UNSIGNED DEFAULT 0 NOT NULL,
    script_hash VARCHAR(64),
    start BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    state INT UNSIGNED NOT NULL,
    state_reason_prev INT UNSIGNED DEFAULT 0 NOT NULL,
    std_err TEXT,
    std_in TEXT,
    std_out TEXT,
    submit BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    submit_line TEXT,
    suspended INT UNSIGNED DEFAULT 0 NOT NULL,
    system_comment TEXT,
    time_eligible BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    time_end BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    time_start BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    time_submit BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    time_suspended INT UNSIGNED DEFAULT 0 NOT NULL,
    timelimit INT UNSIGNED DEFAULT 0 NOT NULL,
    tres_alloc TEXT NOT NULL,
    tres_req TEXT NOT NULL,
    wckey VARCHAR(200) NOT NULL DEFAULT '',
    work_dir TEXT,
    PRIMARY KEY (job_db_inx),
    KEY old_tuple (id_job, id_assoc, submit)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Usage table for TRES tracking
CREATE TABLE IF NOT EXISTS usage_table (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    creation_time BIGINT UNSIGNED NOT NULL,
    mod_time BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    deleted TINYINT DEFAULT 0 NOT NULL,
    id_assoc INT UNSIGNED NOT NULL,
    time_start BIGINT UNSIGNED NOT NULL,
    cpu_seconds BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    memory_seconds BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    gpu_seconds BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    node_seconds BIGINT UNSIGNED DEFAULT 0 NOT NULL,
    job_count INT UNSIGNED DEFAULT 0 NOT NULL,
    job_time INT UNSIGNED DEFAULT 0 NOT NULL,
    PRIMARY KEY (id),
    KEY id_assoc (id_assoc),
    KEY time_start (time_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Associations table (minimal for testing)
CREATE TABLE IF NOT EXISTS assoc_table (
    id_assoc INT UNSIGNED NOT NULL AUTO_INCREMENT,
    deleted TINYINT DEFAULT 0 NOT NULL,
    acct VARCHAR(255) NOT NULL,
    user VARCHAR(255) NOT NULL DEFAULT '',
    `partition` VARCHAR(255) NOT NULL DEFAULT '',
    shares INT UNSIGNED DEFAULT 1 NOT NULL,
    PRIMARY KEY (id_assoc),
    UNIQUE KEY user_acct (user, acct, `partition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert default association
INSERT INTO assoc_table (acct, user, `partition`, shares)
VALUES ('default', '', '', 1)
ON DUPLICATE KEY UPDATE shares=shares;

-- Grant permissions (align with docker-compose defaults)
GRANT ALL PRIVILEGES ON slurm_acct_db.* TO 'slurm'@'%';
FLUSH PRIVILEGES;
