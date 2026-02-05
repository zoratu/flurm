# FLURM Troubleshooting Runbook

Common issues and their solutions.

## Quick Diagnostics

```bash
# Check system health
curl -s http://localhost:6820/health | jq .

# Check controller status
scontrol ping

# View recent logs
tail -100 /var/log/flurm/controller.log

# Check node status
sinfo -N -l

# List all jobs with details
squeue -l
```

---

## Controller Issues

### Controller Won't Start

**Symptom:** `flurmctld` exits immediately or won't respond.

**Check:**
1. Port availability
   ```bash
   netstat -ln | grep -E '681[78]|6820'
   # If ports in use, find the process:
   lsof -i :6817
   ```

2. Erlang cookie mismatch (multi-node)
   ```bash
   # Verify cookie is same on all nodes
   cat /etc/flurm/vm.args | grep cookie
   ```

3. Configuration syntax
   ```bash
   # Test Erlang config syntax
   erl -config /etc/flurm/sys -noshell -eval "halt()."
   ```

4. Ra data directory permissions
   ```bash
   ls -la /var/lib/flurm/ra
   # Should be writable by flurm user
   ```

**Fix:**
```bash
# Clear Ra state and restart (WARNING: loses in-flight state)
rm -rf /var/lib/flurm/ra/*
systemctl restart flurmctld
```

---

### Controller Cluster Won't Form

**Symptom:** Controllers start but don't form quorum.

**Check:**
1. Network connectivity
   ```bash
   # From controller1, test connectivity to controller2
   erl -name test@controller1 -setcookie FLURM_SECRET_COOKIE \
       -eval "net_adm:ping('flurm@controller2')."
   ```

2. DNS resolution
   ```bash
   # Verify all controller hostnames resolve
   getent hosts controller1.example.com controller2.example.com
   ```

3. Firewall rules
   ```bash
   # EPMD uses port 4369, Erlang distribution uses dynamic range
   firewall-cmd --list-ports
   # Need: 4369/tcp, 9100-9200/tcp (Erlang dist range)
   ```

**Fix:**
```bash
# Set explicit Erlang port range in vm.args
echo "-kernel inet_dist_listen_min 9100" >> /etc/flurm/vm.args
echo "-kernel inet_dist_listen_max 9200" >> /etc/flurm/vm.args

# Open firewall
firewall-cmd --permanent --add-port=4369/tcp
firewall-cmd --permanent --add-port=9100-9200/tcp
firewall-cmd --reload
```

---

### Leader Election Taking Too Long

**Symptom:** Jobs stuck, slow response after failover.

**Check:**
```bash
# Check Ra cluster status
curl -s http://localhost:6820/api/v1/cluster/status | jq .

# Check for network delays
ping -c 10 controller2.example.com
```

**Fix:**
Tune Ra timeouts in sys.config:
```erlang
{ra, [
    {election_timeout, 1000},        % Default: 5000
    {heartbeat_interval, 200}        % Default: 1000
]}
```

---

## Node Issues

### Node Shows as DOWN

**Symptom:** `sinfo` shows node as DOWN.

**Check:**
1. Node daemon status
   ```bash
   systemctl status flurmd
   ```

2. Node registration
   ```bash
   # From controller
   scontrol show node <nodename>
   ```

3. Network connectivity
   ```bash
   nc -zv controller.example.com 6817
   ```

**Fix:**
```bash
# Restart node daemon
systemctl restart flurmd

# If node stuck in drain
scontrol update nodename=node01 state=resume
```

---

### Node Not Accepting Jobs

**Symptom:** Jobs pending, nodes show IDLE but don't run jobs.

**Check:**
1. Node partition membership
   ```bash
   scontrol show node <nodename> | grep Partitions
   ```

2. Resource availability
   ```bash
   scontrol show node <nodename> | grep -E 'CPUTot|RealMemory|CPUAlloc'
   ```

3. Job requirements vs node capabilities
   ```bash
   scontrol show job <jobid> | grep -E 'NumCPUs|MinMemory|Partition'
   ```

**Fix:**
```bash
# Check if partition is up
scontrol show partition

# Resume partition if down
scontrol update partition=normal state=up
```

---

## Job Issues

### Job Stuck in PENDING

**Symptom:** Job shows PD (pending) indefinitely.

**Check:**
```bash
# Get pending reason
squeue -j <jobid> -O "JobID,Reason"

# Common reasons:
# - Resources: Not enough nodes/CPUs
# - Priority: Other jobs have higher priority
# - QOSMaxJobsPerUser: QOS limit reached
# - AssocMaxJobsLimit: Account limit reached
```

**Fix by reason:**

**Resources:**
```bash
# Check available resources
sinfo -N -l

# Check job requirements
scontrol show job <jobid> | grep -E 'NumNodes|NumCPUs|MinMemory'

# Reduce requirements or wait
```

**Priority:**
```bash
# Boost priority (admin only)
scontrol update job=<jobid> priority=10000
```

**QOS/Assoc Limits:**
```bash
# Check user's limits
sacctmgr show qos format=name,maxjobspu
sacctmgr show association user=<username> format=user,maxjobs
```

---

### Job Fails Immediately

**Symptom:** Job transitions to FAILED or NODE_FAIL quickly.

**Check:**
```bash
# Get job details
scontrol show job <jobid>

# Check job output
cat slurm-<jobid>.out
cat slurm-<jobid>.err  # If separate

# Check node where it ran
sacct -j <jobid> --format=JobID,NodeList,State,ExitCode
```

**Common causes:**
- Script not executable: `chmod +x script.sh`
- Bad shebang: First line should be `#!/bin/bash`
- Missing dependencies: Check PATH and modules
- Out of memory: Increase `--mem` request

---

### Job Output Missing

**Symptom:** Job completes but no output file.

**Check:**
```bash
# Check working directory
scontrol show job <jobid> | grep WorkDir

# Check output file path
scontrol show job <jobid> | grep -E 'StdOut|StdErr'
```

**Fix:**
```bash
# Specify explicit output path
sbatch --output=/absolute/path/job-%j.out script.sh
```

---

## Authentication Issues

### MUNGE Errors

**Symptom:** "MUNGE authentication failed" or "Invalid credential"

**Check:**
1. MUNGE service status
   ```bash
   systemctl status munge
   ```

2. MUNGE key consistency
   ```bash
   # Same key on all nodes?
   md5sum /etc/munge/munge.key
   # Must match everywhere
   ```

3. Clock synchronization
   ```bash
   # MUNGE has 5-minute window by default
   date +%s
   # Compare across nodes - should be within seconds
   ```

4. MUNGE permissions
   ```bash
   ls -la /etc/munge/munge.key
   # Should be: -r-------- 1 munge munge
   ls -la /var/run/munge/
   # Socket should exist
   ```

**Fix:**
```bash
# Regenerate MUNGE key (do on one node, copy to all)
mungekey -c -f
systemctl restart munge

# Copy to other nodes
scp /etc/munge/munge.key othernode:/etc/munge/
ssh othernode "chown munge:munge /etc/munge/munge.key && systemctl restart munge"
```

---

### REST API Authentication Failed

**Symptom:** 401 Unauthorized from REST API.

**Check:**
```bash
# Test with debug
curl -v http://localhost:6820/api/v1/jobs
```

**Fix:**
```bash
# If JWT required, generate token
TOKEN=$(curl -s -X POST http://localhost:6820/api/v1/auth/token \
    -d '{"username":"admin"}' | jq -r .token)

curl -H "Authorization: Bearer $TOKEN" http://localhost:6820/api/v1/jobs
```

---

## Performance Issues

### High Controller CPU

**Symptom:** Controller using >80% CPU constantly.

**Check:**
```bash
# Count pending jobs
squeue -h -t pending | wc -l

# If >10k, scheduler is overloaded
```

**Fix:**
Tune scheduler in sys.config:
```erlang
{flurm_controller, [
    {scheduler_interval, 1000},     % Increase from 100ms
    {max_jobs_per_cycle, 100}       % Limit per scheduling cycle
]}
```

---

### Slow Job Submission

**Symptom:** `sbatch` takes >1s to return.

**Check:**
```bash
# Time submission
time sbatch test.sh

# Check controller response time
curl -w "@curl-format.txt" -s http://localhost:6820/health
```

**Fix:**
1. Check network latency to controller
2. Check controller load
3. Consider adding more controllers for load distribution

---

## Recovery Procedures

### Full Cluster Recovery

If all controllers failed:

```bash
# 1. Stop all controllers
systemctl stop flurmctld  # on all nodes

# 2. Identify node with most recent Ra state
ls -la /var/lib/flurm/ra/  # check timestamps

# 3. Start that node first
systemctl start flurmctld  # on node with newest state

# 4. Wait for it to become leader
sleep 30
curl http://localhost:6820/health

# 5. Start remaining controllers
systemctl start flurmctld  # on other nodes
```

### Job State Recovery

If jobs were lost:

```bash
# Check Ra log for job records
erl -pa _build/default/lib/*/ebin -eval "
    {ok, Log} = ra_log:open(\"/var/lib/flurm/ra\"),
    % Inspect entries
    halt()."

# Requeue affected jobs (if script available)
sbatch --requeue <script.sh>
```

---

## Log Analysis

### Key Log Patterns

```bash
# Controller errors
grep -E "ERROR|CRITICAL" /var/log/flurm/controller.log

# Node registration issues
grep "node registration" /var/log/flurm/controller.log

# Job state transitions
grep "job.*state changed" /var/log/flurm/controller.log

# Ra consensus issues
grep -E "election|leader|follower" /var/log/flurm/controller.log
```

### Enabling Debug Logging

In sys.config:
```erlang
{lager, [
    {handlers, [
        {lager_file_backend, [{file, "/var/log/flurm/debug.log"}, {level, debug}]}
    ]}
]}
```

**Warning:** Debug logging is very verbose. Disable after troubleshooting.

---

## Getting Help

1. Check [FLURM Issues](https://github.com/zoratu/flurm/issues)
2. Review [Architecture](architecture.md) for system understanding
3. Enable debug logging and capture relevant sections
4. Include: FLURM version, Erlang version, OS, and exact error messages
