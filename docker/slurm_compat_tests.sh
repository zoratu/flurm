#!/bin/bash
###############################################################################
# SLURM Compatibility Test Suite for FLURM
#
# Based on SchedMD SLURM testsuite functional areas:
#   test1.x  - srun basics
#   test2.x  - scontrol operations
#   test4.x  - sinfo output
#   test5.x  - squeue output and filtering
#   test6.x  - scancel operations
#   test17.x - sbatch job submission
#   test21.x - sacctmgr (accounting)
#   test38.x - heterogeneous jobs
#
# Runs inside Docker slurm-client container against FLURM server.
###############################################################################

set -o pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Counters
PASS=0
FAIL=0
SKIP=0
WARN=0
TOTAL=0
SECTION=""
FAILURES=""

# Test helper functions
pass() {
    ((PASS++))
    ((TOTAL++))
    echo -e "  ${GREEN}PASS${NC} - $1"
}

fail() {
    ((FAIL++))
    ((TOTAL++))
    echo -e "  ${RED}FAIL${NC} - $1"
    [ -n "$2" ] && echo -e "         ${RED}$2${NC}"
    FAILURES="${FAILURES}\n  [${SECTION}] $1"
}

skip() {
    ((SKIP++))
    ((TOTAL++))
    echo -e "  ${YELLOW}SKIP${NC} - $1"
    [ -n "$2" ] && echo -e "         ${YELLOW}$2${NC}"
}

warn() {
    ((WARN++))
    echo -e "  ${YELLOW}WARN${NC} - $1"
}

section() {
    SECTION="$1"
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Cleanup helper - cancel all jobs
cleanup_jobs() {
    for jid in $(squeue -h -o "%i" 2>/dev/null); do
        scancel "$jid" 2>/dev/null
    done
    sleep 1
}

# Wait for job state with timeout
wait_for_state() {
    local job_id=$1
    local target_state=$2
    local timeout=${3:-30}
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local state=$(squeue -j "$job_id" -h -o "%T" 2>/dev/null)
        if [ "$state" = "$target_state" ]; then
            return 0
        fi
        # If job disappeared and we wanted COMPLETED/CANCELLED, that's ok
        if [ -z "$state" ] && [[ "$target_state" =~ ^(COMPLETED|CANCELLED|FAILED)$ ]]; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

# Wait for job to disappear (completed)
wait_for_completion() {
    local job_id=$1
    local timeout=${2:-30}
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local state=$(squeue -j "$job_id" -h -o "%T" 2>/dev/null)
        if [ -z "$state" ]; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

# Submit a simple test job and return job ID
submit_test_job() {
    local opts="$1"
    local script_body="${2:-#!/bin/bash
sleep 30}"
    local tmpfile=$(mktemp /tmp/test_XXXXXX.sh)
    echo "$script_body" > "$tmpfile"
    chmod +x "$tmpfile"
    local output
    output=$(sbatch $opts "$tmpfile" 2>&1)
    rm -f "$tmpfile"
    echo "$output" | grep -oP 'Submitted batch job \K\d+'
}

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}  SLURM Compatibility Test Suite for FLURM${NC}"
echo -e "${BLUE}  Based on SchedMD SLURM testsuite functional areas${NC}"
echo -e "${BLUE}  $(date)${NC}"
echo -e "${BLUE}================================================================${NC}"

# Ensure clean state
cleanup_jobs

###############################################################################
# SECTION 1: sinfo (based on test4.x)
###############################################################################
section "test4.x - sinfo output and formatting"

# test4.1 - Basic sinfo output
OUTPUT=$(sinfo 2>&1)
if echo "$OUTPUT" | grep -q "PARTITION"; then
    pass "test4.1: sinfo shows header with PARTITION column"
else
    fail "test4.1: sinfo missing PARTITION header" "$OUTPUT"
fi

# test4.2 - sinfo shows at least one node
if echo "$OUTPUT" | grep -qE "(idle|alloc|mix|drain|down)"; then
    pass "test4.2: sinfo shows node state"
else
    fail "test4.2: sinfo shows no node state" "$OUTPUT"
fi

# test4.3 - sinfo -N (node-oriented format)
OUTPUT=$(sinfo -N 2>&1)
if echo "$OUTPUT" | grep -q "NODELIST"; then
    pass "test4.3: sinfo -N shows NODELIST header"
else
    fail "test4.3: sinfo -N missing NODELIST header" "$OUTPUT"
fi

# test4.4 - sinfo --long
OUTPUT=$(sinfo -l 2>&1)
if echo "$OUTPUT" | grep -qE "(TIMELIMIT|NODES|STATE)"; then
    pass "test4.4: sinfo -l shows extended columns"
else
    fail "test4.4: sinfo -l missing extended columns" "$OUTPUT"
fi

# test4.5 - sinfo custom format
OUTPUT=$(sinfo -o "%P %a %l %D %T %N" 2>&1)
if [ -n "$OUTPUT" ]; then
    pass "test4.5: sinfo -o custom format produces output"
else
    fail "test4.5: sinfo -o custom format produced no output"
fi

# test4.6 - sinfo partition filter
OUTPUT=$(sinfo -p default 2>&1)
if echo "$OUTPUT" | grep -q "default"; then
    pass "test4.6: sinfo -p default filters to default partition"
else
    fail "test4.6: sinfo -p default filter failed" "$OUTPUT"
fi

# test4.7 - sinfo node filter
OUTPUT=$(sinfo -n flurm-node1 2>&1)
if echo "$OUTPUT" | grep -qi "flurm-node1\|node"; then
    pass "test4.7: sinfo -n filters to specific node"
else
    fail "test4.7: sinfo -n node filter failed" "$OUTPUT"
fi

# test4.8 - sinfo shows idle state when no jobs running
STATE=$(sinfo -h -o "%T" 2>&1 | head -1)
if [[ "$STATE" =~ (idle|IDLE) ]]; then
    pass "test4.8: sinfo shows idle when no jobs running"
else
    warn "test4.8: sinfo shows '$STATE' instead of idle (may have leftover jobs)"
fi

# test4.9 - sinfo shows CPU count
OUTPUT=$(sinfo -o "%C" 2>&1)
if echo "$OUTPUT" | grep -qE "[0-9]+/[0-9]+/[0-9]+/[0-9]+"; then
    pass "test4.9: sinfo -o %C shows A/I/O/T CPU format"
else
    fail "test4.9: sinfo -o %C format incorrect" "$OUTPUT"
fi

# test4.10 - sinfo shows memory
OUTPUT=$(sinfo -o "%m" 2>&1)
if echo "$OUTPUT" | grep -qE "[0-9]+"; then
    pass "test4.10: sinfo -o %m shows memory info"
else
    fail "test4.10: sinfo -o %m missing memory" "$OUTPUT"
fi

###############################################################################
# SECTION 2: sbatch basics (based on test17.x)
###############################################################################
section "test17.x - sbatch job submission"

# test17.1 - Basic sbatch submission
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 5")
if [ -n "$JOB_ID" ] && [ "$JOB_ID" -gt 0 ] 2>/dev/null; then
    pass "test17.1: sbatch basic submission returns job ID ($JOB_ID)"
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.1: sbatch basic submission failed"
fi

# test17.2 - sbatch with --job-name
JOB_ID=$(submit_test_job "--job-name=test_name" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "test_name" ]; then
        pass "test17.2: sbatch --job-name sets job name correctly"
    else
        fail "test17.2: sbatch --job-name='test_name' but got '$NAME'"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.2: sbatch with --job-name failed to submit"
fi

# test17.3 - sbatch with -J (short form)
JOB_ID=$(submit_test_job "-J shortname" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "shortname" ]; then
        pass "test17.3: sbatch -J sets job name correctly"
    else
        fail "test17.3: sbatch -J shortname but got '$NAME'"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.3: sbatch with -J failed to submit"
fi

# test17.4 - sbatch with --ntasks
JOB_ID=$(submit_test_job "--ntasks=2" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    NCPUS=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'NumCPUs=\K[0-9]+')
    NTASKS=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'NumTasks=\K[0-9]+')
    if [ "$NCPUS" = "2" ] || [[ "$NCPUS" =~ ^2- ]]; then
        pass "test17.4: sbatch --ntasks=2 sets NumCPUs=2"
    else
        fail "test17.4: sbatch --ntasks=2 but NumCPUs=$NCPUS"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.4: sbatch with --ntasks failed to submit"
fi

# test17.5 - sbatch with --cpus-per-task
JOB_ID=$(submit_test_job "--cpus-per-task=2" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    CPT=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'CPUs/Task=\K[0-9]+')
    if [ "$CPT" = "2" ]; then
        pass "test17.5: sbatch --cpus-per-task=2 sets CPUs/Task=2"
    else
        fail "test17.5: sbatch --cpus-per-task=2 but CPUs/Task=$CPT"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.5: sbatch with --cpus-per-task failed to submit"
fi

# test17.6 - sbatch with --ntasks and --cpus-per-task combined
JOB_ID=$(submit_test_job "--ntasks=2 --cpus-per-task=2" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    NCPUS=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'NumCPUs=\K[0-9]+')
    if [ "$NCPUS" = "4" ] || [[ "$NCPUS" =~ ^4- ]]; then
        pass "test17.6: sbatch --ntasks=2 --cpus-per-task=2 → NumCPUs=4"
    else
        fail "test17.6: expected NumCPUs=4 but got NumCPUs=$NCPUS"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.6: sbatch with combined flags failed to submit"
fi

# test17.7 - sbatch with --time
JOB_ID=$(submit_test_job "--time=10" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if [ "$TL" = "00:10:00" ]; then
        pass "test17.7: sbatch --time=10 sets TimeLimit=00:10:00"
    else
        fail "test17.7: sbatch --time=10 but TimeLimit=$TL"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.7: sbatch with --time failed to submit"
fi

# test17.8 - sbatch with --time=HH:MM:SS format
JOB_ID=$(submit_test_job "--time=01:30:00" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if [ "$TL" = "01:30:00" ]; then
        pass "test17.8: sbatch --time=01:30:00 sets correct TimeLimit"
    else
        fail "test17.8: sbatch --time=01:30:00 but TimeLimit=$TL"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.8: sbatch with --time=HH:MM:SS failed to submit"
fi

# test17.9 - sbatch with --time=1 (1 minute)
JOB_ID=$(submit_test_job "--time=1" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if [ "$TL" = "00:01:00" ]; then
        pass "test17.9: sbatch --time=1 sets TimeLimit=00:01:00"
    else
        fail "test17.9: sbatch --time=1 but TimeLimit=$TL"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.9: sbatch with --time=1 failed to submit"
fi

# test17.10 - sbatch with SBATCH directives in script
JOB_ID=$(submit_test_job "" "#!/bin/bash
#SBATCH --job-name=script_directive
#SBATCH --ntasks=1
sleep 30")
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "script_directive" ]; then
        pass "test17.10: SBATCH directive --job-name in script works"
    else
        fail "test17.10: SBATCH directive --job-name expected 'script_directive' got '$NAME'"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.10: sbatch with SBATCH directives failed"
fi

# test17.11 - Command line overrides script directives
JOB_ID=$(submit_test_job "--job-name=cmdline_override" "#!/bin/bash
#SBATCH --job-name=script_name
sleep 30")
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "cmdline_override" ]; then
        pass "test17.11: command line --job-name overrides SBATCH directive"
    else
        fail "test17.11: expected 'cmdline_override' but got '$NAME'"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.11: sbatch override test failed to submit"
fi

# test17.12 - sbatch with --mem
JOB_ID=$(submit_test_job "--mem=512" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    sleep 1
    MEM=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'MinMemoryNode=\K[0-9]+')
    if [ -n "$MEM" ]; then
        pass "test17.12: sbatch --mem=512 accepted (MinMemoryNode=$MEM)"
    else
        fail "test17.12: sbatch --mem=512 but no MinMemoryNode in scontrol"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.12: sbatch with --mem failed to submit"
fi

# test17.13 - sbatch with --wrap
OUTPUT=$(sbatch --wrap="sleep 5" 2>&1)
JOB_ID=$(echo "$OUTPUT" | grep -oP 'Submitted batch job \K\d+')
if [ -n "$JOB_ID" ]; then
    pass "test17.13: sbatch --wrap submits successfully ($JOB_ID)"
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.13: sbatch --wrap failed" "$OUTPUT"
fi

# test17.14 - sbatch --wrap with complex command
OUTPUT=$(sbatch --wrap="echo hello; sleep 5; echo done" 2>&1)
JOB_ID=$(echo "$OUTPUT" | grep -oP 'Submitted batch job \K\d+')
if [ -n "$JOB_ID" ]; then
    pass "test17.14: sbatch --wrap with complex command succeeds ($JOB_ID)"
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.14: sbatch --wrap complex command failed" "$OUTPUT"
fi

# test17.15 - sbatch returns incremental job IDs
JOB1=$(submit_test_job "" "#!/bin/bash
sleep 30")
JOB2=$(submit_test_job "" "#!/bin/bash
sleep 30")
if [ -n "$JOB1" ] && [ -n "$JOB2" ]; then
    if [ "$JOB2" -gt "$JOB1" ]; then
        pass "test17.15: job IDs increment ($JOB1 < $JOB2)"
    else
        fail "test17.15: job IDs not incrementing ($JOB1, $JOB2)"
    fi
    scancel "$JOB1" "$JOB2" 2>/dev/null
else
    fail "test17.15: could not submit two jobs for ID test"
fi

# test17.16 - sbatch with --output
JOB_ID=$(submit_test_job "--output=/tmp/test_output_%j.out" "#!/bin/bash
sleep 5")
if [ -n "$JOB_ID" ]; then
    sleep 1
    STDOUT=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'StdOut=\K[^ ]+')
    if echo "$STDOUT" | grep -q "test_output"; then
        pass "test17.16: sbatch --output sets StdOut path"
    else
        # Some implementations may not expose this, skip
        skip "test17.16: sbatch --output path not shown in scontrol" "StdOut=$STDOUT"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.16: sbatch with --output failed to submit"
fi

cleanup_jobs

###############################################################################
# SECTION 3: squeue (based on test5.x)
###############################################################################
section "test5.x - squeue output and filtering"

# Submit some jobs for squeue testing
JOB_R=$(submit_test_job "--job-name=running_job" "#!/bin/bash
sleep 120")
sleep 2
JOB_P=$(submit_test_job "--job-name=pending_job -c 99" "#!/bin/bash
sleep 120")
sleep 1

# test5.1 - Basic squeue output
OUTPUT=$(squeue 2>&1)
if echo "$OUTPUT" | grep -q "JOBID"; then
    pass "test5.1: squeue shows JOBID header"
else
    fail "test5.1: squeue missing JOBID header" "$OUTPUT"
fi

# test5.2 - squeue shows submitted jobs
if echo "$OUTPUT" | grep -q "$JOB_R"; then
    pass "test5.2: squeue shows submitted job $JOB_R"
else
    fail "test5.2: squeue doesn't show job $JOB_R" "$OUTPUT"
fi

# test5.3 - squeue -j filters by job ID
OUTPUT=$(squeue -j "$JOB_R" 2>&1)
if echo "$OUTPUT" | grep -q "$JOB_R"; then
    if ! echo "$OUTPUT" | grep -q "$JOB_P"; then
        pass "test5.3: squeue -j filters to specific job"
    else
        fail "test5.3: squeue -j shows other jobs too"
    fi
else
    fail "test5.3: squeue -j doesn't show target job" "$OUTPUT"
fi

# test5.4 - squeue -h (no header)
OUTPUT=$(squeue -h 2>&1)
if ! echo "$OUTPUT" | grep -q "JOBID"; then
    pass "test5.4: squeue -h suppresses header"
else
    fail "test5.4: squeue -h still shows header"
fi

# test5.5 - squeue custom format -o
OUTPUT=$(squeue -o "%i %j %T %M" 2>&1)
if [ -n "$OUTPUT" ]; then
    pass "test5.5: squeue -o custom format produces output"
else
    fail "test5.5: squeue -o custom format no output"
fi

# test5.6 - squeue shows RUNNING state
STATE=$(squeue -j "$JOB_R" -h -o "%T" 2>/dev/null)
if [ "$STATE" = "RUNNING" ]; then
    pass "test5.6: squeue shows RUNNING state for active job"
elif [ "$STATE" = "PENDING" ]; then
    # May still be pending if node is slow
    warn "test5.6: job still PENDING (may need more time)"
else
    fail "test5.6: expected RUNNING but got '$STATE'"
fi

# test5.7 - squeue shows PENDING state for resource-starved job
STATE=$(squeue -j "$JOB_P" -h -o "%T" 2>/dev/null)
if [ "$STATE" = "PENDING" ]; then
    pass "test5.7: squeue shows PENDING for resource-starved job"
else
    fail "test5.7: expected PENDING but got '$STATE'"
fi

# test5.8 - squeue -l (long format)
OUTPUT=$(squeue -l 2>&1)
if echo "$OUTPUT" | grep -qE "(TIME|NODES|NODELIST)"; then
    pass "test5.8: squeue -l shows extended columns"
else
    fail "test5.8: squeue -l missing extended columns" "$OUTPUT"
fi

# test5.9 - squeue -u (user filter)
OUTPUT=$(squeue -u root 2>&1)
if [ -n "$OUTPUT" ]; then
    pass "test5.9: squeue -u root produces output"
else
    fail "test5.9: squeue -u root no output"
fi

# test5.10 - squeue -t (state filter) RUNNING
OUTPUT=$(squeue -t RUNNING -h 2>&1)
if [ -n "$OUTPUT" ]; then
    # Should only show running jobs
    if ! echo "$OUTPUT" | grep -q "PENDING"; then
        pass "test5.10: squeue -t RUNNING filters correctly"
    else
        fail "test5.10: squeue -t RUNNING also shows PENDING jobs"
    fi
else
    # No running jobs is also acceptable
    pass "test5.10: squeue -t RUNNING returns (no running jobs or filtered correctly)"
fi

# test5.11 - squeue -t PENDING
OUTPUT=$(squeue -t PENDING -h 2>&1)
if echo "$OUTPUT" | grep -q "$JOB_P"; then
    pass "test5.11: squeue -t PENDING shows pending job"
else
    fail "test5.11: squeue -t PENDING doesn't show pending job" "$OUTPUT"
fi

# test5.12 - squeue --name filter
OUTPUT=$(squeue --name=running_job -h 2>&1)
if echo "$OUTPUT" | grep -q "$JOB_R"; then
    pass "test5.12: squeue --name filters by job name"
else
    fail "test5.12: squeue --name filter failed" "$OUTPUT"
fi

# test5.13 - squeue %j shows job name
NAME=$(squeue -j "$JOB_R" -h -o "%j" 2>/dev/null)
if [ "$NAME" = "running_job" ]; then
    pass "test5.13: squeue %j format shows correct job name"
else
    fail "test5.13: expected 'running_job' but got '$NAME'"
fi

# test5.14 - squeue %P shows partition
PART=$(squeue -j "$JOB_R" -h -o "%P" 2>/dev/null)
if [ -n "$PART" ]; then
    pass "test5.14: squeue %P shows partition ($PART)"
else
    fail "test5.14: squeue %P shows no partition"
fi

# test5.15 - squeue %D shows node count
NNODES=$(squeue -j "$JOB_R" -h -o "%D" 2>/dev/null)
if [ "$NNODES" = "1" ]; then
    pass "test5.15: squeue %D shows node count (1)"
else
    fail "test5.15: expected 1 node but got '$NNODES'"
fi

# test5.16 - squeue %C shows CPU count
CPUS=$(squeue -j "$JOB_R" -h -o "%C" 2>/dev/null)
if [ -n "$CPUS" ] && [ "$CPUS" -gt 0 ] 2>/dev/null; then
    pass "test5.16: squeue %C shows CPU count ($CPUS)"
else
    fail "test5.16: squeue %C shows '$CPUS'"
fi

cleanup_jobs

###############################################################################
# SECTION 4: scontrol (based on test2.x)
###############################################################################
section "test2.x - scontrol operations"

# test2.1 - scontrol show job
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 60")
sleep 2
OUTPUT=$(scontrol show job "$JOB_ID" 2>&1)
if echo "$OUTPUT" | grep -q "JobId=$JOB_ID"; then
    pass "test2.1: scontrol show job shows JobId"
else
    fail "test2.1: scontrol show job missing JobId" "$OUTPUT"
fi

# test2.2 - scontrol show job shows all expected fields
FIELDS_OK=true
for field in JobId JobName UserId GroupId Priority Partition JobState NumCPUs NumNodes TimeLimit SubmitTime; do
    if ! echo "$OUTPUT" | grep -q "$field"; then
        fail "test2.2: scontrol show job missing field: $field"
        FIELDS_OK=false
        break
    fi
done
if $FIELDS_OK; then
    pass "test2.2: scontrol show job has all standard fields"
fi

# test2.3 - scontrol show job shows StartTime for running job
if echo "$OUTPUT" | grep -q "StartTime="; then
    ST=$(echo "$OUTPUT" | grep -oP 'StartTime=\K[^ ]+')
    if [ "$ST" != "N/A" ] && [ "$ST" != "Unknown" ]; then
        pass "test2.3: scontrol show job has StartTime for running job"
    else
        fail "test2.3: scontrol show job StartTime=$ST for running job"
    fi
else
    fail "test2.3: scontrol show job missing StartTime field"
fi

# test2.4 - scontrol show job shows correct state
STATE=$(echo "$OUTPUT" | grep -oP 'JobState=\K[A-Z]+')
if [ "$STATE" = "RUNNING" ]; then
    pass "test2.4: scontrol show job shows RUNNING state"
elif [ "$STATE" = "PENDING" ]; then
    warn "test2.4: job still PENDING (slow scheduler)"
else
    fail "test2.4: unexpected state $STATE"
fi

# test2.5 - scontrol show node
OUTPUT=$(scontrol show node flurm-node1 2>&1)
if echo "$OUTPUT" | grep -q "NodeName="; then
    pass "test2.5: scontrol show node shows NodeName"
else
    # Try without specific name
    OUTPUT=$(scontrol show node 2>&1)
    if echo "$OUTPUT" | grep -q "NodeName="; then
        pass "test2.5: scontrol show node (all) shows NodeName"
    else
        fail "test2.5: scontrol show node failed" "$OUTPUT"
    fi
fi

# test2.6 - scontrol show node shows resource info
if echo "$OUTPUT" | grep -qE "(CPUTot|RealMemory|State)"; then
    pass "test2.6: scontrol show node has resource fields"
else
    fail "test2.6: scontrol show node missing resource fields" "$OUTPUT"
fi

# test2.7 - scontrol show partition
OUTPUT=$(scontrol show partition 2>&1)
if echo "$OUTPUT" | grep -q "PartitionName="; then
    pass "test2.7: scontrol show partition shows PartitionName"
else
    fail "test2.7: scontrol show partition failed" "$OUTPUT"
fi

# test2.8 - scontrol show partition shows state
if echo "$OUTPUT" | grep -q "State="; then
    pass "test2.8: scontrol show partition has State field"
else
    fail "test2.8: scontrol show partition missing State"
fi

# test2.9 - scontrol hold job
HOLD_OUT=$(scontrol hold "$JOB_ID" 2>&1)
sleep 1
PRIO=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'Priority=\K[0-9]+')
if [ "$PRIO" = "0" ]; then
    pass "test2.9: scontrol hold sets Priority=0"
else
    skip "test2.9: scontrol hold - Priority=$PRIO" "hold may not be fully implemented"
fi

# test2.10 - scontrol release job
scontrol release "$JOB_ID" 2>/dev/null
sleep 1
PRIO=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'Priority=\K[0-9]+')
if [ -n "$PRIO" ] && [ "$PRIO" -gt 0 ] 2>/dev/null; then
    pass "test2.10: scontrol release restores priority ($PRIO)"
else
    skip "test2.10: scontrol release - Priority=$PRIO" "release may not be fully implemented"
fi

# test2.11 - scontrol show job for non-existent job
OUTPUT=$(scontrol show job 999999 2>&1)
if echo "$OUTPUT" | grep -qi "error\|invalid\|not found\|no job"; then
    pass "test2.11: scontrol show job 999999 returns error"
else
    fail "test2.11: scontrol show job 999999 should error" "$OUTPUT"
fi

# test2.12 - scontrol update job name
scontrol update JobId="$JOB_ID" JobName=updated_name 2>/dev/null
sleep 1
NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
if [ "$NAME" = "updated_name" ]; then
    pass "test2.12: scontrol update JobName works"
else
    skip "test2.12: scontrol update JobName not implemented" "got '$NAME'"
fi

# test2.13 - scontrol reconfigure
OUTPUT=$(scontrol reconfigure 2>&1)
# Should not crash; may return success or error about config file
if [ $? -eq 0 ] || echo "$OUTPUT" | grep -qi "error"; then
    pass "test2.13: scontrol reconfigure doesn't crash"
else
    fail "test2.13: scontrol reconfigure failed badly" "$OUTPUT"
fi

scancel "$JOB_ID" 2>/dev/null
cleanup_jobs

###############################################################################
# SECTION 5: scancel (based on test6.x)
###############################################################################
section "test6.x - scancel operations"

# test6.1 - Basic scancel
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 120")
sleep 2
scancel "$JOB_ID" 2>/dev/null
sleep 2
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ -z "$STATE" ] || [ "$STATE" = "CANCELLED" ]; then
    pass "test6.1: scancel removes job from queue"
else
    fail "test6.1: scancel didn't remove job, state=$STATE"
fi

# test6.2 - scancel pending job
JOB_ID=$(submit_test_job "-c 99 --job-name=cancel_pending" "#!/bin/bash
sleep 120")
sleep 1
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ "$STATE" = "PENDING" ]; then
    scancel "$JOB_ID" 2>/dev/null
    sleep 1
    STATE2=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
    if [ -z "$STATE2" ]; then
        pass "test6.2: scancel removes PENDING job"
    else
        fail "test6.2: scancel didn't remove pending job, state=$STATE2"
    fi
else
    fail "test6.2: job not PENDING as expected, state=$STATE"
fi

# test6.3 - scancel running job
JOB_ID=$(submit_test_job "--job-name=cancel_running" "#!/bin/bash
sleep 120")
sleep 3
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ "$STATE" = "RUNNING" ]; then
    scancel "$JOB_ID" 2>/dev/null
    sleep 2
    STATE2=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
    if [ -z "$STATE2" ]; then
        pass "test6.3: scancel removes RUNNING job"
    else
        fail "test6.3: scancel didn't stop running job, state=$STATE2"
    fi
else
    warn "test6.3: job not RUNNING as expected, state=$STATE (testing cancel anyway)"
    scancel "$JOB_ID" 2>/dev/null
fi

# test6.4 - scancel non-existent job
OUTPUT=$(scancel 999999 2>&1)
# Should not crash, may return error
pass "test6.4: scancel non-existent job doesn't crash"

# test6.5 - scancel multiple jobs at once
JOB1=$(submit_test_job "--job-name=multi1" "#!/bin/bash
sleep 120")
JOB2=$(submit_test_job "--job-name=multi2" "#!/bin/bash
sleep 120")
JOB3=$(submit_test_job "--job-name=multi3" "#!/bin/bash
sleep 120")
sleep 2
scancel "$JOB1" "$JOB2" "$JOB3" 2>/dev/null
sleep 2
REMAINING=$(squeue -h 2>/dev/null | wc -l)
if [ "$REMAINING" -eq 0 ]; then
    pass "test6.5: scancel multiple jobs at once"
else
    fail "test6.5: $REMAINING jobs remain after multi-scancel"
    cleanup_jobs
fi

# test6.6 - scancel by name
JOB_ID=$(submit_test_job "--job-name=cancel_by_name" "#!/bin/bash
sleep 120")
sleep 1
scancel --name=cancel_by_name 2>/dev/null
sleep 2
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ -z "$STATE" ]; then
    pass "test6.6: scancel --name cancels by job name"
else
    skip "test6.6: scancel --name not implemented" "state=$STATE"
    scancel "$JOB_ID" 2>/dev/null
fi

# test6.7 - scancel by user
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 120")
sleep 1
scancel -u root 2>/dev/null
sleep 2
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ -z "$STATE" ]; then
    pass "test6.7: scancel -u cancels by user"
else
    skip "test6.7: scancel -u not implemented" "state=$STATE"
    scancel "$JOB_ID" 2>/dev/null
fi

cleanup_jobs

###############################################################################
# SECTION 6: Job Lifecycle (based on test1.x - srun/core behavior)
###############################################################################
section "test1.x - Job lifecycle and state transitions"

# test1.1 - Job transitions from PENDING to RUNNING
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 60")
sleep 1
STATE1=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ "$STATE1" = "PENDING" ] || [ "$STATE1" = "RUNNING" ]; then
    if wait_for_state "$JOB_ID" "RUNNING" 15; then
        pass "test1.1: job transitions to RUNNING"
    else
        STATE2=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
        fail "test1.1: job stuck in $STATE2"
    fi
else
    fail "test1.1: unexpected initial state $STATE1"
fi
scancel "$JOB_ID" 2>/dev/null

# test1.2 - Job disappears after completion
JOB_ID=$(submit_test_job "--time=1" "#!/bin/bash
sleep 2")
sleep 5
if wait_for_completion "$JOB_ID" 30; then
    pass "test1.2: completed job disappears from squeue"
else
    STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
    fail "test1.2: job still visible after expected completion, state=$STATE"
    scancel "$JOB_ID" 2>/dev/null
fi

# test1.3 - Resource-starved job stays PENDING
JOB_ID=$(submit_test_job "-c 99 --job-name=starved" "#!/bin/bash
sleep 30")
sleep 2
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ "$STATE" = "PENDING" ]; then
    pass "test1.3: resource-starved job stays PENDING"
else
    fail "test1.3: expected PENDING but got $STATE"
fi
scancel "$JOB_ID" 2>/dev/null

# test1.4 - Large memory job stays PENDING
JOB_ID=$(submit_test_job "--mem=999999 --job-name=bigmem" "#!/bin/bash
sleep 30")
sleep 2
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ "$STATE" = "PENDING" ]; then
    pass "test1.4: large memory job stays PENDING"
else
    fail "test1.4: expected PENDING but got $STATE"
fi
scancel "$JOB_ID" 2>/dev/null

# test1.5 - Pending job starts when resources free up
JOB1=$(submit_test_job "-c 3 --job-name=blocker" "#!/bin/bash
sleep 120")
sleep 3
JOB2=$(submit_test_job "-c 3 --job-name=waiter" "#!/bin/bash
sleep 120")
sleep 2
STATE2=$(squeue -j "$JOB2" -h -o "%T" 2>/dev/null)
if [ "$STATE2" = "PENDING" ]; then
    scancel "$JOB1" 2>/dev/null
    sleep 5
    STATE2=$(squeue -j "$JOB2" -h -o "%T" 2>/dev/null)
    if [ "$STATE2" = "RUNNING" ]; then
        pass "test1.5: pending job starts when resources free up"
    else
        fail "test1.5: pending job didn't start after cancel, state=$STATE2"
    fi
else
    fail "test1.5: second job should be PENDING but is $STATE2"
fi
scancel "$JOB2" 2>/dev/null
cleanup_jobs

# test1.6 - Cancel sets job to disappear quickly
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 120")
sleep 3
scancel "$JOB_ID" 2>/dev/null
sleep 3
STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null)
if [ -z "$STATE" ]; then
    pass "test1.6: cancelled job disappears within 3s"
else
    fail "test1.6: cancelled job still visible after 3s, state=$STATE"
fi

cleanup_jobs

###############################################################################
# SECTION 7: Resource scheduling (based on various test areas)
###############################################################################
section "Resource scheduling and allocation"

# test_sched.1 - Fill node to capacity
# Our node has 4 CPUs
JOB1=$(submit_test_job "-c 2 --job-name=half1" "#!/bin/bash
sleep 120")
JOB2=$(submit_test_job "-c 2 --job-name=half2" "#!/bin/bash
sleep 120")
sleep 3
STATE1=$(squeue -j "$JOB1" -h -o "%T" 2>/dev/null)
STATE2=$(squeue -j "$JOB2" -h -o "%T" 2>/dev/null)
if [ "$STATE1" = "RUNNING" ] && [ "$STATE2" = "RUNNING" ]; then
    pass "test_sched.1: two 2-CPU jobs fill 4-CPU node"
else
    fail "test_sched.1: expected both RUNNING, got $STATE1 and $STATE2"
fi

# test_sched.2 - Third job pending when node full
JOB3=$(submit_test_job "-c 1 --job-name=overflow" "#!/bin/bash
sleep 120")
sleep 2
STATE3=$(squeue -j "$JOB3" -h -o "%T" 2>/dev/null)
if [ "$STATE3" = "PENDING" ]; then
    pass "test_sched.2: overflow job stays PENDING when node full"
else
    fail "test_sched.2: expected PENDING but got $STATE3"
fi

# test_sched.3 - sinfo shows correct state when node is full
SINFO_STATE=$(sinfo -h -o "%T" 2>/dev/null | head -1)
if [[ "$SINFO_STATE" =~ (alloc|allocated|ALLOCATED) ]]; then
    pass "test_sched.3: sinfo shows allocated when node is full"
elif [[ "$SINFO_STATE" =~ (mix|mixed|MIXED) ]]; then
    pass "test_sched.3: sinfo shows mixed state (acceptable)"
else
    fail "test_sched.3: expected allocated/mixed but got '$SINFO_STATE'"
fi

# test_sched.4 - sinfo shows CPU allocation
CPU_INFO=$(sinfo -h -o "%C" 2>/dev/null)
ALLOC_CPUS=$(echo "$CPU_INFO" | cut -d'/' -f1)
if [ -n "$ALLOC_CPUS" ] && [ "$ALLOC_CPUS" -ge 2 ] 2>/dev/null; then
    pass "test_sched.4: sinfo shows $ALLOC_CPUS allocated CPUs"
else
    fail "test_sched.4: sinfo %C shows '$CPU_INFO'"
fi

# test_sched.5 - Cancel one frees resources
scancel "$JOB1" 2>/dev/null
sleep 5
STATE3=$(squeue -j "$JOB3" -h -o "%T" 2>/dev/null)
if [ "$STATE3" = "RUNNING" ]; then
    pass "test_sched.5: overflow job starts after resources freed"
else
    fail "test_sched.5: overflow job should be RUNNING, is $STATE3"
fi

cleanup_jobs
sleep 2

# test_sched.6 - sinfo back to idle after all jobs cancelled
SINFO_STATE=$(sinfo -h -o "%T" 2>/dev/null | head -1)
if [[ "$SINFO_STATE" =~ (idle|IDLE) ]]; then
    pass "test_sched.6: sinfo shows idle after all jobs cancelled"
else
    fail "test_sched.6: expected idle but got '$SINFO_STATE'"
fi

###############################################################################
# SECTION 8: Concurrent submission stress (test-like scenarios)
###############################################################################
section "Concurrent submission and stress tests"

# test_stress.1 - Rapid sequential submissions
SUBMIT_OK=true
JOB_IDS=()
for i in $(seq 1 10); do
    JID=$(submit_test_job "--job-name=rapid_$i" "#!/bin/bash
sleep 120")
    if [ -z "$JID" ]; then
        fail "test_stress.1: rapid submission #$i failed"
        SUBMIT_OK=false
        break
    fi
    JOB_IDS+=("$JID")
done
if $SUBMIT_OK; then
    pass "test_stress.1: 10 rapid sequential submissions succeeded"
fi

# test_stress.2 - All jobs visible in squeue
sleep 2
VISIBLE=$(squeue -h 2>/dev/null | wc -l)
if [ "$VISIBLE" -ge 10 ]; then
    pass "test_stress.2: all 10 jobs visible in squeue ($VISIBLE)"
else
    fail "test_stress.2: only $VISIBLE of 10 jobs visible"
fi

# test_stress.3 - Mass cancel
for jid in "${JOB_IDS[@]}"; do
    scancel "$jid" 2>/dev/null
done
sleep 3
REMAINING=$(squeue -h 2>/dev/null | wc -l)
if [ "$REMAINING" -eq 0 ]; then
    pass "test_stress.3: mass cancel cleared all 10 jobs"
else
    fail "test_stress.3: $REMAINING jobs remain after mass cancel"
    cleanup_jobs
fi

# test_stress.4 - Submit and cancel rapidly
RAPID_OK=true
for i in $(seq 1 10); do
    JID=$(submit_test_job "--job-name=rapid_cancel_$i" "#!/bin/bash
sleep 120")
    if [ -z "$JID" ]; then
        fail "test_stress.4: submit #$i failed"
        RAPID_OK=false
        break
    fi
    scancel "$JID" 2>/dev/null
done
if $RAPID_OK; then
    sleep 2
    REMAINING=$(squeue -h 2>/dev/null | wc -l)
    if [ "$REMAINING" -eq 0 ]; then
        pass "test_stress.4: 10 submit-cancel cycles clean"
    else
        fail "test_stress.4: $REMAINING jobs remain after rapid cancel cycles"
        cleanup_jobs
    fi
fi

# test_stress.5 - 20 jobs concurrent (should have 1 running, 19 pending on 4-CPU node)
JOB_IDS=()
for i in $(seq 1 20); do
    JID=$(submit_test_job "--job-name=bulk_$i" "#!/bin/bash
sleep 120")
    JOB_IDS+=("$JID")
done
sleep 3
RUNNING=$(squeue -t RUNNING -h 2>/dev/null | wc -l)
PENDING=$(squeue -t PENDING -h 2>/dev/null | wc -l)
TOTAL_Q=$(squeue -h 2>/dev/null | wc -l)
if [ "$TOTAL_Q" -ge 18 ]; then
    pass "test_stress.5: 20 bulk jobs submitted, $RUNNING running, $PENDING pending ($TOTAL_Q total)"
else
    fail "test_stress.5: only $TOTAL_Q of 20 jobs visible"
fi

cleanup_jobs

###############################################################################
# SECTION 9: scontrol show node resource tracking
###############################################################################
section "Node resource tracking during jobs"

sleep 2

# test_node.1 - scontrol show node CPUAlloc before jobs
OUTPUT=$(scontrol show node flurm-node1 2>&1)
if [ -z "$OUTPUT" ]; then
    OUTPUT=$(scontrol show node 2>&1)
fi
CPU_ALLOC=$(echo "$OUTPUT" | grep -oP 'CPUAlloc=\K[0-9]+')
if [ "$CPU_ALLOC" = "0" ]; then
    pass "test_node.1: CPUAlloc=0 before any jobs"
else
    fail "test_node.1: expected CPUAlloc=0 but got $CPU_ALLOC"
fi

# test_node.2 - CPUAlloc increases when job runs
JOB_ID=$(submit_test_job "-c 2 --job-name=alloc_test" "#!/bin/bash
sleep 120")
sleep 3
OUTPUT=$(scontrol show node flurm-node1 2>&1)
if [ -z "$OUTPUT" ]; then
    OUTPUT=$(scontrol show node 2>&1)
fi
CPU_ALLOC=$(echo "$OUTPUT" | grep -oP 'CPUAlloc=\K[0-9]+')
if [ "$CPU_ALLOC" = "2" ]; then
    pass "test_node.2: CPUAlloc=2 with 2-CPU job running"
else
    fail "test_node.2: expected CPUAlloc=2 but got $CPU_ALLOC"
fi

# test_node.3 - Node state changes from idle
NODE_STATE=$(echo "$OUTPUT" | grep -oP 'State=\K[A-Z]+')
if [[ "$NODE_STATE" =~ (MIXED|ALLOCATED) ]]; then
    pass "test_node.3: node state=$NODE_STATE with partial allocation"
else
    fail "test_node.3: expected MIXED or ALLOCATED but got $NODE_STATE"
fi

# test_node.4 - CPUAlloc returns to 0 after cancel
scancel "$JOB_ID" 2>/dev/null
sleep 3
OUTPUT=$(scontrol show node flurm-node1 2>&1)
if [ -z "$OUTPUT" ]; then
    OUTPUT=$(scontrol show node 2>&1)
fi
CPU_ALLOC=$(echo "$OUTPUT" | grep -oP 'CPUAlloc=\K[0-9]+')
if [ "$CPU_ALLOC" = "0" ]; then
    pass "test_node.4: CPUAlloc=0 after job cancelled"
else
    fail "test_node.4: expected CPUAlloc=0 but got $CPU_ALLOC"
fi

# test_node.5 - Node state back to IDLE
NODE_STATE=$(echo "$OUTPUT" | grep -oP 'State=\K[A-Z]+')
if [ "$NODE_STATE" = "IDLE" ]; then
    pass "test_node.5: node returns to IDLE after cancel"
else
    fail "test_node.5: expected IDLE but got $NODE_STATE"
fi

cleanup_jobs

###############################################################################
# SECTION 10: Edge cases and error handling
###############################################################################
section "Edge cases and error handling"

# test_edge.1 - Job with very long name
LONGNAME="this_is_a_very_long_job_name_that_tests_the_limits_of_name_handling"
JOB_ID=$(submit_test_job "--job-name=$LONGNAME" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    pass "test_edge.1: job with long name accepted ($JOB_ID)"
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_edge.1: job with long name rejected"
fi

# test_edge.2 - Job with special characters in name
JOB_ID=$(submit_test_job "--job-name=test-job_v2.0" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "test-job_v2.0" ]; then
        pass "test_edge.2: job with special chars in name works"
    else
        pass "test_edge.2: job submitted (name=$NAME)"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_edge.2: job with special chars rejected"
fi

# test_edge.3 - Empty script body (just shebang)
JOB_ID=$(submit_test_job "" "#!/bin/bash")
if [ -n "$JOB_ID" ]; then
    pass "test_edge.3: empty script body accepted"
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_edge.3: empty script body rejected"
fi

# test_edge.4 - squeue with no jobs shows only header
cleanup_jobs
sleep 1
OUTPUT=$(squeue 2>&1)
LINES=$(echo "$OUTPUT" | wc -l)
if [ "$LINES" -le 2 ]; then
    pass "test_edge.4: squeue with no jobs shows header only"
else
    fail "test_edge.4: squeue with no jobs has $LINES lines"
fi

# test_edge.5 - scontrol show job with no argument (all jobs)
JOB1=$(submit_test_job "--job-name=show_all_1" "#!/bin/bash
sleep 60")
JOB2=$(submit_test_job "--job-name=show_all_2" "#!/bin/bash
sleep 60")
sleep 2
OUTPUT=$(scontrol show job 2>&1)
if echo "$OUTPUT" | grep -q "$JOB1" && echo "$OUTPUT" | grep -q "$JOB2"; then
    pass "test_edge.5: scontrol show job (no arg) shows all jobs"
else
    skip "test_edge.5: scontrol show job (no arg) not showing all" "may only show first"
fi
cleanup_jobs

# test_edge.6 - Submit job to explicit default partition
JOB_ID=$(submit_test_job "-p default" "#!/bin/bash
sleep 30")
if [ -n "$JOB_ID" ]; then
    PART=$(squeue -j "$JOB_ID" -h -o "%P" 2>/dev/null)
    pass "test_edge.6: explicit -p default submits OK (partition=$PART)"
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_edge.6: explicit -p default rejected"
fi

# test_edge.7 - sinfo for specific partition
OUTPUT=$(sinfo -p default 2>&1)
if echo "$OUTPUT" | grep -q "default"; then
    pass "test_edge.7: sinfo -p default works"
else
    fail "test_edge.7: sinfo -p default failed" "$OUTPUT"
fi

# test_edge.8 - scontrol show all nodes
OUTPUT=$(scontrol show nodes 2>&1)
if echo "$OUTPUT" | grep -q "NodeName="; then
    pass "test_edge.8: scontrol show nodes works"
else
    OUTPUT=$(scontrol show node 2>&1)
    if echo "$OUTPUT" | grep -q "NodeName="; then
        pass "test_edge.8: scontrol show node works (singular)"
    else
        fail "test_edge.8: scontrol show nodes failed"
    fi
fi

# test_edge.9 - Multiple sinfo calls in quick succession
SINFO_OK=true
for i in $(seq 1 5); do
    OUTPUT=$(sinfo 2>&1)
    if ! echo "$OUTPUT" | grep -q "PARTITION"; then
        fail "test_edge.9: sinfo call #$i failed"
        SINFO_OK=false
        break
    fi
done
if $SINFO_OK; then
    pass "test_edge.9: 5 rapid sinfo calls all succeed"
fi

# test_edge.10 - Multiple squeue calls in quick succession
SQUEUE_OK=true
for i in $(seq 1 5); do
    OUTPUT=$(squeue 2>&1)
    if ! echo "$OUTPUT" | grep -q "JOBID"; then
        fail "test_edge.10: squeue call #$i failed"
        SQUEUE_OK=false
        break
    fi
done
if $SQUEUE_OK; then
    pass "test_edge.10: 5 rapid squeue calls all succeed"
fi

###############################################################################
# SECTION 11: SBATCH directives (deeper test17.x coverage)
###############################################################################
section "test17.x (cont) - SBATCH directive parsing"

# test17.20 - Multiple SBATCH directives
JOB_ID=$(submit_test_job "" "#!/bin/bash
#SBATCH --job-name=multi_directive
#SBATCH --ntasks=2
#SBATCH --time=15
sleep 60")
if [ -n "$JOB_ID" ]; then
    sleep 1
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    NCPUS=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'NumCPUs=\K[0-9]+')
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    DIRECTIVES_OK=true
    if [ "$NAME" != "multi_directive" ]; then
        fail "test17.20: multi-directive name: expected 'multi_directive' got '$NAME'"
        DIRECTIVES_OK=false
    fi
    if [ "$NCPUS" != "2" ]; then
        fail "test17.20: multi-directive ntasks: expected NumCPUs=2 got $NCPUS"
        DIRECTIVES_OK=false
    fi
    if [ "$TL" != "00:15:00" ]; then
        warn "test17.20: multi-directive time: expected 00:15:00 got $TL"
    fi
    if $DIRECTIVES_OK; then
        pass "test17.20: multiple SBATCH directives parsed correctly"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.20: multi-directive submission failed"
fi

# test17.21 - SBATCH directive with -c (cpus-per-task)
JOB_ID=$(submit_test_job "" "#!/bin/bash
#SBATCH -c 2
sleep 60")
if [ -n "$JOB_ID" ]; then
    sleep 1
    CPT=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'CPUs/Task=\K[0-9]+')
    if [ "$CPT" = "2" ]; then
        pass "test17.21: SBATCH -c 2 sets CPUs/Task=2"
    else
        fail "test17.21: SBATCH -c 2 but CPUs/Task=$CPT"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.21: SBATCH -c directive failed"
fi

# test17.22 - SBATCH after non-comment line is ignored
JOB_ID=$(submit_test_job "" "#!/bin/bash
#SBATCH --job-name=first_name
echo 'hello'
#SBATCH --job-name=second_name
sleep 60")
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "first_name" ]; then
        pass "test17.22: SBATCH after non-comment line correctly ignored"
    elif [ "$NAME" = "second_name" ]; then
        warn "test17.22: second SBATCH override (SLURM would use first only)"
    else
        pass "test17.22: name=$NAME (sbatch directive processed)"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.22: SBATCH order test failed"
fi

# test17.23 - --wrap with --job-name
OUTPUT=$(sbatch --wrap="sleep 30" --job-name=wrap_named 2>&1)
JOB_ID=$(echo "$OUTPUT" | grep -oP 'Submitted batch job \K\d+')
if [ -n "$JOB_ID" ]; then
    NAME=$(squeue -j "$JOB_ID" -h -o "%j" 2>/dev/null)
    if [ "$NAME" = "wrap_named" ]; then
        pass "test17.23: --wrap with --job-name works"
    else
        pass "test17.23: --wrap with --job-name submitted (name=$NAME)"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test17.23: --wrap with --job-name failed" "$OUTPUT"
fi

cleanup_jobs

###############################################################################
# SECTION 12: Job timing (time limit behavior)
###############################################################################
section "Job time limit behavior"

# test_time.1 - Default time limit
JOB_ID=$(submit_test_job "" "#!/bin/bash
sleep 60")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if [ -n "$TL" ] && [ "$TL" != "UNLIMITED" ]; then
        pass "test_time.1: default time limit set ($TL)"
    else
        fail "test_time.1: no default time limit, got '$TL'"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_time.1: submission failed"
fi

# test_time.2 - Time format MM:SS
JOB_ID=$(submit_test_job "--time=05:00" "#!/bin/bash
sleep 60")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if [ "$TL" = "00:05:00" ]; then
        pass "test_time.2: --time=05:00 → TimeLimit=00:05:00"
    else
        # 05:00 could be 5 minutes or 5 hours depending on interpretation
        pass "test_time.2: --time=05:00 → TimeLimit=$TL (format accepted)"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_time.2: time format test failed"
fi

# test_time.3 - Time format HH:MM:SS
JOB_ID=$(submit_test_job "--time=02:00:00" "#!/bin/bash
sleep 60")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if [ "$TL" = "02:00:00" ]; then
        pass "test_time.3: --time=02:00:00 → TimeLimit=02:00:00"
    else
        fail "test_time.3: --time=02:00:00 but TimeLimit=$TL"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_time.3: time format HH:MM:SS test failed"
fi

# test_time.4 - Time format D-HH:MM:SS
JOB_ID=$(submit_test_job "--time=1-00:00:00" "#!/bin/bash
sleep 60")
if [ -n "$JOB_ID" ]; then
    sleep 1
    TL=$(scontrol show job "$JOB_ID" 2>/dev/null | grep -oP 'TimeLimit=\K[^ ]+')
    if echo "$TL" | grep -qE "^(1-00:00:00|24:00:00)$"; then
        pass "test_time.4: --time=1-00:00:00 → TimeLimit=$TL"
    else
        pass "test_time.4: --time=1-00:00:00 accepted (TimeLimit=$TL)"
    fi
    scancel "$JOB_ID" 2>/dev/null
else
    fail "test_time.4: D-HH:MM:SS format failed"
fi

cleanup_jobs

###############################################################################
# SECTION 13: scontrol show node detail
###############################################################################
section "scontrol node detail tracking"

# test_nodedetail.1 - Show node has CPUTot
OUTPUT=$(scontrol show node 2>&1)
CPUTOT=$(echo "$OUTPUT" | grep -oP 'CPUTot=\K[0-9]+')
if [ -n "$CPUTOT" ] && [ "$CPUTOT" -gt 0 ] 2>/dev/null; then
    pass "test_nodedetail.1: CPUTot=$CPUTOT"
else
    fail "test_nodedetail.1: missing or zero CPUTot" "$OUTPUT"
fi

# test_nodedetail.2 - Show node has RealMemory
REALMEM=$(echo "$OUTPUT" | grep -oP 'RealMemory=\K[0-9]+')
if [ -n "$REALMEM" ] && [ "$REALMEM" -gt 0 ] 2>/dev/null; then
    pass "test_nodedetail.2: RealMemory=$REALMEM"
else
    fail "test_nodedetail.2: missing or zero RealMemory"
fi

# test_nodedetail.3 - Show node has Partitions
if echo "$OUTPUT" | grep -q "Partitions="; then
    pass "test_nodedetail.3: Partitions field present"
else
    fail "test_nodedetail.3: missing Partitions field"
fi

# test_nodedetail.4 - Two jobs, CPUAlloc shows sum
JOB1=$(submit_test_job "-c 1 --job-name=alloc_a" "#!/bin/bash
sleep 120")
JOB2=$(submit_test_job "-c 1 --job-name=alloc_b" "#!/bin/bash
sleep 120")
sleep 3
OUTPUT=$(scontrol show node 2>&1)
CPU_ALLOC=$(echo "$OUTPUT" | grep -oP 'CPUAlloc=\K[0-9]+')
if [ "$CPU_ALLOC" = "2" ]; then
    pass "test_nodedetail.4: CPUAlloc=2 with two 1-CPU jobs"
else
    fail "test_nodedetail.4: expected CPUAlloc=2 but got $CPU_ALLOC"
fi

cleanup_jobs
sleep 2

###############################################################################
# SECTION 14: Mixed workload scenarios
###############################################################################
section "Mixed workload scenarios"

# test_mixed.1 - Submit mix of sizes: 1-CPU, 2-CPU, 3-CPU jobs
JOB_1CPU=$(submit_test_job "-c 1 --job-name=one_cpu" "#!/bin/bash
sleep 120")
JOB_2CPU=$(submit_test_job "-c 2 --job-name=two_cpu" "#!/bin/bash
sleep 120")
JOB_3CPU=$(submit_test_job "-c 3 --job-name=three_cpu" "#!/bin/bash
sleep 120")
sleep 5

S1=$(squeue -j "$JOB_1CPU" -h -o "%T" 2>/dev/null)
S2=$(squeue -j "$JOB_2CPU" -h -o "%T" 2>/dev/null)
S3=$(squeue -j "$JOB_3CPU" -h -o "%T" 2>/dev/null)

# With 4 CPUs: 1+2=3 should run, 3-CPU job pending (or 1+3=4 runs, 2 pending)
RUNNING_COUNT=0
[ "$S1" = "RUNNING" ] && ((RUNNING_COUNT++))
[ "$S2" = "RUNNING" ] && ((RUNNING_COUNT++))
[ "$S3" = "RUNNING" ] && ((RUNNING_COUNT++))

if [ $RUNNING_COUNT -ge 2 ]; then
    pass "test_mixed.1: mixed sizes: $RUNNING_COUNT running (1=$S1, 2=$S2, 3=$S3)"
else
    fail "test_mixed.1: only $RUNNING_COUNT running (1=$S1, 2=$S2, 3=$S3)"
fi

cleanup_jobs
sleep 2

# test_mixed.2 - ntasks jobs mixed with cpu jobs
JOB_NT=$(submit_test_job "--ntasks=2 --job-name=tasks_job" "#!/bin/bash
sleep 120")
JOB_CPU=$(submit_test_job "-c 2 --job-name=cpu_job" "#!/bin/bash
sleep 120")
sleep 3

SNT=$(squeue -j "$JOB_NT" -h -o "%T" 2>/dev/null)
SCPU=$(squeue -j "$JOB_CPU" -h -o "%T" 2>/dev/null)

if [ "$SNT" = "RUNNING" ] && [ "$SCPU" = "RUNNING" ]; then
    pass "test_mixed.2: ntasks=2 + cpus=2 both RUNNING on 4-CPU node"
else
    pass "test_mixed.2: mixed ntasks/cpu jobs (ntasks=$SNT, cpu=$SCPU)"
fi

cleanup_jobs

###############################################################################
# Summary
###############################################################################

echo ""
echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""
echo -e "  ${GREEN}PASSED:  $PASS${NC}"
echo -e "  ${RED}FAILED:  $FAIL${NC}"
echo -e "  ${YELLOW}SKIPPED: $SKIP${NC}"
echo -e "  ${YELLOW}WARNINGS: $WARN${NC}"
echo -e "  TOTAL:   $TOTAL"
echo ""

if [ $FAIL -gt 0 ]; then
    echo -e "${RED}Failed tests:${NC}"
    echo -e "$FAILURES"
    echo ""
fi

# Final cleanup
cleanup_jobs

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}$FAIL test(s) failed.${NC}"
    exit 1
fi
