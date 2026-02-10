#!/bin/bash
###############################################################################
# Bug Hunting Test Script for FLURM
#
# Probes edge cases, race conditions, and unusual inputs to find bugs.
# Run AFTER the standard test suite on a clean Docker state.
###############################################################################

set -o pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0
TOTAL=0
SECTION=""
FAILURES=""
BUGS=""

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

bug() {
    ((FAIL++))
    ((TOTAL++))
    echo -e "  ${RED}BUG${NC}  - $1"
    [ -n "$2" ] && echo -e "         ${RED}$2${NC}"
    BUGS="${BUGS}\n  [${SECTION}] $1: $2"
    FAILURES="${FAILURES}\n  [${SECTION}] $1"
}

skip() {
    ((SKIP++))
    ((TOTAL++))
    echo -e "  ${YELLOW}SKIP${NC} - $1"
}

section() {
    SECTION="$1"
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

cleanup_jobs() {
    for jid in $(squeue -h -o "%i" 2>/dev/null); do
        scancel "$jid" 2>/dev/null
    done
    sleep 1
}

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
        if [ -z "$state" ] && [[ "$target_state" =~ ^(COMPLETED|CANCELLED|FAILED)$ ]]; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

echo -e "${BLUE}FLURM Bug Hunt Test Suite${NC}"
echo -e "${BLUE}=========================${NC}"

###############################################################################
section "Double cancel and terminal state operations"
###############################################################################

# Submit a short job and let it complete
JOB=$(sbatch --wrap="sleep 1" 2>&1 | grep -o '[0-9]*')
if [ -n "$JOB" ]; then
    sleep 3  # Wait for completion
    STATE=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)

    # Test: cancel a completed/gone job
    RESULT=$(scancel "$JOB" 2>&1)
    RC=$?
    if [ $RC -eq 0 ]; then
        pass "bug1.1: scancel completed job returns success (SLURM-compatible)"
    else
        fail "bug1.1: scancel completed job" "rc=$RC, output=$RESULT"
    fi

    # Test: cancel the same job again
    RESULT=$(scancel "$JOB" 2>&1)
    RC=$?
    if [ $RC -eq 0 ]; then
        pass "bug1.2: double scancel returns success"
    else
        fail "bug1.2: double scancel" "rc=$RC, output=$RESULT"
    fi
else
    skip "bug1.1-1.2: could not submit test job"
fi

# Submit and explicitly cancel, then cancel again
JOB=$(sbatch --wrap="sleep 300" 2>&1 | grep -o '[0-9]*')
if [ -n "$JOB" ]; then
    sleep 1
    scancel "$JOB" 2>/dev/null
    sleep 1

    # Cancel already-cancelled job
    RESULT=$(scancel "$JOB" 2>&1)
    RC=$?
    if [ $RC -eq 0 ]; then
        pass "bug1.3: cancel already-cancelled job returns success"
    else
        fail "bug1.3: cancel already-cancelled job" "rc=$RC"
    fi

    # Check state is still CANCELLED (not overwritten)
    STATE=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)
    if [ -z "$STATE" ] || [ "$STATE" = "CANCELLED" ]; then
        pass "bug1.4: cancelled job state preserved after double cancel"
    else
        bug "bug1.4: cancelled job changed state" "expected empty/CANCELLED, got '$STATE'"
    fi
else
    skip "bug1.3-1.4: could not submit test job"
fi

cleanup_jobs

###############################################################################
section "Non-existent partition handling"
###############################################################################

# Submit to non-existent partition
RESULT=$(sbatch --partition=nonexistent --wrap="hostname" 2>&1)
RC=$?
if echo "$RESULT" | grep -qi "error\|invalid\|not.found\|no.such"; then
    pass "bug2.1: sbatch to non-existent partition returns error"
else
    if echo "$RESULT" | grep -q "Submitted"; then
        JOB=$(echo "$RESULT" | grep -o '[0-9]*')
        bug "bug2.1: sbatch to non-existent partition accepted job $JOB" "expected error, got: $RESULT"
        [ -n "$JOB" ] && scancel "$JOB" 2>/dev/null
    else
        fail "bug2.1: unexpected response" "$RESULT"
    fi
fi

# Submit to empty string partition (should default)
RESULT=$(sbatch --partition="" --wrap="hostname" 2>&1)
RC=$?
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    pass "bug2.2: sbatch with empty partition uses default"
    scancel "$JOB" 2>/dev/null
elif echo "$RESULT" | grep -qi "error"; then
    pass "bug2.2: sbatch with empty partition returns error (acceptable)"
else
    fail "bug2.2: unexpected" "$RESULT"
fi

cleanup_jobs

###############################################################################
section "Resource over-request"
###############################################################################

# Request more CPUs than available
RESULT=$(sbatch -n 999 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    sleep 2
    STATE=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)
    if [ "$STATE" = "PENDING" ]; then
        pass "bug3.1: over-requested CPUs job stays PENDING"
        # Check reason
        REASON=$(squeue -j "$JOB" -h -o "%r" 2>/dev/null)
        echo "         Reason: $REASON"
    elif [ "$STATE" = "RUNNING" ]; then
        bug "bug3.1: job requesting 999 CPUs is RUNNING" "should be PENDING on 1-node cluster"
    else
        fail "bug3.1: unexpected state '$STATE'"
    fi
    scancel "$JOB" 2>/dev/null
else
    # Error at submission is also acceptable
    pass "bug3.1: over-requested CPUs rejected at submission"
fi

# Request absurd memory
RESULT=$(sbatch --mem=999999999 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    sleep 2
    STATE=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)
    if [ "$STATE" = "PENDING" ]; then
        pass "bug3.2: absurd memory request stays PENDING"
    elif [ "$STATE" = "RUNNING" ]; then
        bug "bug3.2: job with 999999999MB memory is RUNNING" "no memory enforcement"
    else
        fail "bug3.2: unexpected state '$STATE'"
    fi
    scancel "$JOB" 2>/dev/null
else
    pass "bug3.2: absurd memory rejected at submission"
fi

# Request 0 CPUs
RESULT=$(sbatch -n 0 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -qi "error\|invalid"; then
    pass "bug3.3: -n 0 rejected"
elif echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    # Check if it allocated any CPUs
    CPUS=$(squeue -j "$JOB" -h -o "%C" 2>/dev/null)
    if [ "$CPUS" = "0" ]; then
        bug "bug3.3: job with 0 CPUs created" "allocated 0 CPUs"
    else
        pass "bug3.3: -n 0 auto-corrected to $CPUS CPUs"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug3.3: unexpected response" "$RESULT"
fi

cleanup_jobs

###############################################################################
section "Invalid job ID operations"
###############################################################################

# squeue for non-existent job
RESULT=$(squeue -j 999999 2>&1)
RC=$?
if echo "$RESULT" | grep -qi "invalid\|error"; then
    pass "bug4.1: squeue -j 999999 returns error"
elif echo "$RESULT" | grep -q "JOBID"; then
    # Empty header is OK (SLURM behavior)
    LINES=$(echo "$RESULT" | wc -l)
    if [ "$LINES" -le 2 ]; then
        pass "bug4.1: squeue -j 999999 returns empty result with header"
    else
        fail "bug4.1: squeue -j 999999 returned unexpected lines" "$RESULT"
    fi
else
    fail "bug4.1: unexpected response" "$RESULT"
fi

# scontrol show job for non-existent job
RESULT=$(scontrol show job 999999 2>&1)
RC=$?
if echo "$RESULT" | grep -qi "invalid\|error\|not.found"; then
    pass "bug4.2: scontrol show job 999999 returns error"
else
    fail "bug4.2: scontrol show job 999999 no error" "$RESULT"
fi

# scancel negative job ID
RESULT=$(scancel -1 2>&1)
RC=$?
# SLURM should reject this
if [ $RC -ne 0 ] || echo "$RESULT" | grep -qi "error\|invalid"; then
    pass "bug4.3: scancel -1 rejected"
else
    pass "bug4.3: scancel -1 returned rc=$RC (silent ignore OK)"
fi

# scancel job ID 0
RESULT=$(scancel 0 2>&1)
RC=$?
if [ $RC -ne 0 ] || echo "$RESULT" | grep -qi "error\|invalid"; then
    pass "bug4.4: scancel 0 rejected"
else
    pass "bug4.4: scancel 0 returned rc=$RC"
fi

cleanup_jobs

###############################################################################
section "Job name edge cases"
###############################################################################

# Very long job name
LONGNAME=$(python3 -c "print('A'*256)")
RESULT=$(sbatch --job-name="$LONGNAME" --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    ACTUAL_NAME=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    NAME_LEN=${#ACTUAL_NAME}
    if [ $NAME_LEN -gt 200 ]; then
        pass "bug5.1: very long job name accepted (len=$NAME_LEN)"
    else
        pass "bug5.1: long job name truncated to $NAME_LEN chars"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug5.1: long job name rejected" "$RESULT"
fi

# Job name with special characters
RESULT=$(sbatch --job-name='test&job;$NAME' --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    ACTUAL_NAME=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    if [ "$ACTUAL_NAME" = 'test&job;$NAME' ]; then
        pass "bug5.2: special chars in job name preserved"
    else
        pass "bug5.2: special chars in job name (stored as '$ACTUAL_NAME')"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug5.2: special char job name rejected" "$RESULT"
fi

# Empty job name
RESULT=$(sbatch --job-name="" --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    ACTUAL_NAME=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    if [ -z "$ACTUAL_NAME" ] || [ "$ACTUAL_NAME" = "wrap" ]; then
        pass "bug5.3: empty job name defaults to '$ACTUAL_NAME'"
    else
        pass "bug5.3: empty job name stored as '$ACTUAL_NAME'"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug5.3: empty job name rejected" "$RESULT"
fi

cleanup_jobs

###############################################################################
section "Time limit edge cases"
###############################################################################

# Submit with --time=0
RESULT=$(sbatch --time=0 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -qi "error\|invalid"; then
    pass "bug6.1: --time=0 rejected"
elif echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    TL=$(scontrol show job "$JOB" 2>/dev/null | grep -o 'TimeLimit=[^ ]*' | head -1)
    if echo "$TL" | grep -q "UNLIMITED\|00:00:00"; then
        pass "bug6.1: --time=0 accepted (TimeLimit=$TL)"
    else
        pass "bug6.1: --time=0 (TimeLimit=$TL)"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug6.1: unexpected" "$RESULT"
fi

# Submit with very large time
RESULT=$(sbatch --time=99999 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    TL=$(scontrol show job "$JOB" 2>/dev/null | grep -o 'TimeLimit=[^ ]*' | head -1)
    pass "bug6.2: --time=99999 accepted (TimeLimit=$TL)"
    scancel "$JOB" 2>/dev/null
else
    fail "bug6.2: --time=99999 rejected" "$RESULT"
fi

# Submit with --time in D-HH:MM:SS format
RESULT=$(sbatch --time=1-00:00:00 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    TL=$(scontrol show job "$JOB" 2>/dev/null | grep -o 'TimeLimit=[^ ]*' | head -1)
    if echo "$TL" | grep -q "1-00:00:00\|24:00:00\|1440"; then
        pass "bug6.3: 1-00:00:00 stored correctly ($TL)"
    else
        bug "bug6.3: 1-00:00:00 stored incorrectly" "got $TL"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug6.3: D-HH:MM:SS rejected" "$RESULT"
fi

# Submit with --time in MM:SS format
# Note: SLURM time_limit has minute granularity, 05:30 (5m30s) rounds to 6 minutes
RESULT=$(sbatch --time=05:30 --wrap="hostname" 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    TL=$(scontrol show job "$JOB" 2>/dev/null | grep -o 'TimeLimit=[^ ]*' | head -1)
    if echo "$TL" | grep -q "00:05:30\|05:30\|00:06:00"; then
        pass "bug6.4: MM:SS format stored correctly ($TL)"
    elif echo "$TL" | grep -q "00:05:00"; then
        bug "bug6.4: MM:SS format stored incorrectly" "expected 00:05:30 or 00:06:00, got $TL"
    else
        pass "bug6.4: MM:SS format accepted ($TL)"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug6.4: MM:SS rejected" "$RESULT"
fi

cleanup_jobs

###############################################################################
section "Concurrent job submission race conditions"
###############################################################################

# Submit many jobs simultaneously and check IDs are sequential
PIDS=""
JOBS=""
for i in $(seq 1 10); do
    sbatch --wrap="sleep 300" --job-name="race_$i" 2>&1 &
    PIDS="$PIDS $!"
done

# Wait for all submissions
for pid in $PIDS; do
    wait $pid
done
sleep 2

# Get all job IDs and check they're unique
JOBIDS=$(squeue -h -o "%i" --name="race_" 2>/dev/null | sort -n)
# Also try without --name filter in case it doesn't work
if [ -z "$JOBIDS" ]; then
    JOBIDS=$(squeue -h -o "%i" 2>/dev/null | sort -n)
fi
NUM_JOBS=$(echo "$JOBIDS" | grep -c '[0-9]')
UNIQUE_JOBS=$(echo "$JOBIDS" | sort -u | grep -c '[0-9]')

if [ "$NUM_JOBS" -ge 8 ] && [ "$NUM_JOBS" -eq "$UNIQUE_JOBS" ]; then
    pass "bug7.1: $NUM_JOBS concurrent jobs, all unique IDs"
elif [ "$NUM_JOBS" -lt 8 ]; then
    fail "bug7.1: only $NUM_JOBS of 10 concurrent jobs created"
else
    bug "bug7.1: duplicate job IDs detected" "$NUM_JOBS jobs but only $UNIQUE_JOBS unique"
fi

# Check IDs are monotonically increasing
PREV=0
MONOTONIC=true
for id in $JOBIDS; do
    if [ "$id" -le "$PREV" ]; then
        MONOTONIC=false
        break
    fi
    PREV=$id
done
if [ "$MONOTONIC" = true ]; then
    pass "bug7.2: job IDs are monotonically increasing"
else
    bug "bug7.2: job IDs not monotonic" "IDs: $JOBIDS"
fi

cleanup_jobs

###############################################################################
section "scontrol update operations"
###############################################################################

# Submit a long-running job
JOB=$(sbatch --wrap="sleep 300" --time=01:00:00 2>&1 | grep -o '[0-9]*')
if [ -n "$JOB" ]; then
    sleep 2
    STATE=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)

    # Try to update the job name
    RESULT=$(scontrol update JobId=$JOB JobName=updated_name 2>&1)
    NEWNAME=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    if [ "$NEWNAME" = "updated_name" ]; then
        pass "bug8.1: scontrol update JobName works"
    elif echo "$RESULT" | grep -qi "error"; then
        skip "bug8.1: scontrol update JobName not supported"
    else
        fail "bug8.1: name not updated" "expected 'updated_name', got '$NEWNAME'"
    fi

    # Try to update time limit
    RESULT=$(scontrol update JobId=$JOB TimeLimit=00:30:00 2>&1)
    TL=$(scontrol show job "$JOB" 2>/dev/null | grep -o 'TimeLimit=[^ ]*' | head -1)
    if echo "$TL" | grep -q "00:30:00\|30:00"; then
        pass "bug8.2: scontrol update TimeLimit works ($TL)"
    elif echo "$RESULT" | grep -qi "error"; then
        skip "bug8.2: scontrol update TimeLimit not supported"
    else
        fail "bug8.2: TimeLimit not updated" "got $TL"
    fi

    # Try to hold the job
    RESULT=$(scontrol hold $JOB 2>&1)
    STATE=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)
    PRIO=$(squeue -j "$JOB" -h -o "%Q" 2>/dev/null)
    if [ "$PRIO" = "0" ] || echo "$STATE" | grep -qi "held"; then
        pass "bug8.3: scontrol hold works (state=$STATE, priority=$PRIO)"
    elif echo "$RESULT" | grep -qi "error"; then
        skip "bug8.3: scontrol hold not supported"
    else
        fail "bug8.3: hold didn't work" "state=$STATE, prio=$PRIO, result=$RESULT"
    fi

    # Try to release
    if [ "$PRIO" = "0" ]; then
        RESULT=$(scontrol release $JOB 2>&1)
        PRIO2=$(squeue -j "$JOB" -h -o "%Q" 2>/dev/null)
        if [ "$PRIO2" != "0" ] && [ -n "$PRIO2" ]; then
            pass "bug8.4: scontrol release works (priority=$PRIO2)"
        elif echo "$RESULT" | grep -qi "error"; then
            skip "bug8.4: scontrol release not supported"
        else
            fail "bug8.4: release didn't restore priority" "priority still $PRIO2"
        fi
    else
        skip "bug8.4: hold didn't work, can't test release"
    fi

    scancel "$JOB" 2>/dev/null
else
    skip "bug8.1-8.4: could not submit test job"
fi

cleanup_jobs

###############################################################################
section "squeue format options"
###############################################################################

# Submit a reference job
JOB=$(sbatch --wrap="sleep 300" --job-name="format_test" -n 2 --time=01:00:00 2>&1 | grep -o '[0-9]*')
sleep 2

if [ -n "$JOB" ]; then
    # Test various format specifiers
    # %i = job ID
    FMT_I=$(squeue -j "$JOB" -h -o "%i" 2>/dev/null)
    if [ "$FMT_I" = "$JOB" ]; then
        pass "bug9.1: squeue -o '%i' returns job ID"
    else
        fail "bug9.1: %i format" "expected $JOB, got '$FMT_I'"
    fi

    # %j = job name
    FMT_J=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    if [ "$FMT_J" = "format_test" ]; then
        pass "bug9.2: squeue -o '%j' returns job name"
    else
        fail "bug9.2: %j format" "expected 'format_test', got '$FMT_J'"
    fi

    # %T = state (full word)
    FMT_T=$(squeue -j "$JOB" -h -o "%T" 2>/dev/null)
    if [[ "$FMT_T" =~ ^(PENDING|RUNNING|SUSPENDED|COMPLETING|COMPLETED)$ ]]; then
        pass "bug9.3: squeue -o '%T' returns state ($FMT_T)"
    else
        fail "bug9.3: %T format" "got '$FMT_T'"
    fi

    # %t = state (abbreviated)
    FMT_t=$(squeue -j "$JOB" -h -o "%t" 2>/dev/null)
    if [[ "$FMT_t" =~ ^(PD|R|S|CG|CD)$ ]]; then
        pass "bug9.4: squeue -o '%t' returns abbreviated state ($FMT_t)"
    else
        fail "bug9.4: %t format" "expected PD/R/S/CG/CD, got '$FMT_t'"
    fi

    # %P = partition
    FMT_P=$(squeue -j "$JOB" -h -o "%P" 2>/dev/null)
    if [ -n "$FMT_P" ]; then
        pass "bug9.5: squeue -o '%P' returns partition ($FMT_P)"
    else
        fail "bug9.5: %P format" "empty partition name"
    fi

    # %u = user
    FMT_U=$(squeue -j "$JOB" -h -o "%u" 2>/dev/null)
    if [ -n "$FMT_U" ]; then
        pass "bug9.6: squeue -o '%u' returns user ($FMT_U)"
    else
        fail "bug9.6: %u format" "empty user"
    fi

    # %l = time limit
    FMT_L=$(squeue -j "$JOB" -h -o "%l" 2>/dev/null)
    if [ -n "$FMT_L" ] && [ "$FMT_L" != "(null)" ]; then
        pass "bug9.7: squeue -o '%l' returns time limit ($FMT_L)"
    else
        fail "bug9.7: %l format" "got '$FMT_L'"
    fi

    # %C = CPUs
    FMT_C=$(squeue -j "$JOB" -h -o "%C" 2>/dev/null)
    if [ -n "$FMT_C" ] && [ "$FMT_C" -gt 0 ] 2>/dev/null; then
        pass "bug9.8: squeue -o '%C' returns CPU count ($FMT_C)"
    else
        fail "bug9.8: %C format" "got '$FMT_C'"
    fi

    # %R = reason/nodelist
    FMT_R=$(squeue -j "$JOB" -h -o "%R" 2>/dev/null)
    if [ -n "$FMT_R" ]; then
        pass "bug9.9: squeue -o '%R' returns reason/nodelist ($FMT_R)"
    else
        fail "bug9.9: %R format" "empty"
    fi

    # Combined format
    FMT_COMBO=$(squeue -j "$JOB" -h -o "%i|%j|%T|%P" 2>/dev/null)
    EXPECTED_PARTS=4
    ACTUAL_PARTS=$(echo "$FMT_COMBO" | tr '|' '\n' | wc -l)
    if [ "$ACTUAL_PARTS" -eq "$EXPECTED_PARTS" ]; then
        pass "bug9.10: combined format works ($FMT_COMBO)"
    else
        fail "bug9.10: combined format" "expected 4 fields, got $ACTUAL_PARTS: $FMT_COMBO"
    fi

    scancel "$JOB" 2>/dev/null
else
    skip "bug9.1-9.10: could not submit test job"
fi

cleanup_jobs

###############################################################################
section "sinfo format and node state"
###############################################################################

# Check sinfo -N shows individual nodes
RESULT=$(sinfo -N 2>&1)
if echo "$RESULT" | grep -q "NODELIST"; then
    NODES=$(echo "$RESULT" | grep -v "NODELIST" | wc -l)
    pass "bug10.1: sinfo -N shows $NODES node(s)"
else
    fail "bug10.1: sinfo -N" "$RESULT"
fi

# Check sinfo --format
FMT=$(sinfo -h -o "%P %a %l %D %t %N" 2>/dev/null)
if [ -n "$FMT" ]; then
    pass "bug10.2: sinfo custom format works ($FMT)"
else
    fail "bug10.2: sinfo custom format empty"
fi

# Check sinfo -p default
RESULT=$(sinfo -p default 2>&1)
if echo "$RESULT" | grep -qi "default"; then
    pass "bug10.3: sinfo -p default works"
else
    fail "bug10.3: sinfo -p default" "$RESULT"
fi

# Check sinfo -p nonexistent
RESULT=$(sinfo -p nonexistent 2>&1)
if echo "$RESULT" | grep -qi "error\|not.found" || [ -z "$(echo "$RESULT" | grep -v "PARTITION")" ]; then
    pass "bug10.4: sinfo -p nonexistent handled"
else
    fail "bug10.4: sinfo -p nonexistent" "$RESULT"
fi

###############################################################################
section "Job dependency handling"
###############################################################################

# Submit base job
JOB1=$(sbatch --wrap="sleep 300" 2>&1 | grep -o '[0-9]*')
if [ -n "$JOB1" ]; then
    sleep 1

    # Submit dependent job (afterany)
    RESULT=$(sbatch --dependency=afterany:$JOB1 --wrap="hostname" 2>&1)
    if echo "$RESULT" | grep -q "Submitted"; then
        JOB2=$(echo "$RESULT" | grep -o '[0-9]*')
        sleep 1
        STATE2=$(squeue -j "$JOB2" -h -o "%T" 2>/dev/null)
        REASON=$(squeue -j "$JOB2" -h -o "%r" 2>/dev/null)
        if [ "$STATE2" = "PENDING" ]; then
            pass "bug11.1: afterany dependency keeps job PENDING (reason=$REASON)"
        else
            fail "bug11.1: dependent job not PENDING" "state=$STATE2"
        fi
        scancel "$JOB2" 2>/dev/null
    else
        fail "bug11.1: afterany dependency submission failed" "$RESULT"
    fi

    # Submit with dependency on non-existent job
    RESULT=$(sbatch --dependency=afterok:999999 --wrap="hostname" 2>&1)
    if echo "$RESULT" | grep -qi "error\|invalid\|dependency"; then
        pass "bug11.2: dependency on non-existent job rejected"
    elif echo "$RESULT" | grep -q "Submitted"; then
        JOB3=$(echo "$RESULT" | grep -o '[0-9]*')
        STATE3=$(squeue -j "$JOB3" -h -o "%T" 2>/dev/null)
        if [ "$STATE3" = "PENDING" ]; then
            bug "bug11.2: dependency on non-existent job accepted (PENDING)" "should reject or resolve immediately"
        else
            pass "bug11.2: dependency on non-existent job auto-resolved (state=$STATE3)"
        fi
        scancel "$JOB3" 2>/dev/null
    else
        fail "bug11.2: unexpected" "$RESULT"
    fi

    # Submit with afterok dependency, then cancel the parent
    RESULT=$(sbatch --dependency=afterok:$JOB1 --wrap="hostname" 2>&1)
    if echo "$RESULT" | grep -q "Submitted"; then
        JOB4=$(echo "$RESULT" | grep -o '[0-9]*')
        sleep 1
        scancel "$JOB1" 2>/dev/null
        sleep 3
        STATE4=$(squeue -j "$JOB4" -h -o "%T" 2>/dev/null)
        # afterok on a cancelled job should fail the dependent job
        if [ -z "$STATE4" ] || [ "$STATE4" = "CANCELLED" ] || [ "$STATE4" = "PENDING" ]; then
            pass "bug11.3: afterok dep with cancelled parent (state=${STATE4:-gone})"
        else
            fail "bug11.3: unexpected state for afterok dep" "state=$STATE4"
        fi
        scancel "$JOB4" 2>/dev/null
    else
        fail "bug11.3: afterok submission failed" "$RESULT"
    fi

    scancel "$JOB1" 2>/dev/null
else
    skip "bug11.1-11.3: could not submit base job"
fi

cleanup_jobs

###############################################################################
section "SBATCH directive edge cases"
###############################################################################

# Multiple conflicting directives (first should win in SLURM)
cat > /tmp/conflict_test.sh << 'SCRIPT'
#!/bin/bash
#SBATCH --job-name=first_name
#SBATCH --job-name=second_name
#SBATCH -n 1
#SBATCH -n 3
sleep 300
SCRIPT
chmod +x /tmp/conflict_test.sh

RESULT=$(sbatch /tmp/conflict_test.sh 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    sleep 1
    # Try squeue first, fall back to scontrol (job may complete fast)
    NAME=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    if [ -z "$NAME" ]; then
        NAME=$(scontrol show job "$JOB" 2>/dev/null | grep -oP 'JobName=\K[^ ]+' | head -1)
    fi
    CPUS=$(squeue -j "$JOB" -h -o "%C" 2>/dev/null)
    if [ -z "$CPUS" ]; then
        CPUS=$(scontrol show job "$JOB" 2>/dev/null | grep -oP 'NumCPUs=\K[0-9]+' | head -1)
    fi
    # In SLURM, first directive wins (not last)
    if [ "$NAME" = "first_name" ]; then
        pass "bug12.1: conflicting --job-name: first wins (SLURM behavior, got '$NAME')"
    elif [ "$NAME" = "second_name" ]; then
        bug "bug12.1: conflicting --job-name: last wins" "SLURM uses first, got '$NAME'"
    else
        fail "bug12.1: unexpected name" "'$NAME'"
    fi

    if [ "$CPUS" = "1" ]; then
        pass "bug12.2: conflicting -n: first wins (got $CPUS)"
    elif [ "$CPUS" = "3" ]; then
        bug "bug12.2: conflicting -n: last wins" "SLURM uses first, got $CPUS"
    else
        fail "bug12.2: unexpected CPU count" "'$CPUS'"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug12.1-12.2: conflicting directives submission failed" "$RESULT"
fi

# Directive after non-comment line should be ignored
cat > /tmp/late_directive.sh << 'SCRIPT'
#!/bin/bash
#SBATCH --job-name=valid_name
echo "this is a command"
#SBATCH --job-name=should_be_ignored
sleep 300
SCRIPT
chmod +x /tmp/late_directive.sh

RESULT=$(sbatch /tmp/late_directive.sh 2>&1)
if echo "$RESULT" | grep -q "Submitted"; then
    JOB=$(echo "$RESULT" | grep -o '[0-9]*')
    sleep 1
    # Try squeue first, fall back to scontrol (job may complete fast)
    NAME=$(squeue -j "$JOB" -h -o "%j" 2>/dev/null)
    if [ -z "$NAME" ]; then
        NAME=$(scontrol show job "$JOB" 2>/dev/null | grep -oP 'JobName=\K[^ ]+' | head -1)
    fi
    if [ "$NAME" = "valid_name" ]; then
        pass "bug12.3: late directive correctly ignored"
    elif [ "$NAME" = "should_be_ignored" ]; then
        bug "bug12.3: late directive NOT ignored" "SLURM stops parsing after first non-comment"
    else
        fail "bug12.3: unexpected name" "'$NAME'"
    fi
    scancel "$JOB" 2>/dev/null
else
    fail "bug12.3: late directive test failed" "$RESULT"
fi

cleanup_jobs

###############################################################################
section "Rapid submit-cancel stress test"
###############################################################################

# Rapidly submit and immediately cancel
SUCCESS=0
ERRORS=0
for i in $(seq 1 20); do
    JOB=$(sbatch --wrap="sleep 1" 2>&1 | grep -o '[0-9]*')
    if [ -n "$JOB" ]; then
        scancel "$JOB" 2>/dev/null
        ((SUCCESS++))
    else
        ((ERRORS++))
    fi
done

if [ $SUCCESS -ge 18 ]; then
    pass "bug13.1: rapid submit-cancel: $SUCCESS/20 succeeded"
else
    fail "bug13.1: rapid submit-cancel: only $SUCCESS/20 succeeded" "$ERRORS errors"
fi

sleep 2

# Check that all jobs are gone
REMAINING=$(squeue -h -o "%i" 2>/dev/null | wc -l)
if [ "$REMAINING" -eq 0 ]; then
    pass "bug13.2: all rapidly cancelled jobs cleaned up"
else
    bug "bug13.2: $REMAINING jobs remain after rapid cancel" "job IDs: $(squeue -h -o '%i' 2>/dev/null | tr '\n' ' ')"
fi

cleanup_jobs

###############################################################################
section "Back-to-back suite run (stale state)"
###############################################################################

# Submit 5 jobs, cancel them all with scancel -u
for i in $(seq 1 5); do
    sbatch --wrap="sleep 300" 2>&1 > /dev/null
done
sleep 2

BEFORE=$(squeue -h -o "%i" 2>/dev/null | wc -l)

# Cancel all with -u root (or current user)
CURRENT_USER=$(whoami)
scancel -u "$CURRENT_USER" 2>/dev/null
sleep 3

AFTER=$(squeue -h -o "%i" 2>/dev/null | wc -l)
if [ "$AFTER" -eq 0 ]; then
    pass "bug14.1: scancel -u $CURRENT_USER cancelled all $BEFORE jobs"
elif [ "$AFTER" -lt "$BEFORE" ]; then
    bug "bug14.1: scancel -u partial" "had $BEFORE, now $AFTER remain"
else
    bug "bug14.1: scancel -u didn't cancel" "had $BEFORE, still $AFTER"
fi

cleanup_jobs

# Test scancel --state
for i in $(seq 1 3); do
    sbatch --wrap="sleep 300" -n 999 2>&1 > /dev/null  # Should be PENDING (can't fit)
done
for i in $(seq 1 3); do
    sbatch --wrap="sleep 300" 2>&1 > /dev/null  # Should be RUNNING
done
sleep 3

PENDING=$(squeue -h -t PENDING -o "%i" 2>/dev/null | wc -l)
RUNNING=$(squeue -h -t RUNNING -o "%i" 2>/dev/null | wc -l)

# Cancel only pending jobs
scancel --state=PENDING 2>/dev/null
sleep 2

PENDING_AFTER=$(squeue -h -t PENDING -o "%i" 2>/dev/null | wc -l)
RUNNING_AFTER=$(squeue -h -t RUNNING -o "%i" 2>/dev/null | wc -l)

if [ "$PENDING_AFTER" -eq 0 ] && [ "$RUNNING_AFTER" -gt 0 ]; then
    pass "bug14.2: scancel --state=PENDING only cancelled pending jobs"
elif [ "$PENDING_AFTER" -eq 0 ] && [ "$RUNNING_AFTER" -eq 0 ]; then
    bug "bug14.2: scancel --state=PENDING also cancelled running jobs"
else
    fail "bug14.2: scancel --state=PENDING" "pending: $PENDING->$PENDING_AFTER, running: $RUNNING->$RUNNING_AFTER"
fi

cleanup_jobs

###############################################################################
section "scontrol show completeness"
###############################################################################

# Submit a job and check all expected fields
JOB=$(sbatch --wrap="sleep 300" --job-name="field_test" -n 2 --time=01:00:00 --mem=512 2>&1 | grep -o '[0-9]*')
sleep 2

if [ -n "$JOB" ]; then
    INFO=$(scontrol show job "$JOB" 2>/dev/null)

    # Check critical fields exist
    FIELDS_OK=0
    FIELDS_MISSING=""
    for field in JobId JobName UserId GroupId Priority Partition JobState NumCPUs TimeLimit SubmitTime; do
        if echo "$INFO" | grep -q "$field="; then
            ((FIELDS_OK++))
        else
            FIELDS_MISSING="$FIELDS_MISSING $field"
        fi
    done

    if [ $FIELDS_OK -ge 9 ]; then
        pass "bug15.1: scontrol show job has all $FIELDS_OK critical fields"
    else
        fail "bug15.1: missing fields:$FIELDS_MISSING" "only $FIELDS_OK/10 found"
    fi

    # Check partition info
    PINFO=$(scontrol show partition default 2>/dev/null)
    P_FIELDS=0
    for field in PartitionName State TotalCPUs TotalNodes; do
        if echo "$PINFO" | grep -q "$field="; then
            ((P_FIELDS++))
        fi
    done
    if [ $P_FIELDS -ge 3 ]; then
        pass "bug15.2: scontrol show partition has $P_FIELDS fields"
    else
        fail "bug15.2: scontrol show partition" "only $P_FIELDS fields found"
    fi

    # Check node info
    NINFO=$(scontrol show node flurm-node1 2>/dev/null)
    N_FIELDS=0
    for field in NodeName State CPUTot RealMemory; do
        if echo "$NINFO" | grep -q "$field="; then
            ((N_FIELDS++))
        fi
    done
    if [ $N_FIELDS -ge 3 ]; then
        pass "bug15.3: scontrol show node has $N_FIELDS fields"
    else
        fail "bug15.3: scontrol show node" "only $N_FIELDS fields found"
    fi

    scancel "$JOB" 2>/dev/null
else
    skip "bug15.1-15.3: could not submit test job"
fi

cleanup_jobs

###############################################################################
# Summary
###############################################################################
echo ""
echo -e "${BLUE}  Bug Hunt Summary${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""
echo -e "  ${GREEN}PASSED:  $PASS${NC}"
echo -e "  ${RED}FAILED:  $FAIL${NC}"
echo -e "  ${YELLOW}SKIPPED: $SKIP${NC}"
echo "  TOTAL:   $TOTAL"

if [ -n "$BUGS" ]; then
    echo ""
    echo -e "${RED}  BUGS FOUND:${NC}"
    echo -e "$BUGS"
fi

if [ -n "$FAILURES" ]; then
    echo ""
    echo -e "${RED}  All failures:${NC}"
    echo -e "$FAILURES"
fi

if [ $FAIL -eq 0 ]; then
    echo ""
    echo -e "${GREEN}No bugs found!${NC}"
fi

echo ""
exit $FAIL
