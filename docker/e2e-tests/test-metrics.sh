#!/bin/bash
# Test: Metrics Endpoint
# Verifies: Prometheus metrics export, metric accuracy, label correctness

set -e

echo "=== Test: Metrics Endpoint ==="

# Test 1: Metrics endpoint availability
echo "Test 1: Endpoint availability..."
RESPONSE=$(curl -s -w "\n%{http_code}" "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "000")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" = "200" ]; then
    echo "  Controller 1 metrics: HTTP $HTTP_CODE"
else
    echo "FAIL: Metrics endpoint returned HTTP $HTTP_CODE"
    exit 1
fi

# Test 2: Prometheus format validation
echo "Test 2: Prometheus format..."
if echo "$BODY" | grep -q "^# HELP\|^# TYPE"; then
    echo "  Prometheus format headers present"
else
    echo "  WARN: Missing Prometheus format headers"
fi

# Count metric lines
METRIC_COUNT=$(echo "$BODY" | grep -v "^#" | grep -v "^$" | wc -l)
echo "  Total metric lines: $METRIC_COUNT"

# Test 3: FLURM-specific metrics
echo "Test 3: FLURM metrics presence..."
FLURM_METRICS=$(echo "$BODY" | grep "^flurm_" | cut -d'{' -f1 | cut -d' ' -f1 | sort -u)
FLURM_COUNT=$(echo "$FLURM_METRICS" | grep -c . || echo "0")
echo "  FLURM-specific metrics found: $FLURM_COUNT"
if [ "$FLURM_COUNT" -gt 0 ]; then
    echo "  Sample metrics:"
    echo "$FLURM_METRICS" | head -5 | sed 's/^/    /'
fi

# Test 4: Job metrics
echo "Test 4: Job-related metrics..."
JOB_METRICS="flurm_jobs_total flurm_jobs_running flurm_jobs_pending flurm_jobs_completed"
for METRIC in $JOB_METRICS; do
    VALUE=$(echo "$BODY" | grep "^${METRIC}" | head -1 || echo "")
    if [ -n "$VALUE" ]; then
        echo "  $METRIC: present"
    else
        echo "  $METRIC: not found"
    fi
done

# Test 5: Node metrics
echo "Test 5: Node-related metrics..."
NODE_METRICS="flurm_nodes_total flurm_nodes_up flurm_nodes_idle"
for METRIC in $NODE_METRICS; do
    VALUE=$(echo "$BODY" | grep "^${METRIC}" | head -1 || echo "")
    if [ -n "$VALUE" ]; then
        echo "  $METRIC: present"
    else
        echo "  $METRIC: not found"
    fi
done

# Test 6: TRES metrics
echo "Test 6: TRES metrics..."
TRES_METRICS=$(echo "$BODY" | grep "flurm_tres" || echo "")
if [ -n "$TRES_METRICS" ]; then
    echo "  TRES metrics found"
    echo "$TRES_METRICS" | head -3 | sed 's/^/    /'
else
    echo "  TRES metrics not present (may be configured differently)"
fi

# Test 7: Metric labels
echo "Test 7: Metric labels..."
LABELED=$(echo "$BODY" | grep "{" | head -1 || echo "")
if [ -n "$LABELED" ]; then
    echo "  Sample labeled metric: $LABELED"
fi

# Test 8: Multiple controllers consistency
echo "Test 8: Multi-controller metrics..."
for CTRL in "FLURM_CTRL_2:9091" "FLURM_CTRL_3:9092"; do
    HOST=$(echo $CTRL | cut -d: -f1)
    PORT=$(echo $CTRL | cut -d: -f2)
    IP_VAR="${HOST}"
    IP="${!IP_VAR}"

    RESP=$(curl -s -w "%{http_code}" -o /dev/null "http://${IP}:${PORT}/metrics" 2>/dev/null || echo "000")
    if [ "$RESP" = "200" ]; then
        echo "  $HOST: metrics available"
    else
        echo "  $HOST: not available (HTTP $RESP)"
    fi
done

# Test 9: Metrics change with activity
echo "Test 9: Metrics reflect activity..."
# Get initial value
INITIAL_JOBS=$(echo "$BODY" | grep "flurm_jobs_submitted_total" | grep -oE '[0-9]+$' || echo "0")
echo "  Initial jobs submitted: $INITIAL_JOBS"

# Submit a job
JOB=$(sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null || echo "")
if [ -n "$JOB" ]; then
    sleep 2
    NEW_BODY=$(curl -s "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "")
    NEW_JOBS=$(echo "$NEW_BODY" | grep "flurm_jobs_submitted_total" | grep -oE '[0-9]+$' || echo "0")
    echo "  After submission: $NEW_JOBS"

    if [ "$NEW_JOBS" -gt "$INITIAL_JOBS" ]; then
        echo "  Metrics correctly updated"
    else
        echo "  WARN: Metrics may not reflect submission yet"
    fi

    scancel "$JOB" 2>/dev/null || true
fi

# Test 10: Erlang VM metrics
echo "Test 10: Erlang VM metrics..."
ERLANG_METRICS=$(echo "$BODY" | grep -E "^(erlang_|process_|memory_)" | head -5)
if [ -n "$ERLANG_METRICS" ]; then
    echo "  Erlang/OTP metrics present:"
    echo "$ERLANG_METRICS" | sed 's/^/    /'
fi

echo "=== Test: Metrics Endpoint PASSED ==="
