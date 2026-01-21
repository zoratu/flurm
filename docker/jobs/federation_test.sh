#!/bin/bash
# FLURM Federation Test Script
# Tests multi-cluster federation capabilities using the FLURM REST API

set -e

echo "=============================================="
echo "     FLURM Federation Test"
echo "=============================================="
echo ""

# Default controller endpoint
CONTROLLER=${CONTROLLER:-"localhost:9100"}
API_BASE="http://${CONTROLLER}/api/v1"

# Helper function for API calls
api_call() {
    local method=$1
    local endpoint=$2
    local data=$3

    if [ -n "$data" ]; then
        curl -s -X "$method" "${API_BASE}${endpoint}" \
            -H "Content-Type: application/json" \
            -d "$data"
    else
        curl -s -X "$method" "${API_BASE}${endpoint}" \
            -H "Content-Type: application/json"
    fi
}

echo "=== Test 1: Check Current Federation Status ==="
echo ""

echo "Checking if federation is enabled..."
RESULT=$(api_call GET "/federation/info" 2>/dev/null || echo '{"error":"not_available"}')
echo "  Response: $RESULT"
echo ""

echo "=== Test 2: List Federated Clusters ==="
echo ""

CLUSTERS=$(api_call GET "/federation/clusters" 2>/dev/null || echo '[]')
echo "  Federated clusters: $CLUSTERS"
echo ""

echo "=== Test 3: Add Test Cluster to Federation ==="
echo ""

# Add a simulated remote cluster
CLUSTER_CONFIG='{
    "name": "remote-cluster-1",
    "host": "remote-ctrl.example.com",
    "port": 6817,
    "auth": {"type": "token", "token": "test-token"},
    "weight": 1,
    "features": ["gpu", "highspeed"],
    "partitions": ["gpu", "batch"]
}'

echo "Adding cluster: remote-cluster-1"
RESULT=$(api_call POST "/federation/clusters" "$CLUSTER_CONFIG" 2>/dev/null || echo '{"error":"not_available"}')
echo "  Response: $RESULT"
echo ""

echo "=== Test 4: Query Cluster Status ==="
echo ""

RESULT=$(api_call GET "/federation/clusters/remote-cluster-1" 2>/dev/null || echo '{"error":"not_found"}')
echo "  Cluster status: $RESULT"
echo ""

echo "=== Test 5: Get Federation Resources ==="
echo ""

RESOURCES=$(api_call GET "/federation/resources" 2>/dev/null || echo '{}')
echo "  Federation resources: $RESOURCES"
echo ""

echo "=== Test 6: Submit Job with Cluster Routing ==="
echo ""

# Submit a job that should be auto-routed
JOB_SPEC='{
    "name": "federation_test",
    "script": "#!/bin/bash\necho Federation test\nsleep 1",
    "partition": "gpu",
    "features": ["gpu"],
    "options": {
        "route_policy": "least_loaded"
    }
}'

echo "Submitting job with auto-routing..."
RESULT=$(api_call POST "/federation/jobs" "$JOB_SPEC" 2>/dev/null || echo '{"error":"not_available"}')
echo "  Response: $RESULT"
echo ""

echo "=== Test 7: Submit Job to Specific Cluster ==="
echo ""

JOB_SPEC_TARGETED='{
    "name": "targeted_job",
    "script": "#!/bin/bash\necho Running on specific cluster",
    "cluster": "remote-cluster-1"
}'

echo "Submitting targeted job to remote-cluster-1..."
RESULT=$(api_call POST "/federation/jobs" "$JOB_SPEC_TARGETED" 2>/dev/null || echo '{"error":"not_available"}')
echo "  Response: $RESULT"
echo ""

echo "=== Test 8: Get Federation Jobs ==="
echo ""

JOBS=$(api_call GET "/federation/jobs" 2>/dev/null || echo '[]')
echo "  Federation jobs: $JOBS"
echo ""

echo "=== Test 9: Test Routing Policies ==="
echo ""

for POLICY in round_robin least_loaded weighted random partition_affinity; do
    JOB_SPEC="{
        \"name\": \"policy_test_${POLICY}\",
        \"script\": \"#!/bin/bash\\necho Testing $POLICY\",
        \"options\": {\"route_policy\": \"$POLICY\"}
    }"

    RESULT=$(api_call POST "/federation/jobs/route" "$JOB_SPEC" 2>/dev/null || echo '{}')
    echo "  $POLICY: $RESULT"
done
echo ""

echo "=== Test 10: Remove Test Cluster ==="
echo ""

RESULT=$(api_call DELETE "/federation/clusters/remote-cluster-1" 2>/dev/null || echo '{"error":"not_available"}')
echo "  Remove result: $RESULT"
echo ""

# Verify cluster list
CLUSTERS=$(api_call GET "/federation/clusters" 2>/dev/null || echo '[]')
echo "  Remaining clusters: $CLUSTERS"
echo ""

echo "=============================================="
echo "     Federation Test Complete"
echo "=============================================="
echo ""
echo "NOTE: Some tests may show 'not_available' if federation"
echo "API is not exposed via HTTP. Tests exercise the API shape."
