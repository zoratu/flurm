#!/bin/bash
#
# Run FLURM CLI tests in Docker environment
#
# Usage: ./run-cli-tests.sh [--rebuild] [--skip-srun]
#
# Options:
#   --rebuild   Rebuild Docker images before testing
#   --skip-srun Skip srun tests (known protocol issue)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_DIR/docker"

REBUILD=false
SKIP_SRUN_FLAG=""

for arg in "$@"; do
    case $arg in
        --rebuild)
            REBUILD=true
            shift
            ;;
        --skip-srun)
            SKIP_SRUN_FLAG="--skip-srun"
            shift
            ;;
    esac
done

echo "FLURM CLI Test Runner"
echo "====================="
echo ""

# Change to docker directory
cd "$DOCKER_DIR"

# Optionally rebuild
if [ "$REBUILD" = true ]; then
    echo "Rebuilding Docker images..."
    docker-compose build --no-cache flurm-server
    echo ""
fi

# Ensure containers are running
echo "Starting Docker containers..."
docker-compose up -d flurm-server flurm-node slurm-client

# Wait for server to be ready
echo "Waiting for FLURM server to be ready..."
sleep 5

# Check if server is responding
MAX_RETRIES=10
RETRY=0
while [ $RETRY -lt $MAX_RETRIES ]; do
    if docker exec docker-slurm-client-1 squeue >/dev/null 2>&1; then
        echo "Server is ready!"
        break
    fi
    echo "  Waiting... (attempt $((RETRY+1))/$MAX_RETRIES)"
    sleep 2
    ((RETRY++))
done

if [ $RETRY -eq $MAX_RETRIES ]; then
    echo "ERROR: Server did not become ready in time"
    exit 1
fi

echo ""

# Copy test script to container and run it
echo "Running CLI tests..."
echo ""
docker cp "$SCRIPT_DIR/test-cli.sh" docker-slurm-client-1:/tmp/test-cli.sh
docker exec docker-slurm-client-1 chmod +x /tmp/test-cli.sh
docker exec docker-slurm-client-1 /tmp/test-cli.sh $SKIP_SRUN_FLAG

EXIT_CODE=$?

echo ""
echo "Test run complete."

exit $EXIT_CODE
