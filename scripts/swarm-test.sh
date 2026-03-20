#!/usr/bin/env bash
#
# Swarm Testing for FLURM
# =======================
# Implements the swarm testing methodology from Groce et al. (ISSTA 2012)
# "Swarm Testing" - using diverse test configurations with feature omission
# to improve coverage and fault detection.
#
# Instead of running all 551 test modules in every test run, each "swarm
# configuration" randomly includes/excludes modules with ~50% probability.
# Multiple configurations run in parallel on spot instances.
#
# Key insight: Feature omission leads to better state space exploration.
# Some test modules actively suppress behaviors (e.g., meck mocks that
# override real module behavior), and features compete for EUnit's
# parallel execution resources. By omitting some, others explore deeper.
#
# Usage:
#   ./scripts/swarm-test.sh generate N        # Generate N swarm configs
#   ./scripts/swarm-test.sh run-local N        # Run N configs locally (sequential)
#   ./scripts/swarm-test.sh run-spot N [INST]  # Run N configs on spot instances
#   ./scripts/swarm-test.sh merge DIR          # Merge coverage from swarm run
#   ./scripts/swarm-test.sh report DIR         # Show swarm coverage report
#   ./scripts/swarm-test.sh status DIR         # Check running swarm jobs
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SWARM_DIR="${PROJECT_DIR}/_build/swarm"
SPOT_PROFILE="weka-pm"

# Swarm configuration parameters (from Groce et al.)
FEATURE_INCLUSION_PROB=0.50  # Coin-toss probability (50% = maximum diversity)
MIN_FEATURES=10              # Minimum modules per config (avoid trivial runs)

# ============================================================================
# Feature Discovery
# ============================================================================

discover_features() {
    # Features = test modules that can be independently enabled/disabled
    # Each module is a "feature" in the swarm testing sense
    find "$PROJECT_DIR/apps" -name "*_tests.erl" -type f \
        | sed 's|.*/||;s|\.erl||' \
        | sort
}

discover_features_by_category() {
    # Group features by category for structured omission
    local all_modules
    all_modules=$(discover_features)

    echo "=== Feature Categories ==="
    for category in pure direct coverage 100cov ifdef meck callback real; do
        local count
        count=$(echo "$all_modules" | grep -c "_${category}_" 2>/dev/null || echo 0)
        echo "  ${category}: ${count} modules"
    done
    echo "  total: $(echo "$all_modules" | wc -l | tr -d ' ') modules"
}

# ============================================================================
# Swarm Configuration Generation
# ============================================================================

generate_config() {
    # Generate a single swarm configuration C_i
    # Each module is included with probability FEATURE_INCLUSION_PROB
    local config_id=$1
    local config_file="${SWARM_DIR}/configs/config_${config_id}.txt"

    local all_modules
    all_modules=$(discover_features)
    local total
    total=$(echo "$all_modules" | wc -l | tr -d ' ')

    mkdir -p "$(dirname "$config_file")"

    local included=0
    local excluded=0
    > "$config_file"

    while IFS= read -r module; do
        # Coin toss: include with FEATURE_INCLUSION_PROB probability
        local rand
        rand=$(( RANDOM % 100 ))
        if [ "$rand" -lt "$(echo "$FEATURE_INCLUSION_PROB * 100" | bc | cut -d. -f1)" ]; then
            echo "$module" >> "$config_file"
            included=$((included + 1))
        else
            excluded=$((excluded + 1))
        fi
    done <<< "$all_modules"

    # Ensure minimum features (paper: avoid trivially small configs)
    if [ "$included" -lt "$MIN_FEATURES" ]; then
        # Add random modules until we hit minimum
        local remaining
        remaining=$(comm -23 <(echo "$all_modules") <(sort "$config_file") | shuf | head -n $((MIN_FEATURES - included)))
        echo "$remaining" >> "$config_file"
        included=$(wc -l < "$config_file" | tr -d ' ')
    fi

    echo "  Config ${config_id}: ${included}/${total} modules (${excluded} omitted)"
}

generate_configs() {
    local n=${1:-10}
    echo "Generating ${n} swarm configurations..."
    echo "  Inclusion probability: ${FEATURE_INCLUSION_PROB}"
    echo "  Min features per config: ${MIN_FEATURES}"
    echo ""

    mkdir -p "${SWARM_DIR}/configs"

    for i in $(seq 1 "$n"); do
        generate_config "$i"
    done

    echo ""
    echo "Configs written to ${SWARM_DIR}/configs/"

    # Also generate C_D (default = all features) for comparison
    discover_features > "${SWARM_DIR}/configs/config_default.txt"
    local total
    total=$(wc -l < "${SWARM_DIR}/configs/config_default.txt" | tr -d ' ')
    echo "  Default (C_D): ${total}/${total} modules"
}

# ============================================================================
# Local Execution
# ============================================================================

run_config_local() {
    local config_id=$1
    local config_file="${SWARM_DIR}/configs/config_${config_id}.txt"
    local result_dir="${SWARM_DIR}/results/${config_id}"

    if [ ! -f "$config_file" ]; then
        echo "ERROR: Config file not found: ${config_file}"
        return 1
    fi

    mkdir -p "$result_dir"

    # Build module list for rebar3 eunit --module=
    local modules
    modules=$(tr '\n' ',' < "$config_file" | sed 's/,$//')
    local module_count
    module_count=$(wc -l < "$config_file" | tr -d ' ')

    echo "[Config ${config_id}] Running ${module_count} modules..."
    local start_time
    start_time=$(date +%s)

    # Run tests with coverage
    cd "$PROJECT_DIR"
    rm -rf _build/test/cover

    if rebar3 eunit --module="$modules" --cover > "$result_dir/test_output.txt" 2>&1; then
        echo "[Config ${config_id}] Tests passed"
    else
        echo "[Config ${config_id}] Some tests failed (see ${result_dir}/test_output.txt)"
    fi

    # Capture coverage data
    if [ -f "_build/test/cover/eunit.coverdata" ]; then
        cp "_build/test/cover/eunit.coverdata" "$result_dir/eunit.coverdata"
    fi

    # Generate coverage report
    rebar3 cover --verbose > "$result_dir/coverage.txt" 2>&1 || true

    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))

    # Extract summary stats
    local tests failures cancelled coverage_pct
    tests=$(grep -oP '\d+ tests' "$result_dir/test_output.txt" | head -1 | grep -oP '\d+' || echo "?")
    failures=$(grep -oP '\d+ failures' "$result_dir/test_output.txt" | head -1 | grep -oP '\d+' || echo "?")
    cancelled=$(grep -oP '\d+ cancelled' "$result_dir/test_output.txt" | head -1 | grep -oP '\d+' || echo "0")
    coverage_pct=$(grep 'total' "$result_dir/coverage.txt" | grep -oP '\d+%' | head -1 || echo "?")

    cat > "$result_dir/summary.json" <<ENDJSON
{
  "config_id": "${config_id}",
  "modules": ${module_count},
  "tests": "${tests}",
  "failures": "${failures}",
  "cancelled": "${cancelled}",
  "coverage": "${coverage_pct}",
  "duration_seconds": ${duration},
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
ENDJSON

    echo "[Config ${config_id}] Done: ${tests} tests, ${failures} failures, ${coverage_pct} coverage (${duration}s)"
}

run_local() {
    local n=${1:-5}
    echo "=== FLURM Swarm Testing (Local) ==="
    echo "Running ${n} swarm configurations sequentially..."
    echo ""

    # Generate configs if they don't exist
    if [ ! -d "${SWARM_DIR}/configs" ] || [ "$(ls "${SWARM_DIR}/configs/config_[0-9]*.txt" 2>/dev/null | wc -l)" -lt "$n" ]; then
        generate_configs "$n"
        echo ""
    fi

    mkdir -p "${SWARM_DIR}/results"

    for i in $(seq 1 "$n"); do
        run_config_local "$i"
    done

    echo ""
    echo "=== Swarm Run Complete ==="
    merge_results "${SWARM_DIR}/results"
}

# ============================================================================
# Spot Instance Execution
# ============================================================================

spot_setup_script() {
    # Generate the setup script to run on spot instances
    # Installs OTP 28, rebar3, clones repo, runs tests
    local config_id=$1
    local config_file="${SWARM_DIR}/configs/config_${config_id}.txt"
    local modules
    modules=$(tr '\n' ',' < "$config_file" | sed 's/,$//')

    cat <<'SETUP_SCRIPT'
#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "=== Swarm Test Worker Setup ==="
echo "Config: CONFIG_ID_PLACEHOLDER"
echo "Host: $(hostname)"
echo "Arch: $(uname -m)"
date -u

# Install Erlang/OTP 28 from Erlang Solutions
echo "=== Installing Erlang/OTP 28 ==="
if ! erl -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().' -noshell 2>/dev/null | grep -q "^28$"; then
    # Add Erlang Solutions repo
    wget -q https://binaries2.erlang-solutions.com/ubuntu/pool/contrib/e/esl-erlang/esl-erlang_28.0.1-1~ubuntu~noble_arm64.deb -O /tmp/esl-erlang.deb 2>/dev/null \
    || wget -q https://binaries2.erlang-solutions.com/ubuntu/pool/contrib/e/esl-erlang/esl-erlang_28.0.1-1~ubuntu~noble_amd64.deb -O /tmp/esl-erlang.deb 2>/dev/null \
    || {
        # Fallback: use kerl to build from source
        echo "Downloading kerl..."
        curl -s -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
        chmod +x kerl
        ./kerl build 28.0.1 28.0.1
        ./kerl install 28.0.1 ~/erlang/28.0.1
        . ~/erlang/28.0.1/activate
    }
    if [ -f /tmp/esl-erlang.deb ]; then
        sudo dpkg -i /tmp/esl-erlang.deb 2>/dev/null || sudo apt-get install -f -y
    fi
fi

echo "Erlang version: $(erl -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().' -noshell)"

# Install rebar3
echo "=== Installing rebar3 ==="
if ! which rebar3 >/dev/null 2>&1; then
    wget -q https://s3.amazonaws.com/rebar3/rebar3 -O /usr/local/bin/rebar3
    chmod +x /usr/local/bin/rebar3
fi

# Clone project
echo "=== Cloning FLURM ==="
WORK_DIR="/tmp/flurm-swarm"
rm -rf "$WORK_DIR"
SETUP_SCRIPT

    # The actual modules and config ID get injected here
    cat <<SETUP_SCRIPT2

# Clone from the local machine (via tailscale) or git
cd /tmp
git clone REPO_URL_PLACEHOLDER "\$WORK_DIR" 2>/dev/null || {
    echo "Clone failed, trying alternative..."
    exit 1
}
cd "\$WORK_DIR"

echo "=== Running Swarm Tests ==="
echo "Config ${config_id}: $(wc -l < "$config_file" | tr -d ' ') modules"

MODULES="${modules}"

rm -rf _build/test/cover
START_TIME=\$(date +%s)

rebar3 eunit --module="\$MODULES" --cover > /tmp/swarm_test_output.txt 2>&1 || true
rebar3 cover --verbose > /tmp/swarm_coverage.txt 2>&1 || true

END_TIME=\$(date +%s)
DURATION=\$((END_TIME - START_TIME))

echo "=== Results ==="
tail -5 /tmp/swarm_test_output.txt
echo ""
grep "total" /tmp/swarm_coverage.txt || echo "No coverage data"
echo "Duration: \${DURATION}s"

# Package results
RESULT_DIR="/tmp/swarm_results_${config_id}"
mkdir -p "\$RESULT_DIR"
cp /tmp/swarm_test_output.txt "\$RESULT_DIR/"
cp /tmp/swarm_coverage.txt "\$RESULT_DIR/"
[ -f _build/test/cover/eunit.coverdata ] && cp _build/test/cover/eunit.coverdata "\$RESULT_DIR/"

echo "SWARM_DONE config=${config_id} duration=\${DURATION}"
SETUP_SCRIPT2
}

run_spot() {
    local n=${1:-10}
    local instance_id=${2:-""}
    local run_id
    run_id="swarm_$(date +%Y%m%d_%H%M%S)"
    local run_dir="${SWARM_DIR}/runs/${run_id}"

    echo "=== FLURM Swarm Testing on Spot Instances ==="
    echo "Run ID: ${run_id}"
    echo "Configurations: ${n}"
    echo ""

    # Generate configs
    generate_configs "$n"
    echo ""

    mkdir -p "$run_dir"

    # Find available spot instances via tailscale
    echo "Finding spot instances..."
    local instances
    if [ -n "$instance_id" ]; then
        instances="$instance_id"
    else
        instances=$(tailscale status 2>/dev/null | grep "zoratu@" | grep "linux" | grep -v "offline" | awk '{print $1}' | head -n "$n")
    fi

    local instance_count
    instance_count=$(echo "$instances" | wc -l | tr -d ' ')
    echo "Available instances: ${instance_count}"

    if [ "$instance_count" -eq 0 ]; then
        echo "ERROR: No spot instances available. Launch some with: spot -P weka-pm l <N>"
        exit 1
    fi

    # Create the remote test script that gets pushed to instances
    local repo_tar="${run_dir}/flurm-src.tar.gz"
    echo "Packaging source tree..."
    cd "$PROJECT_DIR"
    # Package only the source (exclude _build, .git)
    tar czf "$repo_tar" \
        --exclude='_build' \
        --exclude='.git' \
        --exclude='_build' \
        --exclude='*.beam' \
        --exclude='docker' \
        -C "$(dirname "$PROJECT_DIR")" \
        "$(basename "$PROJECT_DIR")" 2>/dev/null

    echo "Source package: $(du -h "$repo_tar" | cut -f1)"
    echo ""

    # Distribute configs across available instances (round-robin)
    local instance_array
    readarray -t instance_array <<< "$instances"
    local num_instances=${#instance_array[@]}

    for i in $(seq 1 "$n"); do
        local inst_idx=$(( (i - 1) % num_instances ))
        local inst_ip="${instance_array[$inst_idx]}"
        local config_file="${SWARM_DIR}/configs/config_${i}.txt"
        local modules
        modules=$(tr '\n' ',' < "$config_file" | sed 's/,$//')
        local module_count
        module_count=$(wc -l < "$config_file" | tr -d ' ')

        echo "[Config ${i}] -> ${inst_ip} (${module_count} modules)"

        # Create remote execution script
        cat > "${run_dir}/run_${i}.sh" <<REMOTE_SCRIPT
#!/usr/bin/env bash
set -euo pipefail
echo "=== Swarm Config ${i} on \$(hostname) ==="
date -u

WORK_DIR="/tmp/flurm-swarm-${i}"
rm -rf "\$WORK_DIR"
mkdir -p "\$WORK_DIR"

# Extract source
cd /tmp
tar xzf /tmp/flurm-src.tar.gz
mv /tmp/flurm "\$WORK_DIR/flurm" 2>/dev/null || cp -r /tmp/flurm "\$WORK_DIR/flurm"
cd "\$WORK_DIR/flurm"

# Build
echo "Building..."
rebar3 compile 2>&1 | tail -3

# Run swarm config
echo "Running ${module_count} test modules..."
MODULES="${modules}"
rm -rf _build/test/cover
START=\$(date +%s)

rebar3 eunit --module="\$MODULES" --cover > /tmp/swarm_out_${i}.txt 2>&1 || true
rebar3 cover --verbose > /tmp/swarm_cov_${i}.txt 2>&1 || true

END=\$(date +%s)
DUR=\$((END - START))

# Results
TESTS=\$(grep -oP '\d+ tests' /tmp/swarm_out_${i}.txt | head -1 || echo "0 tests")
FAILS=\$(grep -oP '\d+ failures' /tmp/swarm_out_${i}.txt | head -1 || echo "0 failures")
COV=\$(grep 'total' /tmp/swarm_cov_${i}.txt | grep -oP '\d+%' | head -1 || echo "?%")

echo "SWARM_RESULT config=${i} \${TESTS} \${FAILS} coverage=\${COV} duration=\${DUR}s"

# Save coverdata for retrieval
mkdir -p /tmp/swarm_results
cp _build/test/cover/eunit.coverdata /tmp/swarm_results/cover_${i}.coverdata 2>/dev/null || true
cp /tmp/swarm_cov_${i}.txt /tmp/swarm_results/coverage_${i}.txt 2>/dev/null || true
REMOTE_SCRIPT

        chmod +x "${run_dir}/run_${i}.sh"

        # Launch on spot instance (background)
        {
            # Copy source tarball and run script
            scp -o ConnectTimeout=10 -o StrictHostKeyChecking=no -q \
                "$repo_tar" "ubuntu@${inst_ip}:/tmp/flurm-src.tar.gz" 2>/dev/null
            scp -o ConnectTimeout=10 -o StrictHostKeyChecking=no -q \
                "${run_dir}/run_${i}.sh" "ubuntu@${inst_ip}:/tmp/run_swarm_${i}.sh" 2>/dev/null
            ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "ubuntu@${inst_ip}" "bash /tmp/run_swarm_${i}.sh" \
                > "${run_dir}/output_${i}.txt" 2>&1

            echo "[Config ${i}] COMPLETED on ${inst_ip}"
        } &

        echo "  Launched in background (PID: $!)"
    done

    echo ""
    echo "All ${n} configs launched. Monitor with:"
    echo "  $0 status ${run_dir}"
    echo ""
    echo "Retrieve results with:"
    echo "  $0 merge ${run_dir}"

    # Save run metadata
    cat > "${run_dir}/metadata.json" <<META
{
  "run_id": "${run_id}",
  "num_configs": ${n},
  "num_instances": ${num_instances},
  "feature_inclusion_prob": ${FEATURE_INCLUSION_PROB},
  "min_features": ${MIN_FEATURES},
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
META

    # Wait for all background jobs
    echo "Waiting for all swarm jobs to complete..."
    wait

    echo ""
    echo "=== All Swarm Jobs Complete ==="
    show_status "$run_dir"
}

# ============================================================================
# Results Merging & Reporting
# ============================================================================

merge_results() {
    local result_dir=$1
    echo "=== Swarm Results Summary ==="
    echo ""

    local total_configs=0
    local total_tests=0
    local total_failures=0

    for output in "${result_dir}"/output_*.txt "${result_dir}"/*/summary.json; do
        [ -f "$output" ] || continue
        total_configs=$((total_configs + 1))

        if [[ "$output" == *.json ]]; then
            local t f c
            t=$(grep -oP '"tests": "\K[^"]+' "$output" 2>/dev/null || echo "0")
            f=$(grep -oP '"failures": "\K[^"]+' "$output" 2>/dev/null || echo "0")
            c=$(grep -oP '"coverage": "\K[^"]+' "$output" 2>/dev/null || echo "?")
            echo "  $(basename "$(dirname "$output")"): ${t} tests, ${f} failures, ${c} coverage"
        elif [[ "$output" == *.txt ]]; then
            local result_line
            result_line=$(grep "SWARM_RESULT" "$output" 2>/dev/null || echo "")
            if [ -n "$result_line" ]; then
                echo "  $(basename "$output" .txt): $result_line"
            fi
        fi
    done

    echo ""
    echo "Total configurations: ${total_configs}"
}

show_status() {
    local run_dir=$1
    echo "=== Swarm Job Status ==="
    for output in "${run_dir}"/output_*.txt; do
        [ -f "$output" ] || continue
        local config
        config=$(basename "$output" .txt | sed 's/output_//')
        if grep -q "SWARM_RESULT\|SWARM_DONE" "$output" 2>/dev/null; then
            local result
            result=$(grep "SWARM_RESULT\|SWARM_DONE" "$output" | tail -1)
            echo "  [Config ${config}] DONE: $result"
        elif [ -s "$output" ]; then
            echo "  [Config ${config}] RUNNING ($(wc -l < "$output") lines output)"
        else
            echo "  [Config ${config}] PENDING"
        fi
    done
}

report() {
    local run_dir=$1
    echo "=== Swarm Testing Report ==="
    echo ""
    echo "Methodology: Groce et al. 'Swarm Testing' (ISSTA 2012)"
    echo "  - Each configuration randomly omits ~50% of test modules"
    echo "  - Feature omission diversity improves coverage & fault detection"
    echo "  - Active suppression: some modules prevent bugs from being found"
    echo "  - Passive suppression: modules compete for parallel execution resources"
    echo ""

    merge_results "$run_dir"

    echo ""
    echo "=== Feature Significance Analysis ==="
    echo "(Which modules, when omitted, led to new coverage or failures)"
    echo ""

    # Analyze which modules appeared in configs that found unique coverage
    for cov_file in "${run_dir}"/*/coverage.txt "${run_dir}"/coverage_*.txt; do
        [ -f "$cov_file" ] || continue
        local config_name
        config_name=$(basename "$(dirname "$cov_file")")
        [ "$config_name" = "$(basename "$run_dir")" ] && config_name=$(basename "$cov_file" .txt)

        # Extract modules that got > 0% coverage in this config
        local covered
        covered=$(grep -E '^\s+\|.*\|.*[1-9][0-9]*%' "$cov_file" 2>/dev/null | wc -l | tr -d ' ')
        local total_cov
        total_cov=$(grep 'total' "$cov_file" 2>/dev/null | grep -oP '\d+%' | head -1 || echo "?%")
        echo "  ${config_name}: ${covered} modules with coverage, total ${total_cov}"
    done
}

# ============================================================================
# Main
# ============================================================================

case "${1:-help}" in
    generate)
        generate_configs "${2:-10}"
        ;;
    categories)
        discover_features_by_category
        ;;
    run-local)
        run_local "${2:-5}"
        ;;
    run-spot)
        run_spot "${2:-10}" "${3:-}"
        ;;
    merge)
        merge_results "${2:-${SWARM_DIR}/results}"
        ;;
    status)
        show_status "${2:-${SWARM_DIR}/runs/$(ls -t "${SWARM_DIR}/runs" 2>/dev/null | head -1)}"
        ;;
    report)
        report "${2:-${SWARM_DIR}/runs/$(ls -t "${SWARM_DIR}/runs" 2>/dev/null | head -1)}"
        ;;
    help|*)
        echo "FLURM Swarm Testing (Groce et al. ISSTA 2012)"
        echo ""
        echo "Usage: $0 <command> [args]"
        echo ""
        echo "Commands:"
        echo "  generate N       Generate N random swarm configurations"
        echo "  categories       Show test feature categories"
        echo "  run-local N      Run N configs locally (sequential)"
        echo "  run-spot N [IP]  Run N configs on spot instances"
        echo "  merge DIR        Merge coverage results"
        echo "  status DIR       Check swarm job status"
        echo "  report DIR       Generate swarm testing report"
        echo ""
        echo "Swarm Parameters:"
        echo "  Inclusion probability: ${FEATURE_INCLUSION_PROB} (coin-toss)"
        echo "  Min features/config:   ${MIN_FEATURES}"
        echo ""
        echo "Example:"
        echo "  $0 generate 20           # Generate 20 diverse configs"
        echo "  $0 run-spot 20           # Run all 20 on spot instances"
        echo "  $0 report                # See results"
        ;;
esac
