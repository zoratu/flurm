# Makefile for FLURM TLA+ Model Checking
#
# Usage:
#   make check           - Run all model checks
#   make check-federation - Run FlurmFederation spec
#   make check-accounting - Run FlurmAccounting spec
#   make check-migration  - Run FlurmMigration spec
#   make check-consensus  - Run FlurmConsensus spec
#   make check-failover   - Run FlurmFailover spec
#   make check-lifecycle  - Run FlurmJobLifecycle spec
#   make check-scheduler  - Run FlurmScheduler spec
#   make clean           - Remove generated files
#   make download        - Download tla2tools.jar
#   make help            - Show this help

.PHONY: all check clean download help
.PHONY: check-federation check-accounting check-migration check-consensus
.PHONY: check-failover check-lifecycle check-scheduler
.PHONY: check-quick check-parallel check-json

# Configuration
TLA2TOOLS := tla2tools.jar
TLA2TOOLS_URL := https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
JAVA := java
JAVA_OPTS := -XX:+UseParallelGC -Xmx4g
TLC := tlc2.TLC
WORKERS := auto
STATES_DIR := states

# All specs
SPECS := FlurmFederation FlurmAccounting FlurmMigration FlurmConsensus \
         FlurmFailover FlurmJobLifecycle FlurmScheduler

# Default target
all: check

# Help target
help:
	@echo "FLURM TLA+ Model Checking Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make check            - Run all model checks"
	@echo "  make check-federation - Run FlurmFederation spec"
	@echo "  make check-accounting - Run FlurmAccounting spec"
	@echo "  make check-migration  - Run FlurmMigration spec"
	@echo "  make check-consensus  - Run FlurmConsensus spec"
	@echo "  make check-failover   - Run FlurmFailover spec"
	@echo "  make check-lifecycle  - Run FlurmJobLifecycle spec"
	@echo "  make check-scheduler  - Run FlurmScheduler spec"
	@echo "  make check-quick      - Quick check with reduced bounds"
	@echo "  make check-parallel   - Run checks in parallel"
	@echo "  make check-json       - Run all checks with JSON output"
	@echo "  make clean            - Remove generated files"
	@echo "  make download         - Download tla2tools.jar"
	@echo "  make help             - Show this help"
	@echo ""
	@echo "Configuration:"
	@echo "  WORKERS=$(WORKERS)    - Number of TLC worker threads"
	@echo "  JAVA_OPTS=$(JAVA_OPTS)"
	@echo ""
	@echo "Examples:"
	@echo "  make check WORKERS=8              # Use 8 workers"
	@echo "  make check-federation WORKERS=4  # Specific spec with 4 workers"

# Download tla2tools.jar
download: $(TLA2TOOLS)

$(TLA2TOOLS):
	@echo "Downloading tla2tools.jar..."
	@curl -L -o $@ $(TLA2TOOLS_URL)
	@echo "Downloaded tla2tools.jar"

# Create states directory
$(STATES_DIR):
	@mkdir -p $(STATES_DIR)

# Run all model checks
check: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh

# Run checks in parallel (separate processes)
check-parallel: $(TLA2TOOLS) $(STATES_DIR)
	@echo "Running all specs in parallel..."
	@$(foreach spec,$(SPECS),./run_tlc.sh --quiet $(spec) &)
	@wait
	@./run_tlc.sh --json --quiet
	@echo "Parallel checks completed. See states/summary.json for results."

# Run with JSON output
check-json: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh --json

# Quick check with limited depth
check-quick: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh --depth 50 --workers 2

# Individual spec targets
check-federation: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmFederation

check-accounting: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmAccounting

check-migration: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmMigration

check-consensus: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmConsensus

check-failover: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmFailover

check-lifecycle: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmJobLifecycle

check-scheduler: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh FlurmScheduler

# Generate DOT state graphs for all specs
graphs: $(TLA2TOOLS) $(STATES_DIR)
	@./run_tlc.sh --dot

# Convert DOT to PNG (requires graphviz)
png: graphs
	@for spec in $(SPECS); do \
		if [ -f $(STATES_DIR)/$$spec_states.dot ]; then \
			dot -Tpng $(STATES_DIR)/$$spec_states.dot -o $(STATES_DIR)/$$spec_states.png; \
		fi; \
	done

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	@rm -rf $(STATES_DIR)/*.txt
	@rm -rf $(STATES_DIR)/*.json
	@rm -rf $(STATES_DIR)/*.dot
	@rm -rf $(STATES_DIR)/*.png
	@rm -rf $(STATES_DIR)/*.dump
	@rm -rf *.toolbox
	@rm -rf metadir
	@rm -rf TE
	@echo "Clean completed"

# Clean everything including tla2tools.jar
distclean: clean
	@rm -f $(TLA2TOOLS)
	@rm -rf $(STATES_DIR)
	@echo "Distribution clean completed"

# Verify Java is available
verify-java:
	@$(JAVA) -version 2>&1 | head -n 1 || (echo "Java not found. Please install Java 11+." && exit 1)

# Run a specific TLA+ file directly (for development)
# Usage: make run SPEC=FlurmFederation
run: $(TLA2TOOLS) $(STATES_DIR) verify-java
	@if [ -z "$(SPEC)" ]; then \
		echo "Usage: make run SPEC=<spec_name>"; \
		exit 1; \
	fi
	$(JAVA) $(JAVA_OPTS) -cp $(TLA2TOOLS) $(TLC) \
		-config $(SPEC).cfg \
		-workers $(WORKERS) \
		-deadlock \
		$(SPEC).tla 2>&1 | tee $(STATES_DIR)/$(SPEC)_output.txt

# Run with liveness checking (slower)
# Usage: make liveness SPEC=FlurmFederation
liveness: $(TLA2TOOLS) $(STATES_DIR) verify-java
	@if [ -z "$(SPEC)" ]; then \
		echo "Usage: make liveness SPEC=<spec_name>"; \
		exit 1; \
	fi
	@echo "Running liveness check for $(SPEC) (this may take a while)..."
	$(JAVA) $(JAVA_OPTS) -cp $(TLA2TOOLS) $(TLC) \
		-config $(SPEC).cfg \
		-workers $(WORKERS) \
		-liveness \
		$(SPEC).tla 2>&1 | tee $(STATES_DIR)/$(SPEC)_liveness.txt

# Print state count estimates
estimate: $(TLA2TOOLS) verify-java
	@echo "State count estimates (may not be accurate):"
	@for spec in $(SPECS); do \
		echo "  $$spec:"; \
		$(JAVA) $(JAVA_OPTS) -cp $(TLA2TOOLS) $(TLC) \
			-config $$spec.cfg \
			-simulate num=1 \
			$$spec.tla 2>&1 | grep -E "(states|diameter)" || true; \
	done

# Simulate random traces (useful for understanding behavior)
# Usage: make simulate SPEC=FlurmFederation NUM=10
simulate: $(TLA2TOOLS) $(STATES_DIR) verify-java
	@if [ -z "$(SPEC)" ]; then \
		echo "Usage: make simulate SPEC=<spec_name> [NUM=10]"; \
		exit 1; \
	fi
	$(JAVA) $(JAVA_OPTS) -cp $(TLA2TOOLS) $(TLC) \
		-config $(SPEC).cfg \
		-simulate num=$(or $(NUM),10) \
		$(SPEC).tla 2>&1 | tee $(STATES_DIR)/$(SPEC)_simulate.txt
