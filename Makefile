# FLURM Makefile - Convenience targets for development

.PHONY: all compile test clean dialyzer xref check release shell \
        proper eunit ct docs chaos-test

REBAR3 ?= rebar3

# Default target
all: compile

# Compile all applications
compile:
	$(REBAR3) compile

# Run all tests
test: eunit proper ct

# Run EUnit tests only
eunit:
	$(REBAR3) eunit --cover

# Run PropEr property-based tests
proper:
	$(REBAR3) proper -n 100

# Run Common Test suites
ct:
	$(REBAR3) ct --cover

# Static analysis with Dialyzer
dialyzer:
	$(REBAR3) dialyzer

# Cross-reference analysis
xref:
	$(REBAR3) xref

# Run all checks (compile, test, dialyzer, xref)
check: compile test dialyzer xref
	@echo "All checks passed!"

# Generate coverage report
cover:
	$(REBAR3) cover --verbose

# Clean build artifacts
clean:
	$(REBAR3) clean
	rm -rf _build/test

# Deep clean including deps
distclean:
	rm -rf _build deps

# Generate documentation
docs:
	$(REBAR3) edoc

# Build production release
release:
	$(REBAR3) as prod release

# Build release tarball
tar:
	$(REBAR3) as prod tar

# Start interactive shell with all apps
shell:
	$(REBAR3) shell

# Start controller shell
shell-ctrl:
	$(REBAR3) shell --apps flurm_controller

# Start node daemon shell
shell-node:
	$(REBAR3) shell --apps flurm_node_daemon

# Run chaos tests (be careful!)
chaos-test:
	@echo "WARNING: Running chaos tests. This will inject failures!"
	@echo "Press Ctrl+C within 5 seconds to abort..."
	@sleep 5
	$(REBAR3) shell --eval "flurm_chaos:start_link(), flurm_chaos:enable()."

# Format code (requires rebar3_format plugin)
fmt:
	$(REBAR3) fmt

# Check formatting
fmt-check:
	$(REBAR3) fmt --check

# Upgrade dependencies
upgrade-deps:
	$(REBAR3) upgrade

# Run TLA+ model checker (requires TLC)
tla-check:
	@echo "Running TLA+ model checker..."
	@cd specs/tla && tlc FlurmScheduler.tla || echo "TLC not installed or spec needs config"

# Help target
help:
	@echo "FLURM Build Targets:"
	@echo ""
	@echo "  make              - Compile all applications"
	@echo "  make test         - Run all tests (eunit, proper, ct)"
	@echo "  make eunit        - Run EUnit tests"
	@echo "  make proper       - Run PropEr property-based tests"
	@echo "  make ct           - Run Common Test suites"
	@echo "  make dialyzer     - Run Dialyzer static analysis"
	@echo "  make xref         - Run cross-reference analysis"
	@echo "  make check        - Run all checks (compile, test, dialyzer, xref)"
	@echo "  make cover        - Generate coverage report"
	@echo "  make docs         - Generate EDoc documentation"
	@echo "  make release      - Build production release"
	@echo "  make shell        - Start interactive shell"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make chaos-test   - Run chaos engineering tests (WARNING!)"
	@echo "  make tla-check    - Run TLA+ model checker"
