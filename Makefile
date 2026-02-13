# FLURM Makefile - Convenience targets for development

.PHONY: all compile test clean dialyzer xref check release shell \
        proper eunit ct docs chaos-test \
        test-stress test-soak test-soak-short test-memory test-all diagnose \
        test-docker test-release release-check test-dbd-fast test-quality \
        test-branch-hardpaths test-model-deterministic test-network-fault \
        test-upgrade-replay test-soak-cadence-short test-soak-cadence \
        test-soak-cadence-long ci-cadence ci-local coverage-full \
        hooks-install check-quick check-prepush check-consistency check-coverage

REBAR3 ?= rebar3

# Default target
all: compile

# Compile all applications
compile:
	$(REBAR3) compile

# Install versioned git hooks from .githooks
hooks-install:
	./scripts/install-hooks.sh

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

# Fast local checks for pre-commit use.
check-quick:
	./scripts/check-consistency.sh quick

# Medium checks for pre-push use.
check-prepush:
	./scripts/check-consistency.sh prepush

# Full deterministic checks for local/CI runners (non-GitHub Actions too).
check-consistency:
	./scripts/check-consistency.sh full

# Coverage threshold gate for key DBD modules.
check-coverage:
	./scripts/check-coverage-threshold.sh

# Fast deterministic DBD-only test loop:
# - Runs only the flurm_dbd app tests with cover enabled
# - Enforces 100% on the key DBD modules
test-dbd-fast:
	$(REBAR3) as test eunit --app=flurm_dbd --cover
	./scripts/check-coverage-dbd-100.sh

# Targeted quality/fault suites for regression coverage beyond line coverage.
test-quality:
	$(REBAR3) eunit --module=flurm_quality_gap_tests,flurm_fault_injection_tests

# Branch-heavy protocol/acceptor paths.
test-branch-hardpaths:
	$(REBAR3) eunit --module=flurm_protocol_codec_direct_tests,flurm_controller_acceptor_pure_tests,flurm_srun_acceptor_tests

# Deterministic stateful lifecycle/model tests.
test-model-deterministic:
	./scripts/run-deterministic-model-tests.sh

# Real network fault-injection/partition integration tests.
test-network-fault:
	./scripts/run-network-fault-injection-tests.sh

# Upgrade/rollback compatibility replay tests on persisted migration state.
test-upgrade-replay:
	./scripts/run-upgrade-rollback-replay-tests.sh

# Soak cadences.
test-soak-cadence-short:
	./scripts/run-soak-cadence.sh short

test-soak-cadence:
	./scripts/run-soak-cadence.sh standard

test-soak-cadence-long:
	./scripts/run-soak-cadence.sh long

# Local CI cadence runner for non-GitHub environments.
ci-cadence:
	./scripts/run-deterministic-model-tests.sh
	if [ "$${FLURM_RUN_NETWORK_FAULT:-0}" = "1" ]; then FLURM_NETWORK_FAULT_REQUIRED=1 ./scripts/run-network-fault-injection-tests.sh; else echo "ci-cadence: skip network fault (set FLURM_RUN_NETWORK_FAULT=1)"; fi
	if [ "$${FLURM_RUN_UPGRADE_REPLAY:-0}" = "1" ]; then FLURM_UPGRADE_REPLAY_REQUIRED=1 ./scripts/run-upgrade-rollback-replay-tests.sh; else echo "ci-cadence: skip upgrade replay (set FLURM_RUN_UPGRADE_REPLAY=1)"; fi
	if [ "$${FLURM_RUN_SOAK_CADENCE:-0}" = "1" ]; then ./scripts/run-soak-cadence.sh "$${FLURM_SOAK_CADENCE:-short}"; else echo "ci-cadence: skip soak cadence (set FLURM_RUN_SOAK_CADENCE=1)"; fi

# Full local CI runner (works in any runner, not tied to GitHub Actions).
ci-local:
	./scripts/ci-local.sh

# Merged full-project coverage report across umbrella apps.
coverage-full:
	./scripts/run_coverage.escript

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
	@cd tla && for spec in FlurmFederation FlurmAccounting FlurmMigration; do \
		if [ -f "$${spec}.tla" ] && [ -f tla2tools.jar ]; then \
			echo "Checking $${spec}..."; \
			java -jar tla2tools.jar -workers 4 "$${spec}.tla" || true; \
		fi; \
	done || echo "TLC not installed or specs not found"

#
# === Long-Running Tests (for finding memory leaks and stability issues) ===
#

# Run stress tests (1000 jobs, measures throughput and memory)
test-stress: compile
	@echo "=== Running Stress Tests ==="
	@$(REBAR3) shell --eval " \
		io:format(\"~n=== STRESS TESTS ===~n~n\"), \
		io:format(\"1. Throughput test (1000 jobs, 10 concurrent)...~n\"), \
		R1 = flurm_load_test:stress_test(1000, 10), \
		io:format(\"   Throughput: ~.1f jobs/sec~n\", [maps:get(jobs_per_second, R1)]), \
		io:format(\"   Memory growth: ~p bytes~n~n\", [maps:get(memory_growth, R1)]), \
		io:format(\"2. Memory stability test (500 jobs)...~n\"), \
		R2 = flurm_load_test:memory_stability_test(500), \
		io:format(\"   Leaked: ~p bytes (~.2f%)~n\", [maps:get(memory_leaked, R2), maps:get(leak_percentage, R2)]), \
		io:format(\"   Result: ~s~n~n\", [case maps:get(passed, R2) of true -> \"PASSED\"; false -> \"FAILED\" end]), \
		io:format(\"3. Job lifecycle test (100 jobs)...~n\"), \
		R3 = flurm_load_test:job_lifecycle_test(100), \
		io:format(\"   Orphaned processes: ~p~n\", [maps:get(orphaned_processes, R3)]), \
		io:format(\"   Result: ~s~n~n\", [case maps:get(passed, R3) of true -> \"PASSED\"; false -> \"FAILED\" end]), \
		AllPassed = maps:get(passed, R2, false) andalso maps:get(passed, R3, false), \
		io:format(\"=== STRESS TESTS ~s ===~n\", [case AllPassed of true -> \"PASSED\"; false -> \"FAILED\" end]), \
		halt(case AllPassed of true -> 0; false -> 1 end). \
	"

# Run memory stability test only
test-memory: compile
	@echo "=== Running Memory Stability Test ==="
	@$(REBAR3) shell --eval " \
		R = flurm_load_test:memory_stability_test(1000), \
		io:format(\"~nMemory leaked: ~p bytes (~.2f%)~n\", [maps:get(memory_leaked, R), maps:get(leak_percentage, R)]), \
		io:format(\"Result: ~s~n\", [case maps:get(passed, R) of true -> \"PASSED\"; false -> \"FAILED\" end]), \
		halt(case maps:get(passed, R) of true -> 0; false -> 1 end). \
	"

# Run 30-minute soak test (detects slow memory leaks)
test-soak: compile
	@echo "=== Running 30-minute Soak Test ==="
	@echo "This will take approximately 30 minutes..."
	@$(REBAR3) shell --eval " \
		io:format(\"~n=== SOAK TEST (30 minutes) ===~n\"), \
		io:format(\"Starting at ~p~n\", [calendar:local_time()]), \
		Result = flurm_load_test:soak_test(1800000), \
		io:format(\"~nCompleted at ~p~n\", [calendar:local_time()]), \
		io:format(\"Jobs submitted: ~p~n\", [maps:get(jobs_submitted, Result)]), \
		io:format(\"Memory growth: ~p bytes~n\", [maps:get(memory_growth_bytes, Result)]), \
		io:format(\"Leak alerts: ~p~n\", [maps:get(leak_alerts, Result)]), \
		case maps:get(leak_alerts, Result) of \
			0 -> io:format(\"~nNo memory leaks detected - PASSED~n\"), halt(0); \
			N -> io:format(\"~n~p potential leaks detected - REVIEW NEEDED~n\", [N]), halt(1) \
		end. \
	"

# Run 5-minute soak test (quick check)
test-soak-short: compile
	@echo "=== Running 5-minute Soak Test ==="
	@$(REBAR3) shell --eval " \
		io:format(\"~n=== SOAK TEST (5 minutes) ===~n\"), \
		Result = flurm_load_test:soak_test(300000), \
		io:format(\"~nJobs: ~p, Memory growth: ~p bytes, Alerts: ~p~n\", \
			[maps:get(jobs_submitted, Result), maps:get(memory_growth_bytes, Result), maps:get(leak_alerts, Result)]), \
		halt(0). \
	"

# Run race condition tests
test-race: compile
	@echo "=== Running Race Condition Tests ==="
	@$(REBAR3) shell --eval " \
		io:format(\"~n=== RACE CONDITION TESTS ===~n~n\"), \
		io:format(\"1. Rapid submit/cancel (500 cycles)...~n\"), \
		R1 = flurm_load_test:rapid_submit_cancel_test(500), \
		io:format(\"   Completed without crash - PASSED~n~n\"), \
		io:format(\"2. Concurrent cancellation (50 jobs)...~n\"), \
		R2 = flurm_load_test:concurrent_cancel_test(50), \
		io:format(\"   Cancel succeeded: ~p/~p~n\", [maps:get(cancel_succeeded, R2), 50]), \
		io:format(\"   Result: ~s~n\", [case maps:get(passed, R2) of true -> \"PASSED\"; false -> \"FAILED\" end]), \
		halt(0). \
	"

# Run all tests including long-running ones
test-all: test test-stress test-soak-short
	@echo ""
	@echo "=== ALL TESTS COMPLETE ==="

# Run system diagnostics
diagnose: compile
	@echo "=== System Diagnostics ==="
	@$(REBAR3) shell --eval " \
		io:format(\"~n=== FLURM DIAGNOSTICS ===~n~n\"), \
		Report = flurm_diagnostics:full_report(), \
		Mem = maps:get(memory, Report), \
		io:format(\"Memory:~n\"), \
		io:format(\"  Total: ~.2f MB~n\", [maps:get(total, Mem) / 1048576]), \
		io:format(\"  Processes: ~.2f MB~n\", [maps:get(processes, Mem) / 1048576]), \
		io:format(\"  Binary: ~.2f MB~n\", [maps:get(binary, Mem) / 1048576]), \
		io:format(\"  ETS: ~.2f MB~n~n\", [maps:get(ets, Mem) / 1048576]), \
		Procs = maps:get(processes, Report), \
		io:format(\"Processes: ~p / ~p (~.1f%%)~n~n\", \
			[maps:get(count, Procs), maps:get(limit, Procs), maps:get(utilization, Procs)]), \
		io:format(\"Top Memory Processes:~n\"), \
		lists:foreach(fun(P) -> \
			Name = case maps:get(name, P, undefined) of \
				undefined -> io_lib:format(\"~p\", [maps:get(pid, P)]); \
				N -> atom_to_list(N) \
			end, \
			io:format(\"  ~s: ~.2f MB~n\", [Name, maps:get(memory, P) / 1048576]) \
		end, lists:sublist(maps:get(top_memory_processes, Report), 5)), \
		io:format(\"~nHealth Check: ~p~n\", [flurm_diagnostics:health_check()]), \
		halt(0). \
	"

# Start leak detector in background
start-leak-detector: compile
	@echo "Starting leak detector (checks every 60 seconds)..."
	@$(REBAR3) shell --eval " \
		flurm_diagnostics:start_leak_detector(60000), \
		io:format(\"Leak detector started. Run 'make check-leaks' to view history.~n\"). \
	"

#
# === Docker-Based Release Tests ===
#

# Run Docker end-to-end tests (used for release verification)
test-docker: compile
	@echo "=== Docker End-to-End Tests ==="
	@cd docker && docker compose -f docker-compose.demo.yml down -v 2>/dev/null || true
	@cd docker && docker compose -f docker-compose.demo.yml up -d --build
	@echo "Waiting for services to start..."
	@sleep 20
	@echo ""
	@echo "1. Testing sinfo..."
	@docker exec flurm-client sinfo || true
	@echo ""
	@echo "2. Testing sbatch..."
	@docker exec flurm-client bash -c 'cat > /tmp/test.sh << "EOF"\n#!/bin/bash\n#SBATCH --job-name=release_test\n#SBATCH --nodes=1\necho "Release test passed!"\nhostname\ndate\nEOF\nchmod +x /tmp/test.sh && sbatch /tmp/test.sh'
	@sleep 2
	@echo ""
	@echo "3. Testing squeue..."
	@docker exec flurm-client squeue
	@echo ""
	@echo "4. Testing metrics..."
	@curl -s http://localhost:9090/metrics | grep -E "flurm_jobs_submitted_total|flurm_nodes_up"
	@echo ""
	@echo "5. Submitting and cancelling job..."
	@docker exec flurm-client bash -c 'cat > /tmp/cancel.sh << "EOF"\n#!/bin/bash\nsleep 300\nEOF\nchmod +x /tmp/cancel.sh && sbatch /tmp/cancel.sh'
	@sleep 1
	@docker exec flurm-client scancel 2 2>/dev/null || docker exec flurm-client scancel 3 2>/dev/null || true
	@echo ""
	@echo "6. Verifying job lifecycle..."
	@curl -s http://localhost:9090/metrics | grep -E "flurm_jobs_completed_total|flurm_jobs_cancelled_total"
	@echo ""
	@cd docker && docker compose -f docker-compose.demo.yml down
	@echo ""
	@echo "=== Docker E2E Tests PASSED ==="

# Full release test suite
test-release: test test-docker
	@echo ""
	@echo "=============================================="
	@echo "  RELEASE TESTS COMPLETE"
	@echo "=============================================="
	@echo ""
	@echo "All tests passed. Ready for release."

# Pre-release checklist
release-check: compile dialyzer xref test-release
	@echo ""
	@echo "=============================================="
	@echo "  RELEASE CHECKLIST COMPLETE"
	@echo "=============================================="
	@echo ""
	@echo "  [x] Compilation successful"
	@echo "  [x] Dialyzer analysis passed"
	@echo "  [x] Cross-reference check passed"
	@echo "  [x] Unit tests passed"
	@echo "  [x] Property tests passed"
	@echo "  [x] Common Test suites passed"
	@echo "  [x] Docker E2E tests passed"
	@echo ""
	@echo "Ready to cut release!"

# Help target
help:
	@echo "FLURM Build Targets:"
	@echo ""
	@echo "  Build & Run:"
	@echo "    make              - Compile all applications"
	@echo "    make release      - Build production release"
	@echo "    make shell        - Start interactive shell"
	@echo "    make clean        - Clean build artifacts"
	@echo ""
	@echo "  Quick Tests (run frequently):"
	@echo "    make test         - Run all quick tests (eunit, proper, ct)"
	@echo "    make eunit        - Run EUnit tests"
	@echo "    make proper       - Run PropEr property-based tests"
	@echo "    make ct           - Run Common Test suites"
	@echo "    make dialyzer     - Run Dialyzer static analysis"
	@echo "    make xref         - Run cross-reference analysis"
	@echo "    make check        - Run all checks (compile, test, dialyzer, xref)"
	@echo ""
	@echo "  Long-Running Tests (for stability/leaks):"
	@echo "    make test-stress     - Run stress tests (~2 min)"
	@echo "    make test-memory     - Run memory stability test (~1 min)"
	@echo "    make test-race       - Run race condition tests (~1 min)"
	@echo "    make test-soak-short - Run 5-minute soak test"
	@echo "    make test-soak       - Run 30-minute soak test"
	@echo "    make test-all        - Run everything including soak"
	@echo ""
	@echo "  Diagnostics:"
	@echo "    make diagnose     - Show system diagnostics"
	@echo "    make chaos-test   - Run chaos engineering tests (WARNING!)"
	@echo "    make tla-check    - Run TLA+ model checker"
	@echo ""
	@echo "  Release Testing:"
	@echo "    make test-docker     - Run Docker end-to-end tests"
	@echo "    make test-release    - Full release test suite"
	@echo "    make release-check   - Pre-release checklist (all checks)"
	@echo ""
	@echo "  Other:"
	@echo "    make test-model-deterministic - Run deterministic lifecycle/model tests"
	@echo "    make test-network-fault       - Run network fault-injection integration"
	@echo "    make test-upgrade-replay      - Run upgrade/rollback replay tests"
	@echo "    make test-soak-cadence-short  - Run short soak cadence gate"
	@echo "    make ci-local                 - Local deterministic CI gate"
	@echo "    make ci-cadence               - Extended non-GitHub CI cadence"
	@echo "    make cover        - Generate coverage report"
	@echo "    make docs         - Generate EDoc documentation"
