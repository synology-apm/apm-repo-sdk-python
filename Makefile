.PHONY: test lint mypy clean-caches test-fast test-quick test-unit test-integration test-cov audit audit-advisories check-lock-sources deptry coverage-gate check-version-consistency check-sdk-import-boundary check-sdk-layers check-browser-layers bump-external-versions build whl docs clean github-act-simulation help
.DEFAULT_GOAL := help

help: ## List available targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-28s %s\n", $$1, $$2}'

test: lint ## Run the full pre-commit gate: `make lint`, then pytest with coverage enforced
	uv run pytest -q -n auto --durations=15 --cov=synology_apm_repo --cov-report=term-missing --cov-report=html

lint: ## Static checks only: format check, lint, version-consistency + import-boundary + SDK layer/import-cycle + browser layer checks, mypy (linux/win32/darwin)
	uv run ruff format --check .
	uv run ruff check .
	@$(MAKE) check-version-consistency
	@$(MAKE) check-sdk-import-boundary
	@$(MAKE) check-sdk-layers
	@$(MAKE) check-browser-layers
	@$(MAKE) mypy

mypy: ## Type-check for linux/win32/darwin, the three runs in parallel (each with its own cache; output printed per platform)
	@mkdir -p .mypy_cache; status=0; \
	for p in linux win32 darwin; do \
		(uv run mypy --platform $$p --cache-dir .mypy_cache/$$p > .mypy_cache/$$p.log 2>&1; echo $$? > .mypy_cache/$$p.rc) & \
	done; wait; \
	for p in linux win32 darwin; do \
		echo "mypy --platform $$p:"; cat .mypy_cache/$$p.log; \
		[ "$$(cat .mypy_cache/$$p.rc)" = 0 ] || status=1; \
	done; exit $$status

test-fast: ## pytest only, no lint/mypy/coverage -- the quickest full-suite run
	uv run pytest -q -n auto

test-quick: ## pytest only, without the Textual Pilot tests (`-m "not pilot"`) -- the quick inner loop
	uv run pytest -q -n auto -m "not pilot"

test-unit: ## pytest over tests/unit/ only (synthetic data)
	uv run pytest -q -n auto tests/unit

test-integration: ## pytest over tests/integration/ only (replays of the recorded fixtures)
	uv run pytest -q -n auto tests/integration

test-cov: ## pytest recording coverage data only (no report, no gate) -- every CI pytest leg; `make coverage-gate` combines and gates them per OS
	uv run pytest -q -n auto --durations=15 --cov=synology_apm_repo --cov-report= --cov-fail-under=0

# The OSes whose pytest legs must all have reported; `act` has the Linux ones only.
COVERAGE_OSES ?= Linux macOS Windows

coverage-gate: ## CI: combine the pytest legs' .coverage.<OS>-<python> files, gate each of COVERAGE_OSES on its own (pyproject's fail_under), then one HTML report of them all
	@status=0; for os in $(COVERAGE_OSES); do \
		uv run coverage combine --keep --quiet --data-file=.coverage.$$os .coverage.$$os-* || exit 1; \
		printf '%s: ' $$os; uv run coverage report --data-file=.coverage.$$os --format=total || status=1; \
		rm -f .coverage.$$os; \
	done; \
	uv run coverage combine --quiet .coverage.*-* && uv run coverage html --fail-under=0 && exit $$status

audit: audit-advisories check-lock-sources ## Dependency checks: known advisories (pip-audit) and where every locked package comes from

audit-advisories: ## Audit every locked dependency, all groups, against the PyPI advisory database (needs network)
	@T=$$(mktemp) && trap 'rm -f "$$T"' EXIT && \
	uv export --frozen --all-packages --all-groups --no-emit-workspace --quiet -o "$$T" && \
	uv run pip-audit -r "$$T" --disable-pip --require-hashes --progress-spinner off

check-lock-sources: ## Verify every package in uv.lock comes from PyPI or the workspace
	uv run python scripts/check_lock_sources.py

deptry: ## Unused, missing and transitive-only dependencies, per package (ignores in each package's pyproject.toml)
	@status=0; for pkg in sdk cli browser; do \
		echo "deptry synology-apm-repo-$$pkg:"; \
		(cd packages/synology-apm-repo-$$pkg && uv run deptry .) || status=1; \
	done; exit $$status

check-version-consistency: ## Verify the three packages share one lockstep version (and the SDK dependency pin matches it)
	uv run python scripts/check_version_consistency.py

check-sdk-import-boundary: ## Verify the CLI/browser packages and examples/ import the SDK only through its public modules, and never each other
	uv run python scripts/check_sdk_import_boundary.py

check-sdk-layers: ## Verify SDK-internal imports only point downward through ARCHITECTURE.md's layers and form no import cycle
	uv run python scripts/check_sdk_layers.py

check-browser-layers: ## Verify each browser layer below screens/ imports only what it builds on (the browser README's MVU section)
	uv run python scripts/check_browser_layers.py

bump-external-versions: ## Rewrite outdated GH Actions pins and upgrade uv.lock within existing constraints (needs network; review `git diff`, then run `make test`)
	uv run python scripts/check_actions_versions.py --write
	uv lock --upgrade

build: test whl ## Run test, then build wheel + sdist → dist/

whl: ## Build wheel + sdist → dist/ (skips test)
	@echo "Cleaning old build artifacts..."
	rm -rf dist/synology-apm-repo-sdk dist/synology-apm-repo-cli dist/synology-apm-repo-browser
	@for pkg in synology-apm-repo-sdk synology-apm-repo-cli synology-apm-repo-browser; do \
		echo "Building $$pkg..."; \
		uv build --package $$pkg -o dist/$$pkg || exit 1; \
		echo ""; \
		echo "dist/$$pkg contents:"; \
		ls -1 dist/$$pkg; \
		echo ""; \
	done

docs: ## Build Sphinx API docs → docs/_build/html (nitpicky; any warning fails the build)
	$(MAKE) -C docs html O="-n -W --keep-going"

# --matrix os:ubuntu-latest: act has no Windows/macOS runners, and act 0.2.89
# mis-evaluates `runs-on: ${{ matrix.os }}` for every leg once ubuntu-latest is
# one of the values (empty image or the wrong OS) unless the matrix is filtered.
github-act-simulation: ## Locally test docs.yml's build job, then release.yml's verified+ci+advisories+gate+build+verify-dist jobs, via `act` (needs act + Docker; never runs deploy/publish-*/github-release)
	@T=$$(mktemp -d) && trap 'rm -rf "$$T"' EXIT && \
	echo '{"ref": "refs/heads/main"}' > "$$T/docs-event.json" && \
	act push -W .github/workflows/docs.yml -j build \
		--eventpath "$$T/docs-event.json" \
		--artifact-server-path "$$T/artifacts-docs" && \
	V=$$(uv run python -c "import tomllib; print(tomllib.load(open('packages/synology-apm-repo-sdk/pyproject.toml', 'rb'))['project']['version'])") && \
	echo "{\"ref\": \"refs/tags/v$$V\"}" > "$$T/release-event.json" && \
	act push -W .github/workflows/release.yml -j verify-dist \
		--eventpath "$$T/release-event.json" \
		--artifact-server-path "$$T/artifacts-release" \
		--matrix os:ubuntu-latest \
		--env COVERAGE_OSES=Linux

clean: ## Remove dist/ and the coverage/docs build artifacts
	rm -rf dist/ htmlcov/
	$(MAKE) -C docs clean

clean-caches: ## Remove the mypy/ruff/pytest caches (CI restores .mypy_cache, so `clean` leaves them alone)
	rm -rf .mypy_cache/ .ruff_cache/ .pytest_cache/
