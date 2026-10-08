.PHONY: test lint mypy clean-caches test-fast test-quick test-unit test-integration test-cov smoke-test record-fixture anonymize-fixtures check-version-consistency check-sdk-import-boundary check-sdk-layers check-browser-layers bump-external-versions build whl docs clean github-act-simulation help
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

test-integration: ## pytest over tests/integration/ only (recorded real-sample fixtures)
	uv run pytest -q -n auto tests/integration

test-cov: ## pytest recording coverage data only (no report, no gate) -- every CI pytest leg; CI combines them and gates the result
	uv run pytest -q -n auto --durations=15 --cov=synology_apm_repo --cov-report= --cov-fail-under=0

smoke-test: ## Run the sdk/cli/browser smoke tests against the real samples configured in tests/smoke/smoke_samples.toml
	@status=0; \
	uv run python -m tests.smoke.sdk || status=1; \
	uv run python -m tests.smoke.cli || status=1; \
	uv run python -m tests.smoke.browser || status=1; \
	exit $$status

record-fixture: ## Re-record tests/integration/ fixtures against a real sample; `PYTHONPATH=tests uv run python -m support.recording.manifest` prints each fixture's TARGET and TEST (several space-separated node ids merge into one recording)
	@test -n "$(TARGET)" -a -n "$(TEST)" || (echo "usage: make record-fixture TARGET=local:<path-to-sample-dir>|profile:<name> TEST=tests/integration/.../test_x.py::test_func" >&2 && exit 1)
	uv run pytest --record-against=$(TARGET) $(TEST)

anonymize-fixtures: ## Re-run the catalog-metadata anonymizer over FIXTURES (default: every tests/fixtures/*.json.gz) -- after extending its SENSITIVE_FIELDS
	PYTHONPATH=tests uv run python -m support.recording.anonymize_catalog_metadata $(FIXTURES)

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
github-act-simulation: ## Locally test docs.yml's build job, then release.yml's test+build+verify-dist jobs, via `act` (needs act + Docker; never runs deploy/publish-*)
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
		--matrix os:ubuntu-latest

clean: ## Remove dist/, coverage/docs build artifacts and smoke-test reports
	rm -rf dist/ htmlcov/ tests/smoke/reports/
	$(MAKE) -C docs clean

clean-caches: ## Remove the mypy/ruff/pytest caches (CI restores .mypy_cache, so `clean` leaves them alone)
	rm -rf .mypy_cache/ .ruff_cache/ .pytest_cache/
