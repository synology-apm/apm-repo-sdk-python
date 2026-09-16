.PHONY: test test-unit smoke-test record-fixture lint format typecheck check-version-consistency check-sdk-import-boundary bump-external-versions build whl docs clean github-act-simulation help
.DEFAULT_GOAL := help

help: ## List available targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-28s %s\n", $$1, $$2}'

test: ## Run the full pre-commit gate: format check, lint, mypy, pytest with coverage enforced, then version-consistency + import-boundary checks
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy
	uv run pytest -q -n auto --cov=synology_apm_repo --cov-report=term-missing --cov-report=html
	@$(MAKE) check-version-consistency
	@$(MAKE) check-sdk-import-boundary

test-unit: ## pytest only, no lint/mypy/coverage -- runs the full suite; used by CI to verify compatibility across older supported Python versions
	uv run pytest -q -n auto

smoke-test: ## Run the sdk/cli/browser smoke tests against real, on-disk sample repositories
	@status=0; \
	uv run python -m tests.smoke.sdk || status=1; \
	uv run python -m tests.smoke.cli || status=1; \
	uv run python -m tests.smoke.browser || status=1; \
	exit $$status

record-fixture: ## Re-record one tests/integration/ fixture against a real sample. TEST accepts multiple space-separated node ids (or a whole file) when several tests share one fixture -- their calls merge automatically.
	@test -n "$(TARGET)" -a -n "$(TEST)" || (echo "usage: make record-fixture TARGET=local:<path-to-sample-dir>|profile:<name> TEST=tests/integration/.../test_x.py::test_func" >&2 && exit 1)
	uv run pytest --record-against=$(TARGET) $(TEST)

lint: ## ruff check only
	uv run ruff check .

format: ## ruff format, in place
	uv run ruff format .

typecheck: ## mypy only
	uv run mypy

check-version-consistency: ## Verify the three packages share one lockstep version (and the SDK dependency pin matches it)
	uv run python scripts/check_version_consistency.py

check-sdk-import-boundary: ## Verify the CLI/browser packages only import the SDK's documented public surface
	uv run python scripts/check_sdk_import_boundary.py

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

docs: ## Build Sphinx API docs → docs/_build/html
	$(MAKE) -C docs html

github-act-simulation: ## Locally test docs.yml's build job, then release.yml's test+build+verify-dist jobs, via `act` (needs act + Docker; never runs deploy/publish-*)
	@echo '{"ref": "refs/heads/main"}' > /tmp/act-docs-event.json
	act push -W .github/workflows/docs.yml -j build \
		--eventpath /tmp/act-docs-event.json \
		--artifact-server-path /tmp/act-artifacts-docs
	@V=$$(uv run python -c "import tomllib; print(tomllib.load(open('packages/synology-apm-repo-sdk/pyproject.toml', 'rb'))['project']['version'])"); \
	echo "{\"ref\": \"refs/tags/v$$V\"}" > /tmp/act-release-event.json; \
	act push -W .github/workflows/release.yml -j verify-dist \
		--eventpath /tmp/act-release-event.json \
		--artifact-server-path /tmp/act-artifacts-release

clean: ## Remove dist/ and coverage/docs build artifacts
	rm -rf dist/ htmlcov/
	$(MAKE) -C docs clean
