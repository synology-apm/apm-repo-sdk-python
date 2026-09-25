# CLAUDE.md — GitHub Actions Conventions

This file provides context for Claude Code when working with files under `.github/workflows/`.

---

## Pinning Conventions

Every `uses:` in `.github/workflows/*.yml` must reference a full commit SHA with a trailing
`# vX.Y.Z` comment (e.g. `uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 #
v7.0.1`), never a mutable version tag or branch. Pin a new action to its commit SHA in the same
commit that adds it — resolve the SHA via `git ls-remote <repo-url> refs/tags/<tag>` (for an
annotated tag, use the dereferenced `<tag>^{}` commit SHA, not the tag object SHA). A local
reusable-workflow reference (`uses: ./.github/workflows/ci.yml`) is not an external action and
is exempt from this rule.

There is no `.github/dependabot.yml` in this repository — pins are kept current only reactively, when a
CVE/advisory is filed against the currently-pinned version, resolved by hand the same way a new
pin is added. There is no proactive/scheduled freshness pass; a human decides when to re-check.
`make bump-external-versions` (`scripts/check_actions_versions.py --write`) automates that
resolution/rewrite step — for every pinned action, it resolves the highest upstream version tag
via `git ls-remote` and rewrites the pin's SHA/tag comment in place — but it's still only ever
run manually, not on a schedule.

> **Warning:** `make github-act-simulation` runs `docs.yml`'s
> `build` job, then `release.yml`'s `verify-dist` job (which pulls in `test` — the
> reusable call into `ci.yml` — and `build` as `needs:` dependencies, so all three run).
> Never point it at `docs.yml`'s `deploy` job or `release.yml`'s
> `publish-sdk`/`publish-cli`/`publish-browser`/`github-release` — those publish to GitHub
> Pages / PyPI via OIDC (`pages: write` / `id-token: write`, PyPI Trusted Publishing) or
> create a real GitHub Release (`contents: write`), which a local `act` run can not and
> should not exercise.
>
> `.github/workflows/dependabot-auto-merge.yml` has no local `act` simulation target —
> its entire logic is a one-line author check gating a real `gh pr merge --auto` call
> against a real pull request, which `act` cannot meaningfully fabricate.

## Dependabot Auto-Merge

`.github/workflows/dependabot-auto-merge.yml` auto-merges every PR authored by `dependabot[bot]`
(via `gh pr merge --auto --squash`, which only enables GitHub's native auto-merge — the actual
merge still waits on the `test` required status check from `ci.yml`). Gating on the author
alone (no label check) is sufficient because this repository has no `dependabot.yml`: with no
version-updates configured for any ecosystem, "Dependabot security updates" is the only
mechanism that can make `dependabot[bot]` open a PR here, so every such PR is a security
update. This applies uniformly regardless of semver bump size, including to
`pypa/gh-action-pypi-publish` and `softprops/action-gh-release`.

> **Warning:** `pypa/gh-action-pypi-publish` and `softprops/action-gh-release` are exercised
> only by `release.yml` on a tag push, never by a PR — so the `test` check passing does not
> mean a security bump to either of these two has actually been run with its new pin. That gap
> is accepted for simplicity rather than special-cased.

> **Warning:** If `dependabot.yml` version-updates are ever added for any ecosystem, this
> workflow must be revisited (e.g. reintroduce a security-only filter) — otherwise it will
> start auto-merging routine, non-security dependency bumps too.

This depends on manual, non-committable repository Settings: Code security (Dependabot alerts +
security updates), General → Pull Requests (Allow auto-merge, Allow squash merging), and a
branch protection rule on `main` requiring the `test` status check.

## Release / publish channel

`release.yml` publishes to public PyPI on every `v*` tag push — see `CONTRIBUTING.md`'s
"Release / publish channel" for what triggers it and the trusted-publishing prerequisite. The one
detail not covered there: where to set it up — Settings → Publishing → Trusted Publishers, pointing
at this repository and the matching `environment:` name.
