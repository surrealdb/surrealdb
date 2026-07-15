# Release Process Documentation

This document describes the SurrealDB release workflow, including how to perform releases, the branching strategy, and version management.

## Table of Contents

- [Overview](#overview)
- [Rolling Builds](#rolling-builds)
- [Downstream Release Automation](#downstream-release-automation)
- [Cutting Release Lines](#cutting-release-lines-cutyml)
- [Release Types](#release-types)
- [Workflow Inputs](#workflow-inputs)
- [Branching Strategy](#branching-strategy)
- [Patch Release Workflow](#patch-release-workflow)
- [Version Management](#version-management)
- [Quick Start](#quick-start)
- [Step-by-Step Instructions](#step-by-step-instructions)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Architecture](#architecture)

## Overview

The release workflow is designed to handle two types of releases:

1. **Versioned Releases**: Stable releases, pre-releases (alpha/beta/rc), and patches
2. **Nightly Releases**: Automated daily builds from the main branch

The workflow is **idempotent**: retrying a failed run (via "Re-run failed jobs") converges without errors or duplicate resources. Re-publishing over an *already-released* version is deliberately guarded — see [Need to Retry a Failed Release](#need-to-retry-a-failed-release).

> For per-release operational notes that administrators should read before
> deploying a new build (wire-format rotations, removed features, etc.),
> see [`UPGRADING.md`](./UPGRADING.md).

## Build-once / promote-many

SurrealDB uses a **build-once / promote-many** model. Every push builds and
stores a complete, release-ready artifact set; cutting a release just *promotes*
an existing build instead of rebuilding. As a result, **all you need to cut a
release is the commit SHA**.

### Rolling builds (`rolling-build.yml` + `rolling-downstream-build.yml`)

The engine rolling build (`rolling-build.yml`) runs on every push to `releases/*`
branches, and — for the nightly (`main`) channel — **once a day on a schedule**
(`0 20 * * *`, ~4h before the `00:00 UTC` nightly promote) rather than on every
`main` push. Only one nightly is promoted per day, and the nightly promote walks
`main` history for the newest built commit, so building every `main` push would be
wasted work; the schedule gives a buffer to notice and re-run a failed nightly
build before promotion. Correctness on every `main` commit is still gated by
`ci.yml`. For each build it:

- builds the full binary matrix (5 platforms) **and** the macOS universal binary,
  and stores them in S3 under `rolling/<branch-slug>/<sha>/`,
- verifies the workspace crates package cleanly and that no publishable crate
  changed without a version bump (`check-crate-versions`),
- builds the multi-arch Docker images, **scans them with Trivy before pushing**
  (CRITICAL/HIGH fails the build, so a vulnerable image never reaches the
  registry), and stores them in a **private ECR** repository as
  `:rolling-<branch-slug>-<sha>` plus a moving `:<branch-slug>` tag, and
- writes a `rolling/<branch-slug>/<sha>/engine.json` manifest — but **only after
  every other engine job has succeeded**. The presence of that manifest is what
  marks the engine side of a commit as releasable.

The **downstream** image is built by a separate workflow
(`rolling-downstream-build.yml`) that runs on pushes to `releases/*` **and**,
for the nightly (`main`) channel, **once a day on a schedule** (`0 20 * * *`,
the same cadence as the engine nightly). The engine and downstream nightlies are
independent runs and fully **decoupled** — if one fails, the other is unaffected.
It can additionally be dispatched on its own (e.g. by the downstream repo via `gh
workflow run`) so a downstream-only change becomes promotable without waiting for
the next engine commit. It records a **separate** `downstream.json` (only when its
image builds), keyed by the same engine SHA, so each side is promoted from its own
manifest and the two sides never share an object.

Rolling builds keep full `mode=max` provenance + SBOM attestations on the private
ECR image (internal audit trail). They do **not** publish crates (crates.io
versions are write-once) and do **not** re-run the CI quality/test matrix —
correctness is gated by `ci.yml` on the same commit. The baked binary version
comes from `Cargo.toml` plus build metadata (commit date/rev), so a `releases/*`
commit already carries its intended release version. Every job is guarded by
`github.repository`, so the workflow never runs in the public mirror.

#### Channels

Each build produces its branch's **channel** via the reusable `rolling-channel.yml`
workflow: the `nightly` channel on `main`, or the line's channel on `releases/*`
(alpha/beta/rc/stable, per the branch's `Cargo.toml` version). The artifacts are
namespaced under `rolling/<branch-slug>/<sha>/` and garbage-collected per line.

#### Test builds on other branches

A manual dispatch against any branch that is **neither `main` nor a `releases/*`
line** is treated as a throwaway test build — useful for verifying a nightly-build
fix on a branch before it merges to `main`. It builds, scans and pushes exactly
like a real build, but the `finalize` step writes **no `engine.json`** (so it can
never be promoted) and a `cleanup-test` job then **deletes** the run's S3 binaries
and ECR image tags, since unpromotable artifacts aren't kept around. Dispatching
against `main` (or `releases/*`) writes the manifest as normal, so a manual `main`
build is promotable afterwards.

#### Manual rebuild (recover a commit's artifacts)

Both rolling workflows accept a `workflow_dispatch` to reproduce a specific
commit's artifacts — handy if they were pruned or lost before the commit could be
promoted. Dispatch **against the branch line** and pass the exact commit:

- `rolling-build.yml` (engine) takes an optional `git-sha` and rebuilds the engine
  binaries, Docker images and `engine.json` from the code at that sha
  (deterministic — same version and build metadata).
- `rolling-downstream-build.yml` (downstream) takes an optional **`engine-git-sha`**.
  Set on its own it reads that engine sha's recorded `downstream.json` and rebuilds
  the **exact downstream commit** it was originally paired with (not the current
  downstream tip), so the rebuild is faithful.

Both inputs accept a **commit SHA or a branch/ref** (a branch resolves to its tip).
The artifact line/slug is derived from the **dispatched ref**, so pass a commit or
ref that belongs to the branch you dispatched against. Rebuilds reuse the same S3
prefix and image tags, so they **overwrite** the commit's existing rolling
artifacts in place. Omitting the sha builds the branch tip (the same as a push).

Engine rebuilds work on any built line, including `main` (nightly). A downstream
dispatch against `main` is **promotable** too (it is the nightly channel): it
builds, scans, pushes and writes a `downstream.json` keyed by the engine sha, so
you can build a downstream image for cloud testing off `main` and promote it as a
downstream-only nightly. Builds on `releases/*` (versioned) and `main` (nightly) are
promotable; a dispatch against any **other** branch still builds/scans/pushes the
image (so you can verify an arbitrary branch builds correctly) but is never marked
promotable.

#### Overriding the engine↔downstream pairing (pick the downstream commit)

By default the downstream image is paired with the downstream branch tip that
matches the engine line (or, on a reproduce, the recorded pairing). To ship a
**specific** downstream commit — decoupling which downstream code a release
contains from the recorded mapping — dispatch `rolling-downstream-build.yml` with
the optional **`downstream-git-sha`** (a downstream **branch**, which resolves to
its tip, or a bare **commit sha**):

- `downstream-git-sha` set, `engine-git-sha` empty → build that downstream
  branch/commit against the **engine branch tip**.
- both set → build that downstream branch/commit against the **chosen engine
  commit** — a fully explicit pairing.

An override is always treated as an intentional pairing (built and, on a
`releases/*` dispatch, recorded as promotable), so it takes precedence over the
recorded `downstream.json` and skips the reproduce lookup. Because
`downstream.json` is keyed by the **engine** sha, promoting that engine commit
later ships exactly the downstream commit you paired here. Both inputs are
optional; leaving both empty preserves the default push/tip behaviour.

### Promotion (`release.yml`, manual / scheduled)

`release.yml` is **promote-only** — it builds and tests nothing. Given a commit
SHA it:

- verifies a rolling `engine.json` manifest (and, for a downstream release, a
  `downstream.json`) exists for the commit. For a **versioned** (release-branch)
  release it **waits** for an in-progress build to finish rather than failing
  immediately (see "Waiting for an in-progress build" below); a nightly instead
  walks `main` history for the newest already-built commit,
- copies + renames the prebuilt binaries from `rolling/<branch>/<sha>/` to the
  release name (e.g. `nightly/`, `v3.1.2/`), sets the download pointers, and
  creates the GitHub release + tag,
- publishes the crates **from the exact SHA** for versioned releases (nightly
  never publishes crates); the crate version is whatever `Cargo.toml` carries at
  that commit,
- **mirrors** the prebuilt Docker image from private ECR to the public DockerHub
  using `docker buildx imagetools create` (no rebuild), reproducing the full tag
  matrix (`:vX.Y.Z`, `:X.Y`, `:X`, `:latest`, `:nightly`, `-dev`, etc.), and
- propagates the binaries to all regions, then prunes superseded rolling
  artifacts for that branch line.

#### Waiting for an in-progress build (versioned releases)

Releasing the **tip** of a release branch often races that tip's rolling build,
which may still be running. Rather than failing immediately, a **versioned**
release **waits** for the required build(s) to finish before the approval gate:

- It always waits for the **engine** build — even for a `components: downstream`
  release, because promoting the downstream image implies wanting to promote the engine at
  the same commit later, so the engine build must succeed too.
- It additionally waits for the **downstream** build when the downstream side is
  being released.

The wait polls S3 for the promotable artifacts and the Actions API for the
build's run state, so a build that has already **failed** errors out immediately
(no point waiting the whole window), while a build still in progress blocks until
it is ready. On timeout it fails with a clear error. The window is the
**`build-wait-minutes`** input (default **120**). This waiting applies to
**versioned (release-branch) releases only** — a nightly never waits; it walks
`main` history for the newest already-built commit.

#### Approval gate

Everything that ships something permanent — crates.io, the public download
bucket + GitHub release, the DockerHub image, the downstream image mirror and the
regional replication — sits behind a single `approve` job. Every one of those
jobs `needs: approve`, and the gate runs **before** `publish-crates`/`promote`,
so **nothing is published until the run is approved**. The gate binds a GitHub
[environment](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)
named after the channel (`nightly`, `alpha`, `beta`, `rc`, `stable`), so approval
is configured **per channel**:

- **Versioned channels** (`alpha`/`beta`/`rc`/`stable`): add **required
  reviewers** to those environments so a human must approve before anything is
  published.
- **`nightly`**: leave it **without reviewers** so the scheduled nightly stays
  unattended.
- **Dry-runs** (`publish: false`): the gate evaluates to *no* environment and
  flows straight through — a dry-run never waits and never publishes.

The `approve` job's name carries the resolved version and channel (e.g.
`Approve release v3.1.2 (stable)`) and it writes a summary table (version,
channel, commit, branch, components, publish, latest), so the approver can verify
exactly what is about to ship directly from the approval prompt / run view.

##### Milestone readiness check

An advisory `check-milestones` job runs **before** `approve`. If a milestone
**named after the release** (e.g. `v3.1.2` or `v3.1.2-beta.1`) exists on the
engine repo and/or the downstream repo, it warns about two things (issues are
ignored — only PRs are considered):

- **Unmerged PRs** — milestone PRs that are still open, so they haven't merged
  anywhere yet.
- **Unbackported PRs** — milestone PRs that *are* merged but do **not** appear in
  the exact commit being released. Merging to `main` is not enough: the change
  must be **backported onto the release line** and be part of the released commit.
  A PR may be backported directly or **bundled inside another backport PR**, so
  presence is checked against the history reachable from the released commit, not
  the PR's own merge commit. A merged PR counts as present if the released commit
  **descends from** its merge commit, the released history references it by `(#N)`
  (GitHub's squash tag), a `git cherry-pick -x` trailer names its merge commit,
  or — the **message-agnostic** signal — the PR's **diff (patch-id)** matches a
  commit on the release line. That last one is what keeps the check accurate when
  backports **rewrite the commit message** (e.g. dropping the `(#N)` tag so PR
  numbers don't leak into the public repo) or cherry-pick without `-x`: the diff
  is unchanged, so the patch-id still matches. The only residual gap is a backport
  whose diff was altered by **conflict resolution**; that is flagged for a human
  to confirm. This is a best-effort heuristic; when it can't find a PR it warns so
  a human can confirm (rather than assuming a gap).
  Engine PRs are checked against the **engine** release commit; downstream PRs
  against the **downstream** commit the image was built from (only when a
  downstream image is being released).

It is purely advisory: it **never fails the run**. Because the approval gate holds
the `approve` job's own steps until *after* approval, the warnings are surfaced
where the approver looks before deciding — as run **annotations** and a **job
summary** on the `check-milestones` job, and folded into the `approve` job
**name** (a `— WARNING: N unmerged, M unbackported milestone PR(s)` suffix) and
its summary table. When there is anything to review it additionally publishes a
**`neutral` check run** ("Milestone readiness") on the released commit — a
distinct non-green, non-red icon in the commit's checks list, since a job's own
icon can only ever be a green tick or a red cross. That publish is best-effort
(`continue-on-error` + an internal guard): a missing `checks: write` grant or API
error only warns and **never fails the job** — which matters because `approve`
needs it, so a red `check-milestones` would skip `approve` and block the release.
Reading the downstream repo's milestone (and checking out its released commit)
uses the downstream App token and is likewise best-effort (skipped, not failed,
if unavailable). Nightly has no versioned milestone, so the check is a no-op
there.

> Required reviewers are a **repository setting** (Settings → Environments), not
> part of this workflow. Until they are configured on the versioned channels, the
> gate is present but does not block. Configure them before the first attended
> release.

#### Docker mirror and attestations

The mirror **strips** the build attestations: it recreates the image index from
the per-platform manifests only. This is required because the ECR image's
`mode=max` provenance encodes the ECR registry host (and therefore the AWS
account ID), which must never reach the public registry. The per-platform
manifest digests are preserved, so the public image is byte-identical to run;
only the build attestations are dropped. Re-publishing clean, regenerated
attestations together with image/binary **signing** is planned as a follow-up.

## Downstream release automation

Once a versioned release is **successfully promoted and published**, `release.yml`
opens a pull request on the Homebrew tap so that release-time chore no longer has
to be done by hand. It is:

- **opt-in** — no-ops green until its secrets are provisioned, and never runs on
  the public mirror,
- authenticated with a **short-lived GitHub App installation token** (no standing
  PAT), scoped to the target repo, and
- opened as a **PR for a human to review and merge** — nothing is auto-merged and
  the App has no merge permission.

> **Cloud version registration** previously opened a PR on the Cloud consumer
> repository to register the newly released SurrealDB version. That repository now
> exposes a management API endpoint for this, so the PR-based registration has
> been removed; calling the endpoint from `release.yml` is a planned follow-up.

### Homebrew tap

For the **latest stable** release only — `latest` is true only for a stable
(non-prerelease) engine release whose version is `>=` the highest already-published
stable version, so betas, downstream-only releases, and older/superseded versions
never apply — `release.yml` checks out the Homebrew tap, runs its
`release.sh` (with `SKIP_GIT=1` and the exact released version pinned as an
argument) to regenerate the `surreal` formula from the published download
metadata, and opens an `Upgrade to vX.Y.Z` PR. It runs after promotion so the
release's download hash already exists.

### Configuration

The Homebrew job uses a single GitHub App (Contents + Pull requests write, **no
merge permission**) installed on the target repo, referenced from this repo's
secrets:

| Secret / variable | Purpose |
| --- | --- |
| `DOWNSTREAM_RELEASE_APP_CLIENT_ID` | GitHub App client id used to mint installation tokens |
| `DOWNSTREAM_RELEASE_APP_PRIVATE_KEY` | GitHub App private key |

The Homebrew tap is a public repo, so its name is hardcoded in the workflow
rather than carried as a secret; the Homebrew job runs whenever the release App
above is configured.

When a secret is absent the corresponding job logs a notice and succeeds without
doing anything.

### Downstream reproducibility tags

The downstream image is built from a downstream commit with a Cargo `[patch]`
that redirects the engine dependency to a **colocated checkout of this engine at
the released engine sha** (see [Rolling downstream](#rolling-builds-rolling-buildyml--rolling-downstream-buildyml)).
The downstream's own manifest, however, only tracks the engine by *branch*, so a
plain `git checkout <downstream-sha>` fetches whatever that branch points at
today — **not** the engine code that actually shipped. That makes a released
downstream image hard to reproduce from source.

To close that gap, on every versioned downstream release `release.yml`'s
`tag-downstream` job creates a **reproducible tag** on the downstream repo:

- it checks out the downstream at the exact built commit (`downstream-sha` from
  the recorded `downstream.json`),
- rewrites every engine dependency (any `Cargo.toml` referencing the engine repo)
  from its `branch`/`tag`/`rev` selector to `rev = <released-engine-sha>` and
  **re-locks only the engine crates** — every other dependency is left exactly as
  the committed downstream `Cargo.lock` had it (the same lock the image build
  started from), so the tag reproduces the shipped dependency graph *by
  construction* rather than by a blanket `cargo update`,
- verifies every engine crate in the lock is pinned to the released sha, then
  commits that pin **on top of** `downstream-sha` and pushes **only the tag**
  (never a branch — the pin commit exists solely to anchor the tag).

The tag name matches the engine release (`vX.Y.Z`), so a downstream incident
maps 1:1 to its engine release. Cloning the downstream and checking out the tag
then rebuilds the exact engine + downstream code that shipped (fetching the
pinned engine commit still requires read access to this private engine repo).

The job runs on dry-runs too — it performs the pin and validation but does not
push — so a dry-run surfaces a broken pin before a real release. It is
idempotent: an existing tag is left untouched unless the release is dispatched
with `overwrite: true`. It reuses the downstream App (`DOWNSTREAM_APP_*` +
`DOWNSTREAM_REPO`), which **must be granted `Contents: write`** to push the tag
(the rolling build uses the same App with only `Contents: read`); it no-ops green
until that App is configured. The pin is implemented by
[`tag-downstream-release.sh`](../.github/scripts/tag-downstream-release.sh).

## Cutting release lines (`cut.yml`)

Promotion never mutates source. Moving the project onto a new version line is a
separate, explicit **cut**, performed by `cut.yml` (Actions → **Cut release**).
A cut produces commits (a new branch and/or a main-bump PR); the rolling build
then turns those commits into promotable artifacts, which you release later with
`release.yml`. The two steps are decoupled, so you can cut now and promote once
the rolling build for the resulting commit is green.

`main` is **always** `X.Y.0-nightly` (the in-flight line's development version).
A cut always creates a `releases/*` branch; what differs is *where in the cycle*
the line is branched, and that controls how `main` moves:

- **Minor cut** (`cut-type: minor`): creates `releases/X.Y` from `main` at
  `X.Y.0-beta.1` and opens a PR moving `main` to `X.(Y+1).0-nightly`. The X.Y line
  is branched at the **end** of its development cycle (beta), so `main` advances
  immediately to the next minor.
- **Major cut** (`cut-type: major`): creates `releases/(X+1).0` from `main` at
  `(X+1).0.0-alpha.1` and opens a PR moving `main` to `(X+1).0.0-nightly`. The new
  major is branched at the **start** of its cycle (alpha), so `main` parks on the
  new major; it only advances to `(X+1).1.0-nightly` once `(X+1).0.0` ships
  stable (automated by `release.yml` — see [Version Management](#version-management)).

So the lifecycle of a major line is: major cut → `releases/(X+1).0` at
`(X+1).0.0-alpha.1`, `main` at `(X+1).0.0-nightly` → alpha/beta/rc/stable all
promoted from the branch → on `(X+1).0.0` stable, `main` advances to
`(X+1).1.0-nightly`.

## Release Types

### Nightly Releases

- **Purpose**: Daily development builds for testing latest features
- **Trigger**: Automatically on a schedule, or manually via workflow dispatch.
  There are **two decoupled nightly promotes**, on separate schedules and separate
  concurrency keys so a failure on one never blocks the other:
  - **`00:00 UTC` — engine nightly** (`components: engine`): promotes the public
    binaries + DockerHub image for the newest built `main` commit.
  - **`00:30 UTC` — downstream nightly** (`components: downstream`): mirrors the
    newest built downstream image to the `nightly` tag. Never tags the
    downstream repo (tagging is versioned-only) and, being on `main`, never moves
    a `latest` pointer.
- **Version Format**: Derived from main's version, which is always `X.Y.0-nightly` (e.g. `3.2.0-nightly`), plus build metadata
- **Artifacts**: Binaries, Docker images (tagged with `nightly`)
- **No**: Crate publishing, Git tags, GitHub releases, or main branch updates

**When to use**: Never manually trigger unless testing the nightly pipeline, or to
promote a **downstream-only** image off `main` for cloud testing (`release-type: nightly`,
`components: downstream`, and a `main` commit or the `main` tip) — this ships the
downstream image without requiring a corresponding engine release and without tagging the
downstream repo.

### Versioned Releases

Versioned releases (`release-type: versioned`) all come from a commit on a
`releases/*` branch, with the version read from `Cargo.toml` at that commit. They
come in several flavors:

#### Pre-Release (Alpha/Beta/RC)

- **Format**: `X.Y.Z-<prerelease>.<N>` (e.g., `4.0.0-alpha.1`, `3.0.0-beta.1`, `3.0.0-rc.2`)
- **Source**: A commit on the release branch for that line (e.g. `releases/4.0`
  for `4.0.0-alpha.1`, `releases/3.0` for `3.0.0-beta.1`). A major cut starts the
  line at `alpha.1`; a minor cut starts it at `beta.1`.
- **Main Branch**: Not updated by the release — `main` stays on its `-nightly`
  development version.
- **Use Case**: Early/feature testing, release candidates.

#### Stable Release (X.Y.0)

- **Format**: `X.Y.0` (e.g., `3.0.0`, `4.0.0`)
- **Source**: A commit on the release branch for that line (e.g. `releases/3.0`)
- **Main Branch**: Advanced to `X.(Y+1).0-nightly` **only if** `main` is still on
  this line's `X.Y.0-nightly` — i.e. for a **major** line (whose cut parked `main`
  on `X.0.0-nightly`). For a **minor** line `main` already advanced at cut time,
  so the release leaves it untouched. (Automated by `advance-main-after-release.sh`.)
- **Latest**: Set automatically when this stable version is `>=` the highest
  already-published stable version (not merely the highest release branch)
- **Use Case**: Major feature releases, production-ready versions

#### Patch Release (X.Y.Z where Z > 0)

- **Format**: `X.Y.Z` (e.g., `3.0.1`, `3.0.2`)
- **Source**: Branch created from the previous version's tag when preparing the patch (e.g. create from `v3.0.0` for release `3.0.1`)
- **Main Branch**: Not updated (remains on next minor nightly)
- **Use Case**: Bug fixes, security patches

## Workflow Inputs

### Required Inputs

#### `release-type`
- **Type**: Choice (`nightly` or `versioned`)
- **Description**: Type of release to perform
- **Default**: `nightly`
- **Note**: `nightly` is cut from `main` and publishes no crates. `versioned`
  covers everything cut from a `releases/*` branch — alpha, beta, rc, stable, and
  patch — with the exact version (and therefore maturity) read from `Cargo.toml`
  at that commit.

### Optional Inputs

#### `git-sha`
- **Type**: String (optional)
- **Default**: empty → the tip of the latest release branch (the highest `releases/X.Y` line)
- **Description**: The commit SHA **or branch** to release. A branch (e.g. `releases/3.2`) resolves to its tip. Left empty, it defaults to the tip of the latest release branch — the usual "ship the newest release line" case. The resolved commit must already have a completed rolling build (an `engine.json` manifest); the release version is taken directly from `Cargo.toml` at that commit, and the branch line is derived from the commit. There is no separate version input.
- **Examples**:
	- empty - release the tip of the latest release branch (the common case)
	- `releases/3.2` - release the tip of that release branch
	- a commit on `main` - for nightly releases (`main` always carries the `-nightly` version)
	- a commit on `releases/3.1` - a specific pre-release, stable, or patch commit

#### `publish`
- **Type**: Boolean
- **Description**: Publish the release (false for dry-run)
- **Default**: `false`
- **When to use**: After verifying dry-run succeeds

#### `components`
- **Type**: Choice (`both`, `engine`, `downstream`)
- **Description**: Which side(s) to promote — the **engine** (public binaries,
  crates, GitHub release, DockerHub image) and/or the **downstream** (the private
  image mirror only).
- **Default**: `both`
- **Notes**: The rolling build writes the two sides as separate manifests
  (`engine.json` and `downstream.json`, keyed by the same engine SHA), so each
  side is promoted from its own manifest and can be selected independently. For a
  **versioned** `downstream` (or `both`) release the promote **waits** for the
  build (see `build-wait-minutes`); a **manual nightly** `downstream` dispatch
  **fails fast** if no downstream image was built for that commit, rather than
  silently shipping without it. The two scheduled nightlies promote one side each
  (`engine` at `00:00`, `downstream` at `00:30`).

#### `overwrite`
- **Type**: Boolean
- **Description**: Allow re-releasing a version that was already published
  (clobbers its binaries, Docker tags and GitHub release). Ignored for nightly.
- **Default**: `false`

#### `build-wait-minutes`
- **Type**: String (integer minutes)
- **Default**: `120`
- **Description**: For a **versioned** (release-branch) release only, how long to
  wait for an in-progress rolling build of the commit to finish before giving up.
  The promote polls for the build's artifacts (and its Actions run state, to fail
  fast on an already-failed build) and only proceeds once they exist; on timeout it
  fails. Nightly ignores this — it walks `main` history for the newest
  already-built commit instead of waiting.

> **Most behaviour is derived, not configured.** Beyond the inputs above
> (`release-type`, `git-sha`, `publish`, `components`, `overwrite`), everything
> else is derived:
>
> - **"latest" is deduced, not an input.** A release is marked latest only when it
>   is a **final** (non-prerelease) **engine** release whose version is `>=` the
>   **highest already-published stable version**. Pre-releases (alpha/beta/rc),
>   nightlies, downstream-only releases, and any older/superseded version are never
>   latest — so re-releasing the current latest (e.g. re-cutting compromised
>   binaries) keeps it latest, while shipping a forgotten older version (or its
>   late component pair) while a newer stable exists does not steal latest.
>   Crucially the branch is **not** the signal: a higher `releases/X.Y` line with
>   no published stable does not own latest, so a lower line legitimately still
>   can. (Implemented in `prepare-vars`' "Determine latest release pointer" step,
>   which compares the version against this repo's published GitHub releases.)
> - **Release branches are created at cut time**, via `cut.yml` — not by a release.
>   Releasing only advances `main` in the one automated case where a line's `.0`
>   ships stable while `main` still rides that line (a major line); the old
>   `update-main` / `main-version` inputs are gone.
> - **There is no `extra-features` input.** Binaries are prebuilt at push time
>   with the standard release feature set (`storage-tikv,jwks,ml`), so the feature
>   set is fixed when the rolling build runs, not at promote time.

## Retention and lifecycle

Rolling artifacts are pruned automatically: on a **successful publish**, the
release prunes the artifacts for commits on that branch line that are **strictly
older** than the released commit. The released commit itself is kept (so the
release stays re-runnable) and is pruned by the next release on the same line.

Pruning is **per side**, symmetric with promotion, so a partial release only
prunes what it actually superseded:

- An **engine** release prunes ancestors' engine artifacts (the heavy S3
  binaries and the engine ECR images).
- A **downstream** release prunes ancestors' downstream ECR images.
- A **both** release prunes both.

The tiny JSON manifests (`engine.json` + `downstream.json`) are **always** kept.
This means promoting one side now and the other side later (for the same commit,
or a newer one) is safe: the side you did **not** release leaves the other side's
ancestor artifacts intact, and whatever a partial release leaves behind is
cleaned up by the **next release of that same side** that supersedes it. So an
engine-only release never deletes a downstream image an ancestor might still need
to promote, and vice versa.

Two consequences worth knowing:

- If nightly releases stop running for a long stretch, un-released `main`
  artifacts accumulate until they resume. (Both nightlies prune independently:
  the engine nightly prunes engine artifacts, the downstream nightly prunes
  downstream images.)
- A release line (or a side of it) that receives commits but is never released
  keeps those artifacts indefinitely.

As a near-zero-effort backstop for orphaned layers from interrupted pushes, set
an **ECR lifecycle policy to expire untagged images after 1 day**. Example
(apply via the AWS console or CLI against the rolling ECR repository):

```json
{
  "rules": [
    {
      "rulePriority": 1,
      "description": "Expire untagged images after 1 day",
      "selection": {
        "tagStatus": "untagged",
        "countType": "sinceImagePushed",
        "countUnit": "days",
        "countNumber": 1
      },
      "action": { "type": "expire" }
    }
  ]
}
```

```bash
aws ecr put-lifecycle-policy \
  --repository-name "<rolling-ecr-repository>" \
  --lifecycle-policy-text file://rolling-ecr-lifecycle.json
```

No scheduled garbage collection is used; prune-on-release plus the untagged
backstop is the whole retention story.

## Branching Strategy

### Branches

```
dev/ci/vX.Y.Z              # Created for the automated version bump PR
                           # (main bump or release-branch patch bump)
backport/<issue>-to-X.Y    # Created for backporting individual fixes (one per fix)
                           # Example: backport/56-to-3.0, backport/57-to-3.0
```

The release version is taken directly from `Cargo.toml` on the `git-ref` being released - the workflow no longer bumps the version or creates a temporary release branch. For patch releases, use the release branch (e.g. `releases/3.1`) for backports and as the release git-ref.

### Branch Lifecycle

1. **For pre-releases (alpha/beta/rc) and stable X.Y.0 releases**:
	- Release from the line's `releases/X.Y` branch, created by a cut
	- The version is read from `Cargo.toml` on that branch

2. **For patch releases (X.Y.Z where Z > 0)**:
	- All fixes must land on `main` first
	- Use the release branch for the series (e.g. `releases/3.1`)
	- For each fix to backport, create individual backport PR (e.g., `backport/56-to-3.0`)
	- Cherry-pick specific fix from main to backport branch
	- Review and merge backport PR into the release branch
	- After all backport PRs are merged, run the release workflow with that branch as git-ref
	- After the release, a PR is automatically opened to bump the release branch to the next patch version (e.g. `3.1.3` → `3.1.4`)

**Best Practice**: Always land fixes on main first, then backport individually. This ensures:
- Main branch always has the latest fixes
- Each backport gets independent code review
- CI checks run on each backported fix
- Clear audit trail of what went into each patch release (one PR per fix)
- Ability to cherry-pick only the fixes needed for a specific patch
- Easy to track which fixes are in which release branches

## Patch Release Workflow

### Main First Philosophy

**All bug fixes must land on `main` first, then be backported to release branches.**

This workflow ensures:
- Main branch is always the most up-to-date and stable
- Fixes are tested on main before backporting
- No "lost fixes" that exist only on release branches
- Clear lineage: every patch fix can be traced back to main

### Individual Backport PRs

**Each fix gets its own backport PR** (e.g., `backport/56-to-3.0`).

Benefits:
- Independent code review for each backport
- Selective backporting (choose which fixes for which releases)
- Easy to track which fixes are in which release
- Simpler to debug if a backport causes issues
- Clear audit trail in PR history

### Example Workflow

```bash
# 1. Fix lands on main
PR #56: "Fix memory leak in query parser" → merged to main

# 2. Create patch branch from previous release tag (when preparing 3.0.1)
git fetch --tags
git checkout -b releases/3.0 v3.0.0
git push origin releases/3.0

# 3. Create individual backport PR
git checkout releases/3.0
git checkout -b backport/56-to-3.0
git cherry-pick abc123  # commit from main
git push origin backport/56-to-3.0
gh pr create --base releases/3.0 --head backport/56-to-3.0

# 4. Review and merge backport PR
# (CI runs, code review happens)
Backport PR merged → releases/3.0 now has the fix

# 5. Repeat for each fix needed in 3.0.1

# 6. When ready, run release workflow
# Git ref: releases/3.0 (version 3.0.1 is read from Cargo.toml on that branch)
```

## Version Management

### Surrealism Crates

**Important**: The `surrealism-*` crates follow independent versioning and are **not updated** during SurrealDB releases.

- Surrealism version: `0.1.x` (independent)
- SurrealDB version: `3.2.0-nightly` (workspace-managed)

The release scripts automatically detect and version only packages starting with `surrealdb-*`:
- ✅ Automatically included: `surrealdb`, `surrealdb-core`, `surrealdb-server`, `surrealdb-types`, `surrealdb-types-derive`, `surrealdb-profiling`
- ❌ Automatically excluded: `surrealism`, `surrealism-runtime`, `surrealism-types`, `surrealism-macros`, `surrealism-demo`

This is handled by the release scripts using `cargo metadata` to dynamically detect package names. If you add a new `surrealdb-*` crate, it will automatically be included in version bumps.

### Main Branch Version Evolution

The version on the `main` branch reflects the **next development target**, not the current release:

```
Timeline (a major line, releases/4.0, where main rides X.0.0-nightly):
┌─────────────┬───────────────┬─────────────┬───────────────┐
│ Release     │ Main Before   │ Release     │ Main After    │
├─────────────┼───────────────┼─────────────┼───────────────┤
│ 4.0.0-alpha.1│ 4.0.0-nightly│ 4.0.0-alpha.1│ 4.0.0-nightly│ (no change)
│ 4.0.0-beta.1│ 4.0.0-nightly │ 4.0.0-beta.1│ 4.0.0-nightly │ (no change)
│ 4.0.0       │ 4.0.0-nightly │ 4.0.0       │ 4.1.0-nightly │ (major line .0)
│ 4.0.1       │ 4.1.0-nightly │ 4.0.1       │ 4.1.0-nightly │ (no change)
│ 4.1.0       │ 4.2.0-nightly │ 4.1.0       │ 4.2.0-nightly │ (no change; the
│             │               │             │               │  minor cut for
│             │               │             │               │  releases/4.1 already
│             │               │             │               │  advanced main)
└─────────────┴───────────────┴─────────────┴───────────────┘
```

`main` always carries a `-nightly` version; it never holds a pre-release or a
stable version. It always rides the in-flight line as `X.Y.0-nightly` and only
advances to the next minor (or major) once that line's `.0` is settled — either
at the cut that branches it (minor) or when its `.0` ships stable (major).

### When main moves

`main` only ever moves between `-nightly` versions. It moves in two places:

1. **At cut time (`cut.yml`)**:
	- **Minor cut**: `main` → next minor nightly (and `releases/X.Y` is created at
	  `X.Y.0-beta.1`). The line is branched at the end of its dev cycle, so `main`
	  moves on immediately.
		- main `3.4.0-nightly` → minor cut → main `3.5.0-nightly`, `releases/3.4` at `3.4.0-beta.1`
	- **Major cut**: `main` → the new major's `-nightly` (and `releases/(X+1).0` is
	  created at `(X+1).0.0-alpha.1`). The line is branched at the start of its
	  cycle, so `main` parks on the new major.
		- main `3.5.0-nightly` → major cut → main `4.0.0-nightly`, `releases/4.0` at `4.0.0-alpha.1`

2. **At release time (`release.yml`), the one automated case**: when a stable
   `X.Y.0` ships and `main` is still on that exact line's `X.Y.0-nightly`, `main`
   advances to `X.(Y+1).0-nightly`. In practice this only fires for a **major**
   line (a minor line already advanced `main` at cut time).
	- release `4.0.0` (from `releases/4.0`), main `4.0.0-nightly` → main `4.1.0-nightly`

There is no `main-version` override input: if an automated bump PR needs a
different target, edit the PR before merging it.

## Quick Start

> **Reading the examples below**: the release input (`git-sha`) accepts a
> **commit SHA or a branch**, and defaults to the tip of the latest release branch
> when left empty. Wherever an example says `Git ref: releases/3.1`, you can pass
> that branch directly (it resolves to the tip) or a specific commit SHA on it;
> the resolved commit must have a completed rolling build. The branch line,
> version, and release type are all derived from the resolved commit.

### Performing a Dry-Run

Always test with a dry-run first:

1. Go to **Actions** → **Release** → **Run workflow**
2. Select inputs:
	- Release type: `versioned`
	- Git SHA: a commit on the `releases/3.1` branch (the version is read from `Cargo.toml` at that commit)
	- Publish: `false` ← **Leave unchecked for dry-run**
3. Click **Run workflow**
4. Verify all jobs succeed

### Publishing the Release

After successful dry-run:

1. **Run workflow again** with same inputs
2. **Check "Publish"** checkbox
3. Click **Run workflow**
4. Monitor the release
5. Merge any release-branch patch/pre-release bump PR the workflow opens. (The
   `main` bump is **not** done here — it already happened when the line was cut;
   see [Cutting Release Lines](#cutting-release-lines-cutyml).)

## Step-by-Step Instructions

### Pre-Release (Alpha/Beta/RC)

> A line's first pre-release is created by its cut (`alpha.1` for a major line,
> `beta.1` for a minor line); subsequent pre-releases come from the auto-opened
> branch bump PRs. All are released the same way, from a `releases/*` commit.

**Example**: Releasing `3.0.0-beta.2` (version `3.0.0-beta.2` in the release branch's `Cargo.toml`)

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Publish: ✗
	```
	→ Verify dry-run succeeds

2. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Publish: ✓
	```

3. **Post-Release**:
	- No `main` branch update for pre-releases — `main` stays on its `-nightly` version
	- "latest" is not set (pre-release)
	- A PR is automatically opened to bump the release branch to its next pre-release (e.g. `3.0.0-beta.2` → `3.0.0-beta.3`)

### Stable Release (X.Y.0)

**Example**: Releasing `3.0.0` (version `3.0.0` in the release branch's `Cargo.toml`)

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Publish: ✗
	```

2. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Publish: ✓
	```

3. **Post-Release**:
	- "latest" is set automatically because `3.0.0` is stable and no higher stable
	  version has been published yet — no input needed
	- `main` is advanced **only if** it still rides this line (a major line, where
	  the cut parked `main` on `3.0.0-nightly` → it now moves to `3.1.0-nightly`).
	  For a minor line `main` was already advanced at cut time and is left untouched.

### Patch Release (X.Y.Z)

**Example**: Releasing `3.0.1`

**Important**: All fixes must land on `main` first, then be backported one fix at a time.

1. **Create patch branch from previous tag** (if not already created):
	```bash
	git fetch --tags
	git checkout -b releases/3.0 v3.0.0
	git push origin releases/3.0
	```

2. **Ensure fixes are merged to main**:
	```bash
	# All bug fixes should already be merged to main branch
	# Example: Fix #56 has been merged to main as commit abc123
	```

3. **Backport each fix individually** (one PR per fix):
	```bash
	# For fix #56
	git checkout releases/3.0
	git pull origin releases/3.0

	# Create a backport branch for this specific fix
	git checkout -b backport/56-to-3.0

	# Cherry-pick the specific fix from main
	git cherry-pick <commit-hash-from-main>

	# Push the backport branch
	git push origin backport/56-to-3.0

	# Create PR targeting releases/3.0
	gh pr create --base releases/3.0 --head backport/56-to-3.0 \
		--title "Backport #56 to releases/3.0" \
		--body "Backports fix #56 from main for 3.0.1 release.

	Original PR: #56
	Original commit: <commit-hash>"
	```

4. **Repeat step 3** for each fix that needs backporting (e.g., fix #57, #58, etc.)
	- One backport PR per fix
	- Review and merge each PR individually

5. **After all backport PRs are merged**, proceed with release

6. **Dry-Run**:
	```
	Release type: versioned
	Git ref: releases/3.0  ← Permanent release branch (version 3.0.1 in its Cargo.toml)
	Publish: ✗
	```

7. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Publish: ✓
	```

8. **Post-Release**:
	- No main branch update (fixes already on main)
	- Main remains at `3.1.0-nightly`
	- "latest" is set automatically only if this version is `>=` the highest
	  already-published stable version; patching an older line while a newer stable
	  exists is correctly **not** marked latest
	- The `releases/3.0` branch is permanent and kept for future patches
	- Because the release was cut from a `releases/*` branch, a PR is automatically opened bumping `releases/3.0` to the next patch (e.g. `3.0.1` → `3.0.2`)

### Moving to a new major version

Moving onto a new major is a **cut**, not a release input. Run the **Cut
release** workflow (`cut.yml`) with `cut-type: major`; it creates
`releases/(X+1).0` at `(X+1).0.0-alpha.1` and opens the PR moving `main` to
`(X+1).0.0-nightly`. You then promote alpha/beta/rc/stable from that branch with
`release-type: versioned`; once `(X+1).0.0` ships stable, `main` is automatically
advanced to `(X+1).1.0-nightly`. See
[Cutting Release Lines](#cutting-release-lines-cutyml).

## Examples

### Example 1: Beta Release Series

```bash
# Initial state: main = 3.0.0-nightly

# Minor cut (cut.yml, cut-type: minor): creates releases/3.0 at 3.0.0-beta.1
# and opens the PR moving main -> 3.1.0-nightly. The main bump happens HERE,
# at cut time — not at stable-release time.
→ main becomes 3.1.0-nightly; releases/3.0 starts at 3.0.0-beta.1

# Release beta.1 (releases/3.0 Cargo.toml = 3.0.0-beta.1)
→ Release 3.0.0-beta.1 (from releases/3.0); main unchanged

# Release beta.2 (after bumping releases/3.0 to 3.0.0-beta.2)
→ Release 3.0.0-beta.2 (from releases/3.0); main unchanged

# Release stable (releases/3.0 Cargo.toml = 3.0.0)
→ Release 3.0.0 (from releases/3.0); main already at 3.1.0-nightly
→ latest set automatically (stable + no higher stable version published)
```

### Example 2: Patch Release Series

```bash
# Initial state: main = 3.1.0-nightly, v3.0.0 tag exists

# Create patch branch from tag (when preparing 3.0.1)
git checkout -b releases/3.0 v3.0.0
git push origin releases/3.0

# Fix #56 lands on main first
→ PR #56 merged to main

# Backport fix #56 individually
git checkout releases/3.0
git checkout -b backport/56-to-3.0
git cherry-pick <commit-from-main>
git push origin backport/56-to-3.0
# Create PR against releases/3.0, review, and merge

# Fix #57 lands on main
→ PR #57 merged to main

# Backport fix #57 individually
git checkout releases/3.0
git checkout -b backport/57-to-3.0
git cherry-pick <commit-from-main>
# Create PR, review, merge

# After all needed backports are merged
→ Release 3.0.1 (from releases/3.0, no main update)
→ Main stays: 3.1.0-nightly (already has fixes)

# More fixes for 3.0.2 (same branch releases/3.0, or create new from v3.0.1)
→ Fix #60 lands on main
→ Backport #60 to releases/3.0 (one PR)
→ Fix #61 lands on main
→ Backport #61 to releases/3.0 (one PR)

→ Release 3.0.2 (from releases/3.0, no main update)
→ Main stays: 3.1.0-nightly
```

### Example 3: Parallel Releases

```bash
# Main: 3.1.0-nightly
# Create branches from tags when needed: releases/3.0 from v3.0.0, releases/2.1 from v2.1.4

# Can release patches for older versions simultaneously:
→ Release 2.1.5 (from releases/2.1, branch created from v2.1.4)
→ Release 3.0.2 (from releases/3.0, branch created from v3.0.0)
→ Release 3.1.0-beta.1 (from releases/3.1)

# All independent, no conflicts
```

## Troubleshooting

### Workflow Fails on Branch Creation

**Problem**: A branch already exists from a previous run.

**Solution**: Re-runs are safe. The automated version-bump PR branch (`dev/ci/vX.Y.Z`) is deleted and recreated on each run. A `cut` that already pushed its release branch (`releases/X.Y`) then failed later is also safe to re-run: the cut reuses that branch **only** when it matches this exact cut (same base commit and pre-release version) and refuses if it looks like a different, pre-existing line. If manual intervention is ever needed:

```bash
# Delete the automated version bump PR branch
git push origin --delete dev/ci/vX.Y.Z

# Re-run the workflow
```

### Crate Publishing Fails

**Problem**: Some crates already published

**Solution**: The workflow automatically detects this and succeeds if all crates are published. If genuinely failed:

1. Check the error message for which crate failed
2. Manually publish if needed: `cargo publish -p <crate-name>`
3. Re-run the workflow (idempotent)

### PR Already Exists

**Problem**: The main-bump PR from a cut (`cut.yml`) already exists

**Solution**: The cut workflow updates existing PRs idempotently. No action needed.

### Wrong Version on Main

**Problem**: Main version wasn't updated correctly

**Solution**:

1. Manually create a PR to fix the version:
	```bash
	git checkout main
	git pull
	
	# Build list of surrealdb-* packages (auto-excludes surrealism-*)
	PACKAGES=$(cargo metadata --format-version 1 --no-deps | \
		jq -r '.packages[].name' | \
		grep '^surrealdb' | \
		sed 's/^/--package /' | \
		tr '\n' ' ')
	
	# Update only surrealdb packages
	cargo set-version $PACKAGES X.Y.Z-correct
	cargo update -p surrealdb -p surrealdb-core -p surrealdb-server
	git commit -am "chore: fix version to X.Y.Z-correct"
	git push origin HEAD:chore/fix-version
	# Create PR and merge
	```
	
	**Note**: The release scripts automatically exclude `surrealism-*` packages by only versioning packages with the `surrealdb-*` prefix.

### Need to Retry a Failed Release

**Problem**: A release failed partway through (e.g. `mirror-docker` failed after `promote` already created the GitHub release and copied binaries).

**Solution**: Use GitHub's **"Re-run failed jobs"** on the failed run. This reuses the successful `prepare-vars` (so the overwrite guard, which already passed, is not re-evaluated) and re-runs only the failed jobs — each of which is idempotent:
- crate publishing skips already-published crates
- `promote` updates the existing GitHub release and overwrites the (identical) binaries/pointers
- the Docker/downstream mirrors and region propagation re-tag/re-sync in place
- `prune` is a no-op for anything already deleted
- the bump/advance/cut PRs reuse or recreate their branch and edit-or-create their PR

**Caveat — starting a brand-new run:** a *fresh* `workflow_dispatch` (not "re-run failed jobs") of a version that was already partially published will be **stopped by the overwrite guard**, because the GitHub release or `download.surrealdb.com/vX.Y.Z/` prefix now exists. This is deliberate — it prevents silently clobbering a real release. To intentionally re-publish over it, dispatch again with `overwrite: true`. Nightly is exempt (it overwrites the `nightly` name by design). Prefer "re-run failed jobs" for ordinary retries.

## Architecture

### Workflow Jobs

```
prepare-vars (resolve SHA -> branch; wait for/verify rolling engine.json + downstream.json; compute version + latest)
	├─→ check-milestones (advisory: warn on unmerged + unbackported PRs in the vX.Y.Z milestone on both repos)
	├─→ approve (single human gate; needs check-milestones, so its name shows any warning)
	│      ↓  (every publish job below needs: approve)
	├─→ publish-crates (versioned releases; from the exact SHA)
	│      ↓
	├─→ promote (copy + rename binaries, pointers, GitHub release)
	│      ├─→ mirror-docker (ECR -> DockerHub, strip attestations)
	│      └─→ propagate (binaries to all regions)
	│             ↓
	│          ├─ bump-release-version (auto: versioned release from a releases/* branch)
	│          ├─ advance-main-version (auto: when a stable X.Y.0 ships and main still rides that line)
	│          ├─ update-homebrew-tap (auto: latest stable release)
	│          └─→ summarize-prs (clickable list of the follow-up PRs opened above)
	├─→ mirror-downstream (downstream side: ECR rolling image -> release tag + ACR)
	│      └─→ tag-downstream (pin the released engine sha on the downstream commit, push the tag)
	└─→ prune (delete superseded rolling artifacts per released side; runs whenever the released side(s) succeed)
	       └─→ prune-images (superseded ECR images: engine repo gated on engine side, downstream repo on downstream side)
```

The release builds and tests nothing — it promotes the rolling build for the
given `git-sha`. The release version is read from `Cargo.toml` at that commit; no
version bump happens during the release. When the release is cut from a
`releases/*` branch, the post-release `bump-release-version` job automatically
opens a PR moving that branch to its next version (next patch for a stable
release, next pre-release number otherwise). Separately, when a stable `X.Y.0`
ships while `main` still rides that line, `advance-main-version` opens a PR
moving `main` to `X.(Y+1).0-nightly`. On a successful latest-stable publish, the
`update-homebrew-tap` job opens a PR bumping the Homebrew formula — see
[Downstream release automation](#downstream-release-automation).
Finally, `summarize-prs` collects whichever of these follow-up PRs were actually
opened into a single clickable list in the run's job summary, so they are easy to
find, review and merge.

### Scripts

All multi-line bash logic is extracted to `.github/scripts/`:

- **`cut-release.sh`**: Cuts a new line from `main` — minor cut (`releases/X.Y` at `X.Y.0-beta.1` + main-bump PR) or major cut (`releases/(X+1).0` at `(X+1).0.0-alpha.1` + main-bump PR). Driven by `cut.yml`.
- **`bump-release-version.sh`**: Bumps the release branch to its next version (next patch for a stable release, next pre-release number otherwise) and opens a PR (reads the released version from the code)
- **`advance-main-after-release.sh`**: After a stable `X.Y.0` ships, advances `main` to `X.(Y+1).0-nightly` via a PR — but only if `main` still rides that line (the major-line case). A no-op otherwise.
- **`tag-downstream-release.sh`**: Pins the downstream repo's engine dependency to the released engine sha (rewrites the git selector to `rev`, re-locks only the engine crates, verifies), commits that pin on top of the built downstream commit, and pushes a reproducible `vX.Y.Z` tag. Driven by `release.yml`'s `tag-downstream` job. See [Downstream reproducibility tags](#downstream-reproducibility-tags).
- **`compute-nightly-version.sh`**: Computes nightly version from main branch

### Key Features

1. **Idempotency**: All operations handle re-runs gracefully
2. **Validation**: Comprehensive input validation before execution
3. **Dry-Run**: Test entire workflow without publishing
4. **Version from code**: The release version is whatever is committed on the `git-ref`
5. **Version Logic**: Smart auto-calculation with manual override for the main branch bump
6. **Nightly Builds**: Version derived from main, no code changes needed

## Additional Resources

- [GitHub Actions Workflow](../.github/workflows/release.yml)
- [Build Documentation](BUILDING.md)
- [Contributing Guidelines](../CONTRIBUTING.md)
