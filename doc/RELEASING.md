# Release Process Documentation

This document describes the SurrealDB release workflow, including how to perform releases, the branching strategy, and version management.

## Table of Contents

- [Overview](#overview)
- [Rolling Builds](#rolling-builds)
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

The workflow is **fully idempotent**, meaning you can safely retry any release without errors or duplicate resources.

> For per-release operational notes that administrators should read before
> deploying a new build (wire-format rotations, removed features, etc.),
> see [`UPGRADING.md`](./UPGRADING.md).

## Rolling Builds

Separately from the versioned/nightly release workflow described below,
`rolling-build.yml` runs on every push to `main` and to `releases/*` branches.
It builds the full binary matrix and Docker images for that commit and publishes
them by commit SHA — binaries to `s3://download.surrealdb.com/rolling/<sha>/`
and images to `surrealdb/surrealdb:rolling-<sha>` plus a moving
`surrealdb/surrealdb:<branch-slug>` tag (e.g. `:main`, `:releases-3-1`).

Rolling builds do **not** publish crates (crates.io versions are write-once) and
do **not** re-run the CI quality/test matrix — correctness is gated by `ci.yml`
on the same commit. The baked binary version comes from `Cargo.toml` plus build
metadata, so a `releases/*` commit already carries its intended release version.
Every job is guarded by `github.repository`, so the workflow never runs in the
public mirror.

Official releases are still cut via the versioned workflow documented below; the
tag-to-promote model that re-tags these rolling artifacts is being introduced
incrementally.

## Release Types

### Nightly Releases

- **Purpose**: Daily development builds for testing latest features
- **Trigger**: Automatically at midnight UTC, or manually via workflow dispatch
- **Version Format**: Derived from main's version, which is always `X.Y.0-nightly` (e.g. `3.2.0-nightly`), plus build metadata
- **Artifacts**: Binaries, Docker images (tagged with `nightly`)
- **No**: Crate publishing, Git tags, GitHub releases, or main branch updates

**When to use**: Never manually trigger unless testing the nightly pipeline.

### Versioned Releases

Versioned releases come in several flavors:

#### Pre-Release (Alpha/Beta/RC)

- **Format**: `X.Y.Z-<prerelease>.<patch>` (e.g., `3.0.0-beta.1`, `3.1.0-alpha.2`)
- **Source**: Cut from the release branch for that line (e.g. `releases/3.0`)
- **Main Branch**: Not updated (main always stays on its `-nightly` development version)
- **Use Case**: Feature testing, early adopter releases

#### Stable Release (X.Y.0)

- **Format**: `X.Y.0` (e.g., `3.0.0`, `4.0.0`)
- **Source**: Cut from the release branch for that line (e.g. `releases/3.0`)
- **Main Branch**: Bumped to next minor nightly: `X.(Y+1).0-nightly` (via `update-main`)
	- Example: Release `3.0.0` → Main becomes `3.1.0-nightly`
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
- **Note**: Determines which other inputs are relevant

#### `git-ref`
- **Type**: String
- **Description**: The git ref (branch/tag/commit) to build from. The release version is taken directly from `Cargo.toml` on this ref - there is no separate version input.
- **Default**: `main`
- **Examples**:
	- `main` - for nightly builds (`main` always carries the `-nightly` version)
	- `releases/3.1` - permanent release branch, for pre-releases, stable, and patch releases

### Optional Inputs

#### `publish`
- **Type**: Boolean
- **Description**: Publish the release (false for dry-run)
- **Default**: `false`
- **When to use**: After verifying dry-run succeeds

#### `latest`
- **Type**: Boolean
- **Description**: Mark as latest release
- **Default**: `false`
- **When to use**: For the most recent stable release only
- **Note**: Not applicable for nightly releases

#### `update-main`
- **Type**: Boolean
- **Description**: Update main branch version after release
- **Default**: `false`
- **When to use**: For stable `X.Y.0` releases (bumps `main` to the next minor nightly)
- **Note**: Not applicable for pre-releases, patch releases, or nightly releases (these never change `main`)

#### `main-version`
- **Type**: String
- **Description**: Override auto-calculated main version
- **Default**: Auto-calculated based on release type
- **When to use**: Only when bumping to next major version (e.g., `4.0.0-nightly`)
- **Example**: Release `3.5.0`, but set main to `4.0.0-nightly` instead of `3.6.0-nightly`

#### `extra-features`
- **Type**: String
- **Description**: Extra features enabled in the binary
- **Default**: `storage-tikv,jwks,ml`
- **When to use**: For custom feature combinations

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

1. **For pre-releases and stable X.Y.0 releases**:
	- Build directly from the `main` branch
	- The version is read from `Cargo.toml` on `main`

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
Timeline:
┌─────────────┬───────────────┬─────────────┬───────────────┐
│ Release     │ Main Before   │ Release     │ Main After    │
├─────────────┼───────────────┼─────────────┼───────────────┤
│ 3.0.0-beta.1│ 3.0.0-nightly │ 3.0.0-beta.1│ 3.0.0-nightly │ (no change)
│ 3.0.0-beta.2│ 3.0.0-nightly │ 3.0.0-beta.2│ 3.0.0-nightly │ (no change)
│ 3.0.0       │ 3.0.0-nightly │ 3.0.0       │ 3.1.0-nightly │
│ 3.0.1       │ 3.1.0-nightly │ 3.0.1       │ 3.1.0-nightly │ (no change)
│ 3.1.0       │ 3.1.0-nightly │ 3.1.0       │ 3.2.0-nightly │
└─────────────┴───────────────┴─────────────┴───────────────┘
```

`main` always carries a `-nightly` version; it never holds a `-beta`/`-rc` or a
stable version. Only a stable `X.Y.0` release moves `main` forward to the next
minor nightly. Pre-releases and patches are cut from release branches and leave
`main` untouched.

### Auto-Calculation Rules

The workflow only ever moves `main` between `-nightly` versions:

1. **Stable X.Y.0**: Bump to next minor nightly
	- `3.0.0` → Main: `3.1.0-nightly`
	- `3.5.0` → Main: `3.6.0-nightly`

2. **Pre-release or patch X.Y.Z** (Z > 0): No change to main
	- `3.0.0-beta.1` → Main: unchanged (stays on `-nightly`)
	- `3.0.1` → Main: unchanged (stays on `-nightly`)

### Manual Override

Use `main-version` input when transitioning to next major version:

```
Release: 3.5.0
Auto: 3.6.0-nightly
Override: 4.0.0-nightly
```

## Quick Start

### Performing a Dry-Run

Always test with a dry-run first:

1. Go to **Actions** → **Release** → **Run workflow**
2. Select inputs:
	- Release type: `versioned`
	- Git ref: `releases/3.1` (a release branch; the version is read from `Cargo.toml` on this ref)
	- Update main: `false`
	- Publish: `false` ← **Leave unchecked for dry-run**
3. Click **Run workflow**
4. Verify all jobs succeed

### Publishing the Release

After successful dry-run:

1. **Run workflow again** with same inputs
2. **Check "Publish"** checkbox
3. Click **Run workflow**
4. Monitor the release
5. Merge any version-bump PR opened by the workflow (the `main` bump for a stable `X.Y.0`, or the release-branch patch/pre-release bump)

## Step-by-Step Instructions

### Pre-Release (Alpha/Beta/RC)

**Example**: Releasing `3.0.0-beta.2` (version `3.0.0-beta.2` in the release branch's `Cargo.toml`)

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Update main: ✗
	Publish: ✗
	```
	→ Verify dry-run succeeds

2. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Update main: ✗
	Latest: ✗
	Publish: ✓
	```

3. **Post-Release**:
	- No `main` branch update for pre-releases — `main` stays on its `-nightly` version
	- A PR is automatically opened to bump the release branch to its next pre-release (e.g. `3.0.0-beta.2` → `3.0.0-beta.3`)

### Stable Release (X.Y.0)

**Example**: Releasing `3.0.0` (version `3.0.0` in the release branch's `Cargo.toml`)

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Update main: ✓
	Publish: ✗
	```

2. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Update main: ✓
	Latest: ✓  ← Mark as latest
	Publish: ✓
	```

3. **Post-Release**:
	- Merge PR "Bump version to 3.1.0-nightly" (opened against `main` by `update-main`)
	- Main branch now at `3.1.0-nightly`

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
	Update main: ✗  ← Don't update main for patches
	Publish: ✗
	```

7. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.0
	Update main: ✗
	Latest: ✓  ← If this is now the latest stable
	Publish: ✓
	```

8. **Post-Release**:
	- No main branch update (fixes already on main)
	- Main remains at `3.1.0-nightly`
	- The `releases/3.0` branch is permanent and kept for future patches
	- Because the release was cut from a `releases/*` branch, a PR is automatically opened bumping `releases/3.0` to the next patch (e.g. `3.0.1` → `3.0.2`)

### Major Version Bump

**Example**: Releasing `3.5.0` but moving to `4.0.0-nightly` on main

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: releases/3.5
	Update main: ✓
	Main version: 4.0.0-nightly  ← Override
	Publish: ✗
	```

2. **Publish**:
	```
	Release type: versioned
	Git ref: releases/3.5
	Update main: ✓
	Main version: 4.0.0-nightly
	Latest: ✓
	Publish: ✓
	```

3. **Post-Release**:
	- Merge PR "Bump version to 4.0.0-nightly"
	- Main branch now at `4.0.0-nightly`

## Examples

### Example 1: Beta Release Series

```bash
# Initial state: main = 3.0.0-nightly; release work happens on releases/3.0

# Release beta.1 (releases/3.0 Cargo.toml = 3.0.0-beta.1)
→ Release 3.0.0-beta.1 (from releases/3.0)
→ Main unchanged (stays 3.0.0-nightly)

# Release beta.2
→ Release 3.0.0-beta.2 (from releases/3.0)
→ Main unchanged (stays 3.0.0-nightly)

# Release stable (releases/3.0 Cargo.toml = 3.0.0, update main)
→ Release 3.0.0 (from releases/3.0, update main)
→ Main becomes: 3.1.0-nightly
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

**Problem**: Branch already exists from previous run

**Solution**: The workflow is idempotent and automatically deletes/recreates branches. If manual intervention is needed:

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

**Problem**: PR to update main already exists

**Solution**: The workflow automatically updates existing PRs. No action needed.

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

**Problem**: Release failed partway through

**Solution**: Just re-run the workflow with the same inputs. The workflow is fully idempotent and will:
- Delete and recreate branches
- Reuse existing PRs
- Skip already-published crates
- Update existing GitHub releases

## Architecture

### Workflow Jobs

```
validate-inputs
	├─→ update-main (if update-main=true, versioned only)
	↓
prepare-vars ←────────────────┐
	↓                         │
	├─→ build (Linux/macOS/Windows)
	├─→ publish-crates        │
	│    ↓                    │
	├─→ publish (binaries)    │
	│    ↓                    │
	├─→ docker                │
	├─→ package-macos         │
	│    ↓                    │
	└─→ propagate ────────────┘
	     ↓
	bump-release-version (auto: versioned release from a releases/* branch)
```

The release version is read from `Cargo.toml` on the provided `git-ref`; no
version bump happens during the release. When the release is cut from a
`releases/*` branch, the post-release `bump-release-version` job automatically
opens a PR moving that branch to its next patch version.

### Scripts

All multi-line bash logic is extracted to `.github/scripts/`:

- **`bump-nightly-version.sh`**: Updates main branch version and creates PR (reads the released version from the code)
- **`bump-release-version.sh`**: Bumps the release branch to the next patch version and creates PR (reads the released version from the code)
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
