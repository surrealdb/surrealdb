# Release Process Documentation

This document describes the SurrealDB release workflow, including how to perform releases, the branching strategy, and version management.

## Table of Contents

- [Overview](#overview)
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

## Release Types

### Nightly Releases

- **Purpose**: Daily development builds for testing latest features
- **Trigger**: Automatically at midnight UTC, or manually via workflow dispatch
- **Version Format**:
	- If main is pre-release (e.g., `3.0.0-beta`): `3.0.0-nightly`
	- If main is stable (e.g., `3.0.0`): `3.1.0-nightly`
- **Artifacts**: Binaries, Docker images (tagged with `nightly`)
- **No**: Crate publishing, Git tags, GitHub releases, or main branch updates

**When to use**: Never manually trigger unless testing the nightly pipeline.

### Versioned Releases

Versioned releases come in several flavors:

#### Pre-Release (Alpha/Beta/RC)

- **Format**: `X.Y.Z-<prerelease>.<patch>` (e.g., `3.0.0-beta.1`, `3.1.0-alpha.2`)
- **Main Branch**: Updated to `X.Y.Z-<prerelease>` (patch stripped)
	- Example: Release `3.0.0-beta.2` → Main becomes `3.0.0-beta`
- **Use Case**: Feature testing, early adopter releases

#### Stable Release (X.Y.0)

- **Format**: `X.Y.0` (e.g., `3.0.0`, `4.0.0`)
- **Main Branch**: Bumped to next minor alpha: `X.(Y+1).0-alpha`
	- Example: Release `3.0.0` → Main becomes `3.1.0-alpha`
- **Patch Branch**: Creates long-lived `release/X.Y` branch for future patches
	- Example: Release `3.0.0` → Creates `release/3.0` branch
- **Use Case**: Major feature releases, production-ready versions

#### Patch Release (X.Y.Z where Z > 0)

- **Format**: `X.Y.Z` (e.g., `3.0.1`, `3.0.2`)
- **Source Branch**: Released from `release/X.Y` patch branch
- **Main Branch**: Not updated (remains on next minor alpha)
- **Use Case**: Bug fixes, security patches

## Workflow Inputs

### Required Inputs

#### `release-type`
- **Type**: Choice (`nightly` or `versioned`)
- **Description**: Type of release to perform
- **Default**: `nightly`
- **Note**: Determines which other inputs are available

#### `git-ref`
- **Type**: String
- **Description**: The git ref (branch/tag/commit) to build from
- **Default**: `main`
- **Examples**:
	- `main` - for pre-releases and new stable releases
	- `release/3.0` - for patch releases

#### `release-version`
- **Type**: String
- **Description**: **[Versioned only]** Semantic version for the release
- **Format**: Must be valid semver (e.g., `3.0.0`, `3.0.0-beta.1`)
- **Validation**: Automatically validated by workflow

### Optional Inputs

#### `update-main`
- **Type**: Boolean
- **Description**: **[Versioned only]** Create PR to update main branch version
- **Default**: `false`
- **When to use**: Always `true` for releases from main branch

#### `main-version`
- **Type**: String
- **Description**: **[Versioned only]** Override the auto-calculated main branch version
- **Default**: Auto-calculated based on release type
- **When to use**: Only when bumping to next major version (e.g., `4.0.0-alpha`)
- **Example**: Release `3.5.0`, but set main to `4.0.0-alpha` instead of `3.6.0-alpha`

#### `latest`
- **Type**: Boolean
- **Description**: **[Versioned only]** Mark this release as "latest"
- **Default**: `false`
- **When to use**: For the most recent stable release only

#### `publish`
- **Type**: Boolean
- **Description**: Actually publish artifacts (false for dry-run)
- **Default**: `false`
- **When to use**: After verifying dry-run succeeds

## Branching Strategy

### Long-Lived Branches

```
main (3.1.0-alpha)
├── release/3.0 (for 3.0.x patches)
├── release/2.1 (for 2.1.x patches)
└── release/2.0 (for 2.0.x patches)
```

### Temporary Branches

```
release/vX.Y.Z             # Created during version bump, deleted after release
chore/bump-main-to-vX.Y.Z  # Created for main version PR
backport/<issue>-to-X.Y    # Created for backporting individual fixes (one per fix)
                           # Example: backport/56-to-3.0, backport/57-to-3.0
```

### Branch Lifecycle

1. **For pre-releases and stable X.Y.0 releases**:
	- Build from `main` branch
	- Temporary `release/vX.Y.Z` branch created for version bump
	- After release, `release/vX.Y.Z` is deleted

2. **For stable X.Y.0 releases**:
	- Long-lived `release/X.Y` branch created for future patches
	- Remains permanently for patch releases

3. **For patch releases (X.Y.Z where Z > 0)**:
	- All fixes must land on `main` first
	- For each fix to backport, create individual backport PR (e.g., `backport/56-to-3.0`)
	- Cherry-pick specific fix from main to backport branch
	- Review and merge backport PR into `release/X.Y`
	- Repeat for each fix (one PR per fix)
	- After all backport PRs are merged, build from `release/X.Y` branch
	- No new long-lived branches created

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

# 2. Create individual backport PR
git checkout release/3.0
git checkout -b backport/56-to-3.0
git cherry-pick abc123  # commit from main
git push origin backport/56-to-3.0
gh pr create --base release/3.0 --head backport/56-to-3.0

# 3. Review and merge backport PR
# (CI runs, code review happens)
Backport PR merged → release/3.0 now has the fix

# 4. Repeat for each fix needed in 3.0.1

# 5. When ready, run release workflow
Release 3.0.1 from release/3.0 branch
```

## Version Management

### Main Branch Version Evolution

The version on the `main` branch reflects the **next development target**, not the current release:

```
Timeline:
┌─────────────┬─────────────┬─────────────┬─────────────┐
│ Release     │ Main Before │ Release     │ Main After  │
├─────────────┼─────────────┼─────────────┼─────────────┤
│ 3.0.0-beta.1│ 3.0.0-alpha │ 3.0.0-beta.1│ 3.0.0-beta  │
│ 3.0.0-beta.2│ 3.0.0-beta  │ 3.0.0-beta.2│ 3.0.0-beta  │
│ 3.0.0       │ 3.0.0-beta  │ 3.0.0       │ 3.1.0-alpha │
│ 3.0.1       │ 3.1.0-alpha │ 3.0.1       │ 3.1.0-alpha │ (no change)
│ 3.1.0       │ 3.1.0-alpha │ 3.1.0       │ 3.2.0-alpha │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

### Auto-Calculation Rules

The workflow automatically determines the next main version:

1. **Pre-release** (contains `-`): Strip patch number
	- `3.0.0-beta.1` → Main: `3.0.0-beta`
	- `3.0.0-rc.3` → Main: `3.0.0-rc`

2. **Stable X.Y.0**: Bump to next minor alpha
	- `3.0.0` → Main: `3.1.0-alpha`
	- `3.5.0` → Main: `3.6.0-alpha`

3. **Patch X.Y.Z** (Z > 0): No change to main
	- `3.0.1` → Main: unchanged (`3.1.0-alpha`)

### Manual Override

Use `main-version` input when transitioning to next major version:

```
Release: 3.5.0
Auto: 3.6.0-alpha
Override: 4.0.0-alpha
```

## Quick Start

### Performing a Dry-Run

Always test with a dry-run first:

1. Go to **Actions** → **Release** → **Run workflow**
2. Select inputs:
	- Release type: `versioned`
	- Git ref: `main`
	- Release version: `3.0.0-beta.2`
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
5. Merge the PR to update main branch

## Step-by-Step Instructions

### Pre-Release (Alpha/Beta/RC)

**Example**: Releasing `3.0.0-beta.2`

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: main
	Release version: 3.0.0-beta.2
	Update main: ✗
	Publish: ✗
	```
	→ Verify dry-run succeeds

2. **Publish**:
	```
	Release type: versioned
	Git ref: main
	Release version: 3.0.0-beta.2
	Update main: ✗
	Latest: ✗
	Publish: ✓
	```

3. **Post-Release**:
	- Merge PR "Bump version to 3.0.0-beta"
	- Main branch now at `3.0.0-beta`

### Stable Release (X.Y.0)

**Example**: Releasing `3.0.0`

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: main
	Release version: 3.0.0
	Update main: ✓
	Publish: ✗
	```

2. **Publish**:
	```
	Release type: versioned
	Git ref: main
	Release version: 3.0.0
	Update main: ✓
	Latest: ✓  ← Mark as latest
	Publish: ✓
	```

3. **Post-Release**:
	- Merge PR "Bump version to 3.1.0-alpha"
	- Main branch now at `3.1.0-alpha`
	- Long-lived branch `release/3.0` created for patches

### Patch Release (X.Y.Z)

**Example**: Releasing `3.0.1`

**Important**: All fixes must land on `main` first, then be backported one fix at a time.

1. **Ensure fixes are merged to main**:
	```bash
	# All bug fixes should already be merged to main branch
	# Example: Fix #56 has been merged to main as commit abc123
	```

2. **Backport each fix individually** (one PR per fix):
	```bash
	# For fix #56
	git checkout release/3.0
	git pull origin release/3.0

	# Create a backport branch for this specific fix
	git checkout -b backport/56-to-3.0

	# Cherry-pick the specific fix from main
	git cherry-pick <commit-hash-from-main>

	# Push the backport branch
	git push origin backport/56-to-3.0

	# Create PR targeting release/3.0
	gh pr create --base release/3.0 --head backport/56-to-3.0 \
		--title "Backport #56 to release/3.0" \
		--body "Backports fix #56 from main for 3.0.1 release.

	Original PR: #56
	Original commit: <commit-hash>"
	```

3. **Repeat step 2** for each fix that needs backporting (e.g., fix #57, #58, etc.)
	- One backport PR per fix
	- Review and merge each PR individually

4. **After all backport PRs are merged**, proceed with release

5. **Dry-Run**:
	```
	Release type: versioned
	Git ref: release/3.0  ← Use patch branch
	Release version: 3.0.1
	Update main: ✗  ← Don't update main for patches
	Publish: ✗
	```

4. **Publish**:
	```
	Release type: versioned
	Git ref: release/3.0
	Release version: 3.0.1
	Update main: ✗
	Latest: ✓  ← If this is now the latest stable
	Publish: ✓
	```

5. **Post-Release**:
	- No main branch update (fixes already on main)
	- Main remains at `3.1.0-alpha`

### Major Version Bump

**Example**: Releasing `3.5.0` but moving to `4.0.0-alpha` on main

1. **Dry-Run**:
	```
	Release type: versioned
	Git ref: main
	Release version: 3.5.0
	Update main: ✓
	Main version: 4.0.0-alpha  ← Override
	Publish: ✗
	```

2. **Publish**:
	```
	Release type: versioned
	Git ref: main
	Release version: 3.5.0
	Update main: ✓
	Main version: 4.0.0-alpha
	Latest: ✓
	Publish: ✓
	```

3. **Post-Release**:
	- Merge PR "Bump version to 4.0.0-alpha"
	- Main branch now at `4.0.0-alpha`

## Examples

### Example 1: Beta Release Series

```bash
# Initial state: main = 3.0.0-alpha

# Release beta.1
→ Release 3.0.0-beta.1 (from main, update main)
→ Main becomes: 3.0.0-beta

# Release beta.2
→ Release 3.0.0-beta.2 (from main, update main)
→ Main stays: 3.0.0-beta (idempotent)

# Release stable
→ Release 3.0.0 (from main, update main)
→ Main becomes: 3.1.0-alpha
→ Creates: release/3.0 branch
```

### Example 2: Patch Release Series

```bash
# Initial state: main = 3.1.0-alpha, release/3.0 exists

# Fix #56 lands on main first
→ PR #56 merged to main

# Backport fix #56 individually
git checkout release/3.0
git checkout -b backport/56-to-3.0
git cherry-pick <commit-from-main>
git push origin backport/56-to-3.0
# Create PR against release/3.0, review, and merge

# Fix #57 lands on main
→ PR #57 merged to main

# Backport fix #57 individually
git checkout release/3.0
git checkout -b backport/57-to-3.0
git cherry-pick <commit-from-main>
# Create PR, review, merge

# After all needed backports are merged
→ Release 3.0.1 (from release/3.0, no main update)
→ Main stays: 3.1.0-alpha (already has fixes)

# More fixes for 3.0.2
→ Fix #60 lands on main
→ Backport #60 to release/3.0 (one PR)
→ Fix #61 lands on main
→ Backport #61 to release/3.0 (one PR)

→ Release 3.0.2 (from release/3.0, no main update)
→ Main stays: 3.1.0-alpha
```

### Example 3: Parallel Releases

```bash
# Main: 3.1.0-alpha
# release/3.0: exists
# release/2.1: exists

# Can release patches for older versions simultaneously:
→ Release 2.1.5 (from release/2.1)
→ Release 3.0.2 (from release/3.0)
→ Release 3.1.0-beta.1 (from main)

# All independent, no conflicts
```

## Troubleshooting

### Workflow Fails on Branch Creation

**Problem**: Branch already exists from previous run

**Solution**: The workflow is idempotent and automatically deletes/recreates branches. If manual intervention is needed:

```bash
# Delete the temporary release branch
git push origin --delete release/vX.Y.Z

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
	cargo set-version --workspace X.Y.Z-correct
	cargo update -p surrealdb -p surrealdb-core -p surrealdb-server
	git commit -am "chore: fix version to X.Y.Z-correct"
	git push origin HEAD:chore/fix-version
	# Create PR and merge
	```

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
	↓
bump-version (versioned only)
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
	create-patch-branch (stable X.Y.0 only)
	update-main (if update-main=true)
	cleanup-release-branch
```

### Scripts

All multi-line bash logic is extracted to `.github/scripts/`:

- **`bump-version.sh`**: Creates release branch and bumps version
- **`create-patch-branch.sh`**: Creates long-lived patch branch for stable releases
- **`update-main-version.sh`**: Updates main branch version and creates PR
- **`compute-nightly-version.sh`**: Computes nightly version from main branch

### Key Features

1. **Idempotency**: All operations handle re-runs gracefully
2. **Validation**: Comprehensive input validation before execution
3. **Dry-Run**: Test entire workflow without publishing
4. **Branching**: Automatic management of temporary and long-lived branches
5. **Version Logic**: Smart auto-calculation with manual override
6. **Nightly Builds**: Version derived from main, no code changes needed

## Additional Resources

- [GitHub Actions Workflow](../.github/workflows/release.yml)
- [Build Documentation](BUILDING.md)
- [Contributing Guidelines](../CONTRIBUTING.md)
