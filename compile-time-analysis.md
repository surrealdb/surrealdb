# Compile-time analysis — perf/compile-time-20260528-0210

Cold and incremental build profiles for the surrealdb-private workspace on
macOS (aarch64-apple-darwin), default-features `cargo build` (the dev
default command-line entry: allocator, allocation-tracking, storage-mem,
storage-surrealkv, storage-rocksdb, storage-tikv, scripting, http,
surrealism, graphql, mcp, cli).

## Baseline

| Scenario | Wall-clock |
|---|---:|
| Cold full `cargo build` | **165s** |
| Incremental rebuild after `touch surrealdb/core/src/lib.rs` | **19.6s** |

Per-target longest tails (cold, parsed from `cargo --timings` HTML):

| Crate | self | frontend | codegen | Notes |
|---|---:|---:|---:|---|
| `surrealdb-core` | 107.1s | 72.6s | 34.5s | Single biggest blocker |
| `aws-lc-sys` (build script) | 34.3s | — | — | C library build, parallel with core |
| `surrealdb-librocksdb-sys` (build script) | 25.1s | — | — | C++ librocksdb build — on critical path |
| `surrealdb-server` | 23.6s | 14.4s | 9.1s | Downstream of core |
| `wasm-opt-sys` (build script) | 19.7s | — | — | Binaryen C++ build, surrealism only |
| `surrealdb` (facade) | 12.1s | 3.6s | 8.5s | Downstream of core |
| `wasmtime` | 6.4s | — | — | surrealism only |
| `surrealdb-mcp` | 8.1s | 2.8s | 5.3s | Downstream of core |

## Cold-build critical path

```
vcpkg → libz-sys (5s) → surrealdb-librocksdb-sys build script (25.1s)
       → surrealdb-librocksdb-sys (1.4s) → surrealdb-rocksdb (0.1s)
       → surrealdb-core (107.1s) → surreal bin (6.8s)        ≈ 165s
```

The critical path is entirely **librocksdb-sys (25s C++ build) →
surrealdb-core (107s rustc) → surreal bin link**. Everything else (aws-lc-sys,
wasm-opt-sys, wasmtime, console-subscriber, etc.) parallelises with these
three and is *not* on the critical path on this hardware.

## Inside surrealdb-core — `rustc -Ztime-passes`

Cold (no incremental cache), 71.5s total rustc wall-clock for the crate:

| Pass | Time |
|---|---:|
| codegen_to_LLVM_IR | 20.0s |
| type_check_crate | 19.1s |
| LLVM_passes | 19.0s |
| generate_crate_metadata | 15.4s |
| coherence_checking | 14.1s |
| monomorphization_collector_graph_walk | 13.3s |
| MIR_borrow_checking | 11.2s |
| macro_expand_crate | **1.5s** |

Incremental rebuild (touch lib.rs), 16.3s rustc wall-clock:

| Pass | Time |
|---|---:|
| macro_expand_crate | 3.3s |
| generate_crate_metadata | 2.9s |
| serialize_dep_graph | 2.7s |
| link | 1.7s |

The crate is ~298k LOC across 1090 .rs files and is the leaf most
developers edit. Cost is dominated by *raw code volume*: type/borrow
check, codegen, LLVM, monomorphization, and coherence each cost
11–20s and scale with size + trait-impl count, not with any single
proc-macro.

## Ranked bottleneck hypotheses

### 1. surrealdb-core is monolithic (107s / 14s incremental)

**Evidence.** It is the sole rustc invocation on the critical path
after librocksdb-sys. macro_expand_crate is only 1.5s of the 71s
cold rustc — so proc-macros are *not* the cost. The cost is the
volume of code itself: 1109 `Clone` derives, 743 `PartialEq`, 261
`storekey::Encode`/`BorrowDecode` derives, 319 `#[instrument]`
attributes, ~50 `phf!` macro invocations, 124 `compat_test!`
invocations, and ~98k impl blocks worth of generated trait code.

**Fix shape.** The only fix shape with >10% impact would be splitting
surrealdb-core into multiple crates (parser, expr, kvs, idx, exec,
gql separable). That is **out of scope** per this routine
("Splitting an existing crate into multiple crates").

### 2. librocksdb-sys build script (25s on critical path)

**Evidence.** Sits in front of surrealdb-core in the dependency
order because surrealdb-core depends on `rocksdb`. The 25s is
compiling C++ rocksdb sources via the bundled `static` feature
(building rocksdb.a). Bindgen runs in the same script.

**Fix shape.** Would need to ship pre-generated bindings or
switch off the static build. Neither is a single-PR change
inside this workspace — `surrealdb-librocksdb-sys` is an external
fork. **Out of scope.**

### 3. Duplicate dep versions

Workspace has duplicate versions of: `hashbrown` (5), `itertools` (5),
`thiserror` (4), `rand_core` (4), `rand` (4), `phf_shared` (4),
`winnow` (3), `wasm-encoder` (3), `wasmparser` (3), `getrandom` (3),
plus ~20 doubles. Each duplicate compiles independently.

**Fix shape.** Picking off any one of these would require either
updating a transitive dep we don't directly own (the witx/wiggle
tree still pins thiserror v1) or pinning workspace deps to a single
version that all transitive users accept. The biggest offender
(`thiserror v1`) is pinned by wasmtime-wasi's witx/wiggle chain
which we can't update unilaterally. Estimated upside per
deduplicated crate: 0.3–1s self-time of one extra rustc invocation,
running in parallel — usually 0s of wall-clock win.

### 4. console-subscriber feature-gate

Identified as a follow-up in the prior compile-time PR
([surrealdb-private#269](https://github.com/surrealdb/surrealdb-private/pull/269)).

**Evidence on this hardware.** console-subscriber + its unique deps
(console-api, hdrhistogram, crossbeam-channel, hyper-util) total
0.8s of CPU time, all parallelising with other work. Console
support is already runtime-gated by `SURREAL_TOKIO_CONSOLE_ENABLED`
(default false). Feature-gating compilation would save ~0s of
wall-clock on this build. (The prior PR's "2.5 min" estimate was
based on a different machine / build profile.)

**Conclusion.** Not worth a PR's churn on the current data.

### 5. `#[instrument]` density (319 attributes in surrealdb-core)

**Evidence.** Each `#[tracing::instrument]` expands to a span
guard around the function body. macro_expand_crate cold-build is
1.5s total — so the expansion itself is cheap. Generated code does
add a few lines per fn that flow through type check / codegen, but
distinguishing that contribution from the rest of the crate's
volume is not possible from cargo --timings alone.

**Fix shape.** Replacing with hand-written `let _span = span!(...).entered();`
would touch 319 sites with measurable behavioural risk
(observability surface change) and uncertain payoff. Out of scope
under "Anything that changes runtime behavior" — instrument output
*is* runtime behavior we ship.

### 6. Misplaced dev-only dependencies

The prior compile-time PR moved `rstest` from `[dependencies]` to
`[dev-dependencies]` in `surrealdb-types`. Scanning every workspace
`Cargo.toml` for similar misplacements (rstest, criterion, pprof,
wiremock, test-log, serial_test, env_logger, temp-dir, paste,
mockito, httpmock, proptest, quickcheck): **no remaining
misplacements found**. The `pprof` entry in `surrealdb-server`'s
`[dependencies]` is correctly behind the optional
`performance-profiler` feature.

## Decision

The dominant 65% of cold-build wall-clock (107s/165s) sits in a
single rustc invocation of `surrealdb-core`. Reducing it
materially needs either crate-splitting or a sweeping proc-macro
substitution across hundreds of sites — both **out of scope** for
this routine.

The remaining bottlenecks (librocksdb-sys build script, duplicate
deps, console-subscriber, instrument density) each fail at least one
of: ≥10% wall-clock impact, in-scope per task, no design discussion
required.

**Per the routine's stop criteria, this run surfaces analysis only
and does not land a fix.** Recommended followups, none small enough
to ship in a focused PR without prior design alignment:

- Discuss splitting `surrealdb-core` (likely 3–5 crates: parser/expr,
  kvs, idx/exec, gql, mcp/scripting bindings). Cold-build wall-clock
  would drop to whichever sub-crate becomes the new critical path,
  and incremental edits inside one sub-crate would skip the others.
- Coordinate a workspace-wide thiserror v2 unification (witx/wiggle
  blocker on the v1 holdouts).
- Look at the surrealdb-librocksdb-sys fork to see whether
  pre-generated bindings + a `bindgen-static` feature can be added,
  and whether the C++ build can be split or cached more aggressively.

## Subsequent scheduled-run verification — 2026-05-28 22:05 BST

Second invocation of this scheduled task fired ~16h after the
analysis above was produced. Re-checked whether anything has shifted
that would change the conclusion:

- New main commits since 05:52: `e0f306862` (sdk replay-log fix, 152
  lines in the `surrealdb` facade) and `e2ab333f7` (rpc cancellation,
  +1154 lines concentrated in `surrealdb/server/src/rpc/websocket.rs`).
  Both land outside `surrealdb-core`; neither shifts the dominant
  bottleneck (107s of 165s cold wall-clock).
- Widened the dev-dep misplacement scan to add `insta`, `tempfile`,
  `pretty_assertions`, `assert_cmd`, `assert_fs`, `trycmd`, `rexpect`,
  `test-strategy`, `fake`, `trybuild`, `expect-test`, `test-case`,
  `rstest_reuse`, `ctor`, `datatest-stable`. Only `tempfile` shows up
  in `[dependencies]` of `surrealdb-server`, `surrealism/runtime`, and
  feature-gated in `surrealdb-core` — all are legitimate production
  uses (`surrealdb/server/src/cli/upgrade.rs:165`,
  `surrealdb/server/src/cli/module/build.rs:11`,
  `surrealism/runtime/src/package.rs:10`). No new misplacements.
- `compat_test!` in `surrealdb/core/src/catalog/compat/tests.rs`
  (124 invocations from the prior analysis) expands `paste::paste!`
  into `#[test] fn` items; `#[test]` strips the items in non-test
  builds before type-check / codegen, so the only non-test cost is
  the paste expansion itself — included in the 1.5s
  `macro_expand_crate` budget already accounted for.
- Top-level `[features].default` (allocator, allocation-tracking,
  storage-mem, storage-surrealkv, storage-rocksdb, storage-tikv,
  scripting, http, surrealism, graphql, mcp, cli) is the
  binary-release configuration. Trimming default features is a
  behaviour change on `cargo build` — out of scope per the routine.

Conclusion unchanged: no in-scope fix offers ≥10% wall-clock impact.
Per the routine's stop criteria this run surfaces verification only;
no fix landed, no PR opened, no Slack notification posted.

## Third scheduled-run — 2026-05-29 21:00 BST — fix landed

The two prior runs ranked bottlenecks by *which crate* costs the most
(surrealdb-core, librocksdb-sys) and concluded every large lever was
out of scope. They never examined the **build profile** itself — a
lever the routine explicitly lists in scope ("disabling expensive opt
levels or LTO on debug profiles").

### Root cause

The workspace has **no `[profile.dev]` override**, so a plain
`cargo build` / `cargo test` (exactly what the baseline measures, and
what developers who don't use `cargo make` get) inherits cargo's
default `debug = 2` — full local-variable DWARF — for every crate,
including the ~300k-LOC `surrealdb-core`. Full debuginfo is a sizable
slice of codegen and of the final link, and is rarely used in everyday
iteration. The team already runs `debug = "line-tables-only"` in
`[profile.make]` and `[profile.profiling]`; only the bare `dev`
profile was left at the expensive default.

### Fix

Add a `[profile.dev]` matching the team's existing fast-debug config:

```toml
[profile.dev]
debug = "line-tables-only"
split-debuginfo = "unpacked"
```

`line-tables-only` keeps file:line resolution for panics,
`RUST_BACKTRACE`, and samplers/profilers while dropping local-variable
DWARF; `unpacked` keeps debuginfo in the per-object files so no
`dsymutil` pass runs. Runtime behaviour (emitted machine code) is
unchanged — only the DWARF sections differ. A one-off full-debuginfo
build for stepping under a debugger is still available via
`cargo build --config 'profile.dev.debug=2'`.

### Measured results (this machine, aarch64-apple-darwin, rustc 1.95.0)

Re-baselined on current `main` (22c66982d), since the prior 165s/19.6s
figures predate several `main` commits and machine-state drift.

| Scenario | Baseline | After | Δ |
|---|---:|---:|---:|
| Cold full `cargo build` (wall-clock) | 187.8s | 167.1s | **−20.7s / −11.0%** |
| Incremental (`touch core/lib.rs`) | 20.7s | 19.6s | −1.1s / −5.4% |

Per-crate rustc self-time (from `cargo --timings`):

| Crate | Baseline | After | Δ |
|---|---:|---:|---:|
| `surrealdb-core` | 102.6s | 81.6s | **−20.5%** |
| `surrealdb-server` | 23.8s | 13.4s | −44% |
| `surrealdb` (facade) | 11.6s | 5.3s | −54% |
| `surrealdb-mcp` | 7.8s | 4.0s | −49% |

The per-crate rustc wins are large (20–54%), but cold *wall-clock*
only moves 11% because the critical path is gated by the unaffected
C++ build scripts (`aws-lc-sys` 48s, `surrealdb-librocksdb-sys` 47s)
that run alongside / ahead of `surrealdb-core`. Incremental moves less
(−5.4%) because the incremental cache already skips most codegen; the
debuginfo saving there is concentrated in the final link.

### Why this over the alternatives

- Crate-splitting `surrealdb-core` (the biggest theoretical lever):
  out of scope, needs design alignment.
- librocksdb-sys / aws-lc-sys C++ build scripts: external forks, out
  of scope.
- Dep dedup / console-subscriber gating: ~0s wall-clock on this
  hardware (see runs 1–2).

The dev-profile debuginfo change is the only lever that is fully
in-scope (single-file, no external repos, behaviour byte-equivalent at
runtime), needs no design discussion, and clears the ≥10% cold bar.

### Verification

- Cold + incremental clean builds: link OK (exit 0).
- `cargo clippy --workspace --all-targets -- -D warnings`: clean.
- `cargo test --workspace --no-run`: all test binaries compile/link
  under the new profile (exit 0).
- `cargo test -p surrealdb-core --lib`: 2662 passed, 0 failed.
  (Full `cargo test --workspace` runtime not executed — its TiKV
  integration suites need a live cluster, and a debuginfo-only profile
  change cannot alter test outcomes.)

### Followups for later runs

- Discuss splitting `surrealdb-core` into parser/expr, kvs, idx/exec,
  gql sub-crates — the only path to moving the cold critical path
  below ~100s.
- Investigate pre-generated bindings / cached C++ output for the
  `surrealdb-librocksdb-sys` fork (47s on the critical path).
