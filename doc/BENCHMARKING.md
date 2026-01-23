# Performance Benchmark Workflow

This document explains the automated CRUD performance benchmark workflow that runs on every pull request.

## Overview

The benchmark workflow (`crud-bench.yml`) automatically runs CRUD benchmarks using [crud-bench](https://github.com/surrealdb/crud-bench) to measure SurrealDB performance. It posts the results as PR comments for review.

## Workflow Triggers

The benchmark workflow runs automatically on:

- **Pull Requests:** When a PR is opened, synchronized (new commits), or reopened
- **Manual Dispatch:** Can be manually triggered via GitHub Actions UI for testing
  - Allows specifying a custom crud-bench revision for testing upgrades

## Matrix Strategy

The workflow uses a **Cartesian product matrix** to maximize parallelization:

- **6 configurations** × **4 key types** = **24 independent jobs**
- Each job runs one benchmark with a clean database state
- Jobs run in parallel (subject to available runners)
- Failed jobs don't block other combinations

**Benefits:**
- ✅ Faster completion time (~5-7 minutes total vs ~13 minutes sequential per job)
- ✅ Better fault isolation (one failure doesn't stop others)
- ✅ Individual retry capability per config/key-type
- ✅ Clearer progress tracking (24 jobs vs 7)
- ✅ Guaranteed clean state for each benchmark
- ✅ Shared binary builds reduce redundant compilation

## crud-bench Version Management

The workflow uses a **pinned version** of crud-bench to ensure consistent benchmarking across all PRs (apples-to-apples comparison). This prevents benchmark variations caused by changes in the benchmarking tool itself.

**Current pinned revision:** `f8429e8cb3c4aa10dad72ac330f6d59af7d9f76c` (Jan 2024)

### Why Pin the Version?

- **Consistency:** All PRs benchmark against the same tool version
- **Reproducibility:** Results are comparable across different time periods
- **Controlled Upgrades:** Intentionally upgrade when ready, not automatically
- **Avoid False Positives:** Tool changes won't be mistaken for performance regressions

### How to Upgrade crud-bench

When you want to upgrade to a newer version of crud-bench:

1. **Find the target revision:**
   ```bash
   # Get latest commit from crud-bench main branch
   git ls-remote https://github.com/surrealdb/crud-bench.git HEAD
   ```

2. **Update the workflow:**
   - Edit `.github/workflows/crud-bench.yml`
   - Update `CRUD_BENCH_REVISION` in the `env` section
   - Update the comment at the top with the new revision and date

3. **Test the upgrade:**
   - Use workflow dispatch with the new revision as input
   - Verify benchmarks complete successfully
   - Check that output format hasn't changed

4. **Commit and verify:**
   - Create a PR with the update
   - Let benchmarks run to ensure everything works
   - Historical comparisons will continue using the new version going forward

### Manual Override

You can test a different crud-bench version without changing the workflow:

1. Go to **Actions** → **Performance Benchmarks**
2. Click **Run workflow**
3. Enter a git revision (commit hash, tag, or branch name)
4. Click **Run workflow**

## Benchmark Configurations

The workflow tests multiple SurrealDB configurations:

### Networked Benchmarks
These benchmarks test SurrealDB as a server using the binary built from your PR:
1. **Memory:** SurrealDB server with in-memory storage (`surrealdb-memory`)
2. **RocksDB:** SurrealDB server with RocksDB persistent storage (`surrealdb-rocksdb`)
3. **SurrealKV:** SurrealDB server with SurrealKV storage (`surrealdb-surrealkv`)

### Embedded Benchmarks
These benchmarks test the SurrealDB SDK embedded in the benchmark tool using Cargo patching:
1. **Embedded Memory:** SurrealDB SDK with in-memory storage (`surrealdb` + `-e memory`)
2. **Embedded RocksDB:** SurrealDB SDK with RocksDB storage (`surrealdb` + `-e rocksdb`)
3. **Embedded SurrealKV:** SurrealDB SDK with SurrealKV storage (`surrealdb` + `-e surrealkv`)

### Other Database Engines
The workflow also benchmarks:
- **SurrealKV:** Direct SurrealKV storage engine (embedded, no network endpoint)
- **SurrealMX:** SurrealMX storage engine (embedded, no network endpoint)

**All benchmarks use code from your PR** - networked benchmarks use the compiled binary, while embedded benchmarks use the SDK via Cargo patching.

### Customizing Network Endpoints

Networked SurrealDB benchmarks require an `endpoint` field to specify the connection URL. The `endpoint` field is **optional** for benchmarks that don't use network connections:

```yaml
# Networked benchmark - requires endpoint
- name: memory
  database: surrealdb-memory
  endpoint: ws://localhost:8000
  description: SurrealDB with in-memory storage

# HTTP variant (for testing REST API performance)
- name: memory-http
  database: surrealdb-memory
  endpoint: http://localhost:8000
  description: SurrealDB with in-memory storage (HTTP)

# Embedded benchmark - requires endpoint to specify storage
- name: embedded-memory
  database: surrealdb
  endpoint: memory
  description: SurrealDB embedded with in-memory storage

# Local database - no endpoint needed
- name: surrealkv
  database: surrealkv
  description: SurrealKV
```

**How endpoints are used:**
- **Networked SurrealDB** (`surrealdb-*`): Connection URL (e.g., `ws://localhost:8000`, `http://localhost:8000`)
  - Default if omitted: `ws://127.0.0.1:8000`
- **Embedded SurrealDB** (`surrealdb`): Storage engine specification (e.g., `memory`, `rocksdb:~/data`)
  - Default if omitted: `ws://127.0.0.1:8000` (not useful for embedded mode, so always specify!)
- **SurrealKV/SurrealMX**: Not used (these are local embedded databases)

This flexibility allows you to:
- Compare WebSocket vs HTTP performance for networked benchmarks
- Test different storage engines for embedded benchmarks
- Benchmark remote endpoints if needed

## Benchmark Parameters

The workflow uses a **matrix strategy** to test each configuration with multiple key types:

- **Matrix dimensions:**
  - **Configurations:** 6 (memory, rocksdb, embedded-memory, embedded-rocksdb, surrealkv-local, surrealmx)
  - **Key types:** 4 (integer, string26, string90, string250)
  - **Total jobs:** 24 (6 configs × 4 key types)

- **Benchmark parameters per job:**
  - **Samples:** 10,000 operations per CRUD operation
  - **Clients:** 12 concurrent clients
  - **Threads:** 48 threads per client
  - **Order:** Randomized key generation (`-r` flag)

Each matrix job runs independently with a clean database state, ensuring accurate performance measurements. Multiple key types ensure performance is measured across different indexing scenarios (small integers vs. long strings).

## Operations Tested

For each configuration, the benchmark measures:

- **Create:** Insert operations with unique records
- **Read:** Select operations by primary key
- **Update:** Modify existing records
- **Delete:** Remove records
- **Scans:** Table scans and range queries (if configured)

## Metrics Collected

For each operation, the following metrics are captured:

- **Throughput:** Operations per second
- **Latency:**
  - Average (mean)
  - P50 (median)
  - P95 (95th percentile)
  - P99 (99th percentile)
- **Total time:** Complete operation duration
- **Sample count:** Number of operations performed

## Performance Analysis

The workflow measures the following for each benchmark configuration:

- **Throughput:** Operations per second for each CRUD operation
- **Latency percentiles:** P50, P95, and P99 latencies
- **Sample count:** Number of operations performed

Results are posted as a PR comment for manual review and comparison.

## Result Reporting

### PR Comments

After benchmarks complete, the workflow posts a comment on the PR with:

- **Summary Table:** Throughput and latency percentiles for each config/operation
- **Detailed Metrics:** Expandable section with full metrics including average latency and total time
- **Methodology:** How the benchmarks were run

### Example Report

```markdown
## 🔍 CRUD Benchmark Results

### Summary

| Configuration | Operation | Throughput | P50 Latency | P95 Latency | P99 Latency | Samples |
|--------------|-----------|------------|-------------|-------------|-------------|---------|
| memory | Create | 125k ops/s | 8.2μs | 12.5μs | 18.3μs | 10000 |
| memory | Read | 185k ops/s | 5.4μs | 8.1μs | 11.2μs | 10000 |
| rocksdb | Create | 38k ops/s | 26.3μs | 45.2μs | 68.5μs | 10000 |
| rocksdb | Read | 52k ops/s | 19.1μs | 32.8μs | 48.9μs | 10000 |
```

## Result Artifacts

Benchmark results are stored as GitHub Actions artifacts:

- **Current results:** Individual JSON files for each configuration (30 days retention)
- **Analysis report:** Markdown and JSON files with the full analysis (90 days retention)

These can be downloaded from the workflow run for further analysis or comparison.

## Manually Running Benchmarks

### Trigger via GitHub UI

1. Go to **Actions** tab
2. Select **Performance Benchmarks** workflow
3. Click **Run workflow**
4. Select branch and click **Run workflow**

### Run Locally

To run benchmarks locally for testing:

```bash
# Clone crud-bench
git clone https://github.com/surrealdb/crud-bench.git
cd crud-bench

# Build crud-bench
cargo build --release

# Run benchmark against embedded SurrealDB with integer keys
./target/release/crud-bench -d surrealdb -e memory -s 10000 -c 12 -t 48 -k integer -r

# Run benchmark with string26 keys
./target/release/crud-bench -d surrealdb -e memory -s 10000 -c 12 -t 48 -k string26 -r

# Results will be in result*.json files
```

## Interpreting Results

### Understanding the Metrics

- **Throughput:** Higher is better - measures how many operations can be performed per second
- **Latency P50 (median):** Half of operations complete faster than this time
- **Latency P95:** 95% of operations complete faster than this time
- **Latency P99:** 99% of operations complete faster than this time

### What to Look For

Review the results to:

1. **Verify expected performance:** Check that performance aligns with expectations for your changes
2. **Compare configurations:** See how different storage engines perform
3. **Identify anomalies:** Look for unexpectedly low throughput or high latency
4. **Review across operations:** Check if changes affect specific CRUD operations more than others

### Variability

Benchmark results can vary due to:

- **CI runner variance:** Different runners may have different performance characteristics
- **System load:** Background processes can affect timing
- **Network overhead:** For networked benchmarks, network conditions matter
- **Warmup effects:** First runs may be slower than subsequent runs

For more reliable comparisons, consider:

- Running benchmarks multiple times
- Comparing against your local development environment
- Looking at trends across multiple commits rather than single runs

## Troubleshooting

### Benchmark Failed to Run

Check the workflow logs for:

- **Build failures:** SurrealDB binary failed to compile
- **Server startup issues:** SurrealDB server failed to start or become ready
- **Timeout:** Benchmark took longer than 45 minutes (rare)

### Analysis Script Errors

If the Python analysis script fails:

1. Verify JSON format matches expected crud-bench output
2. Check that result files were generated by crud-bench
3. Review workflow logs for parsing errors

### Benchmarks are Slow

The workflow uses parallel matrix execution, so total wall-clock time should be ~5-7 minutes if runners are available.

**Build Phase (parallel):**
- SurrealDB binaries with minimal features: ~3-4 minutes
- crud-bench with patched surrealdb: ~5 minutes
- **Total build time: ~5 minutes** (builds run in parallel)

**Benchmark Phase (parallel):**
- 24 jobs execute simultaneously
- Each job downloads pre-built binaries and runs benchmarks: ~30-60 seconds
- **Total benchmark time: ~1 minute** (when sufficient runners are available)

If individual jobs are too slow:

- Reduce sample count: Change `-s 10000` in the workflow to `-s 5000` or `-s 2500`
- Reduce concurrency: Change `-c 12 -t 48` to lower values like `-c 8 -t 16`

If you want to run fewer benchmarks:

- Skip specific configs: Add them to the `exclude:` section in the matrix
- Skip key types: Add specific config/key combinations to `exclude:`
- Example:
  ```yaml
  exclude:
    - config: rocksdb
      key_type: string250
  ```

## Architecture

### Workflow Jobs

The workflow uses a **three-phase architecture** with shared binary builds:

#### Phase 1: Build Jobs (parallel)

1. **build-surrealdb-binaries** (matrix job: 2 parallel builds)
   - Builds SurrealDB binaries with **minimal features** for each storage type
   - Matrix dimensions: 2 configs (memory, rocksdb)
   - Features per build:
     - `memory`: `--no-default-features --features "storage-mem,http"`
     - `rocksdb`: `--no-default-features --features "storage-rocksdb,http"`
   - Uploads binaries as artifacts: `surreal-binary-{config}`
   - Timeout: 15 minutes per build
   - **Expected time: ~3-4 minutes per build** (parallel)

2. **build-crud-bench** (single job)
   - Checks out SurrealDB sources and crud-bench repository
   - Aligns version compatibility between crud-bench and PR
   - Uses Cargo patching (`.github/tools/.cargo/config.toml`) to link crud-bench against local surrealdb
   - Builds crud-bench with patched dependencies
   - Uploads binary as artifact: `crud-bench-binary`
   - Timeout: 15 minutes
   - **Expected time: ~5 minutes**

#### Phase 2: Benchmark Jobs (parallel, 24 jobs)

3. **benchmark** (matrix job: 24 parallel jobs)
   - **Depends on:** `build-surrealdb-binaries`, `build-crud-bench`
   - Matrix dimensions: 6 configs × 4 key types = 24 jobs
   - Each job:
     - Downloads pre-built binaries from build phase
       - SurrealDB binary (if `needs_server == true`)
       - crud-bench binary (always)
     - Checks out crud-bench repository for directory structure
     - Starts SurrealDB server (for networked benchmarks only)
     - Runs single benchmark with specific config and key type
     - Uploads results as individual artifact
   - Clean state for every job (no data carryover)
   - Timeout: 30 minutes per job
   - **Expected time: ~30-60 seconds per job** (just benchmark execution + artifact I/O)

#### Phase 3: Analysis Job

4. **analyze-and-report**
   - Downloads benchmark results from all 24 matrix jobs
   - Merges all JSON result files
   - Parses and analyzes the results
   - Generates markdown and JSON reports
   - Posts/updates PR comment with results
   - Uploads analysis artifacts

### How PR Code is Used

The workflow ensures all benchmarks test your PR's code through **shared build jobs** that create binaries once and distribute them to all benchmark jobs:

#### Build Phase

1. **SurrealDB Binaries (for networked benchmarks)**
   - The `build-surrealdb-binaries` job builds minimal-feature binaries from your PR
   - Each binary includes only the features needed for its storage type:
     - `memory`: In-memory storage + HTTP server
     - `rocksdb`: RocksDB storage + HTTP server
   - Binaries are uploaded as artifacts and downloaded by benchmark jobs
   - This ensures networked benchmarks test your PR's server code

2. **crud-bench Binary (for all benchmarks)**
   - The `build-crud-bench` job uses Cargo patching (`.github/tools/.cargo/config.toml`)
   - This patches the `surrealdb` dependency in crud-bench to use your local SDK code
   - When crud-bench builds, it links against your PR's surrealdb SDK
   - The binary is uploaded as an artifact and downloaded by all benchmark jobs
   - This ensures embedded benchmarks test your PR's SDK code

#### Benchmark Phase

**Networked Benchmarks (memory, rocksdb):**
1. Download storage-specific SurrealDB binary from build phase
2. Start local SurrealDB server using the downloaded binary
3. Download and run crud-bench binary (which also uses your PR's SDK)
4. crud-bench connects to local server via `ws://localhost:8000`

**Embedded Benchmarks (embedded-memory, embedded-rocksdb, surrealkv-local, surrealmx):**
1. Download crud-bench binary from build phase (linked against your PR's SDK)
2. Run crud-bench with embedded storage (no server needed)
3. Benchmarks execute entirely within the SDK from your PR

**Key Benefits:**
- ✅ All benchmarks test the same PR code
- ✅ Binaries built once, used 24 times (efficient)
- ✅ Minimal features = faster builds
- ✅ Consistent test environment across all jobs

### Analysis Script

The Python script (`.github/scripts/analyze_benchmark.py`) handles:

- Parsing crud-bench JSON output
- Formatting metrics for display
- Markdown report generation
- JSON output for debugging

## Future Enhancements

Potential improvements to consider:

- **Historical Data Storage:** Store results in a database for trend analysis and regression detection
- **Trend Visualization:** Graphs showing performance over time
- **Flamegraphs:** CPU profiling for performance analysis
- **Memory Profiling:** Track memory usage alongside performance
- **Custom Benchmarks:** Allow PRs to specify custom benchmark configs
- **Benchmark Dashboard:** GitHub Pages site with historical trends
- **Comparison Mode:** Compare PR against specific commits or branches
- **Automated Regression Detection:** Statistical analysis to flag performance regressions

## References

- [crud-bench Repository](https://github.com/surrealdb/crud-bench)
- [crud-bench Documentation](https://github.com/surrealdb/crud-bench#readme)
- [SurrealDB Documentation](https://surrealdb.com/docs)

## Questions?

For questions or issues with the benchmark workflow:

1. Check this documentation
2. Review workflow logs in GitHub Actions
3. Open an issue with `benchmark` label
4. Ask in the SurrealDB Discord #performance channel

