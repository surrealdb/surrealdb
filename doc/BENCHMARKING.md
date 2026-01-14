# Performance Benchmark Workflow

This document explains the automated CRUD performance benchmark workflow that runs on every pull request.

## Overview

The benchmark workflow (`crud-bench.yml`) automatically runs CRUD benchmarks using [crud-bench](https://github.com/surrealdb/crud-bench) to measure SurrealDB performance. It posts the results as PR comments for review.

## Workflow Triggers

The benchmark workflow runs automatically on:

- **Pull Requests:** When a PR is opened, synchronized (new commits), or reopened
- **Manual Dispatch:** Can be manually triggered via GitHub Actions UI for testing
  - Allows specifying a custom crud-bench revision for testing upgrades

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

Each configuration runs with:

- **Samples:** 10,000 operations per CRUD operation
- **Clients:** 12 concurrent clients
- **Threads:** 48 threads per client
- **Order:** Randomized key generation (`-r` flag)

These parameters balance test duration with statistical significance while providing high concurrency stress testing.

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

# Run benchmark against embedded SurrealDB
./target/release/crud-bench -d surrealdb -e memory -s 10000 -c 12 -t 48 -r

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

If benchmarks take too long:

- Reduce sample count: Change `-s 10000` to `-s 5000` or `-s 2500`
- Reduce concurrency: Change `-c 12 -t 48` to lower values like `-c 8 -t 16`
- Skip configurations: Comment out matrix entries in `crud-bench.yml`

## Architecture

### Workflow Jobs

1. **crud-benchmark** (matrix job)
   - Builds SurrealDB from source
   - Clones and builds crud-bench
   - Runs benchmarks for each configuration
   - Uploads JSON results as artifacts

2. **analyze-and-report**
   - Downloads benchmark results from all matrix jobs
   - Parses and analyzes the results
   - Generates markdown and JSON reports
   - Posts PR comment with results
   - Uploads analysis artifacts

### How PR Code is Used

The workflow ensures all benchmarks test your PR's code through two mechanisms:

#### Networked Benchmarks (surrealdb-memory, surrealdb-rocksdb, surrealdb-surrealkv)
1. The workflow builds the SurrealDB binary from your PR
2. For networked configurations, it starts a local SurrealDB server using this binary
3. crud-bench connects to this local server via `ws://localhost:8000`
4. The server runs entirely from your PR's code

#### Embedded Benchmarks (embedded-memory, embedded-rocksdb, embedded-surrealkv)
1. The workflow uses Cargo patching (`.github/tools/.cargo/config.toml`)
2. This patches the `surrealdb` dependency in crud-bench to use your local SDK code
3. When crud-bench builds, it links against your PR's surrealdb SDK
4. The embedded benchmarks run entirely from your PR's code

Both approaches ensure apples-to-apples comparison: all benchmarks test the same PR code, just in different deployment modes (server vs embedded).

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

