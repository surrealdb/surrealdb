# Performance Benchmark Workflow

This document explains the automated CRUD performance benchmark workflow that runs on every pull request to detect performance regressions.

## Overview

The benchmark workflow (`crud-bench.yml`) automatically runs CRUD benchmarks using [crud-bench](https://github.com/surrealdb/crud-bench) to detect performance regressions in SurrealDB. It compares current performance against a 30-day historical baseline and posts warnings as PR comments when significant regressions are detected.

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

The workflow tests three SurrealDB configurations:

1. **Memory:** SurrealDB with in-memory storage engine (`surrealdb-memory`)
2. **RocksDB:** SurrealDB with RocksDB persistent storage (`surrealdb-rocksdb`)
3. **Embedded:** SurrealDB embedded SDK with in-memory storage

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

## Regression Detection

### Baseline Calculation

The analysis script compares current results against a 30-day rolling baseline:

1. Fetches historical results from the `benchmark-results` branch
2. Calculates median and standard deviation for each metric
3. Uses the last 30 days of data from the `main` branch

### Regression Threshold

A performance regression is flagged when:

- **Throughput drops by >15%** compared to the 30-day median
- The difference is statistically significant (Z-test at 95% confidence)

### Statistical Significance

The analysis uses a Z-test to determine if observed differences are statistically significant:

```
z = (current - baseline) / stddev
```

A z-score > 1.96 indicates significance at the 95% confidence level.

## Result Reporting

### PR Comments

After benchmarks complete, the workflow posts a comment on the PR with:

- **Summary Table:** Status, current vs baseline performance for each config/operation
- **Performance Warnings:** Detailed list of detected regressions with percentage changes
- **Performance Improvements:** List of operations that got faster
- **Detailed Metrics:** Expandable section with full latency percentiles
- **Methodology:** How the analysis was performed

### Status Indicators

- ✅ **Green Check:** No regression, stable performance
- ⚠️ **Yellow Warning:** Performance regression detected
- 🚀 **Rocket:** Performance improvement detected
- ℹ️ **Info:** No baseline data available (first run)

### Example Report

```markdown
## 🔍 CRUD Benchmark Results

### ⚠️ Performance Regressions Detected

| Configuration | Operation | Status | Current | Baseline | Change |
|--------------|-----------|--------|---------|----------|--------|
| memory | Create | ✅ | 125k ops/s | 123k ops/s | +1.6% |
| rocksdb | Create | ⚠️ | 38k ops/s | 45k ops/s | -15.6% |
| embedded | Read | 🚀 | 185k ops/s | 175k ops/s | +5.7% |

### ⚠️ Performance Warnings
- **rocksdb - Create:** 15.6% slower than 30-day median (38k ops/s vs 45k ops/s) (statistically significant)
```

## Historical Data Storage

### Storage Location

Historical benchmark results are stored in the `benchmark-results` branch in this repository.

### Structure

```
benchmark-results/
  ├── index.json
  ├── README.md
  └── results/
      ├── 2026-01-05/
      │   ├── abc123-memory.json
      │   ├── abc123-rocksdb.json
      │   └── abc123-embedded.json
      └── 2026-01-06/
          └── ...
```

### Retention Policy

- Results are stored indefinitely
- Only results from `main` branch are stored
- PR results are not stored (only compared against historical data)

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

### What is a Regression?

A regression occurs when performance decreases significantly compared to the historical baseline:

- **Throughput regression:** Operations per second decreases by >15%
- **Latency regression:** Operation time increases by >15%

### When to Investigate

You should investigate when:

1. **Multiple operations regress** in the same configuration
2. **Regression is statistically significant** (marked in report)
3. **Regression is consistent** across multiple commits
4. **Regression affects production-critical paths** (e.g., read operations)

### Benign Regressions

Some regressions may be acceptable:

- **Intentional tradeoffs:** Improved safety at cost of speed
- **One-time spikes:** Random variance in CI environment (rare with 5K samples)
- **Minor regressions:** <5% changes that fall within noise
- **Temporary regressions:** Partially implemented optimizations

### False Positives

False positives can occur due to:

- **Insufficient baseline data:** Less than 7 days of history
- **Environmental variance:** Different CI runner performance
- **Incomplete warmup:** First runs after cold start

If you suspect a false positive, manually re-run the benchmark.

## Adjusting Thresholds

To adjust the regression detection threshold, edit the analysis script:

```python
# In .github/scripts/analyze_benchmark.py
analyzer = BenchmarkAnalyzer(
    results_dir=args.results_dir,
    historical_days=30,  # Change lookback period
    regression_threshold=0.15  # Change threshold (0.15 = 15%)
)
```

## Troubleshooting

### Benchmark Failed to Run

Check the workflow logs for:

- **Docker issues:** Ensure Docker is available on the runner
- **Build failures:** SurrealDB binary failed to compile
- **Timeout:** Benchmark took longer than 45 minutes (rare)

### No Historical Data

On the first run or after resetting the `benchmark-results` branch:

- Results will show "No baseline data available"
- After ~7 days of data, comparisons become meaningful
- After 30 days, the full baseline window is available

### Analysis Script Errors

If the Python analysis script fails:

1. Check that numpy and scipy are installed
2. Verify JSON format matches expected crud-bench output
3. Check for corrupted historical data files

### Benchmarks are Slow

If benchmarks take too long:

- Reduce sample count: Change `-s 10000` to `-s 5000` or `-s 2500`
- Reduce concurrency: Change `-c 12 -t 48` to lower values like `-c 8 -t 16`
- Skip configurations: Comment out matrix entries in `crud-bench.yml`

## Performance Baseline Initialization

For the first 7-30 days after enabling this workflow:

1. **Days 1-7:** Not enough data for reliable baselines
2. **Days 7-30:** Baselines are available but may be noisy
3. **Day 30+:** Full 30-day rolling baseline provides stable comparison

During initialization, focus on:

- Detecting **major regressions** (>30% drops)
- Building intuition for normal variance
- Watching for systematic trends

## Architecture

### Workflow Jobs

1. **crud-benchmark** (matrix job)
   - Builds SurrealDB from source
   - Clones and builds crud-bench
   - Runs benchmarks for each configuration
   - Uploads JSON results as artifacts

2. **analyze-and-report**
   - Downloads benchmark results
   - Fetches historical data from `benchmark-results` branch
   - Runs statistical analysis
   - Posts PR comment with results
   - Stores results to `benchmark-results` branch (main only)

### Analysis Script

The Python script (`.github/scripts/analyze_benchmark.py`) handles:

- Parsing crud-bench JSON output
- Loading historical data
- Statistical calculations (median, stddev, z-tests)
- Markdown report generation
- JSON output for debugging

## Future Enhancements

Potential improvements to consider:

- **Trend Visualization:** Graphs showing performance over time
- **Flamegraphs:** CPU profiling for regressions
- **Memory Profiling:** Track memory usage alongside performance
- **Custom Benchmarks:** Allow PRs to specify custom benchmark configs
- **Benchmark Dashboard:** GitHub Pages site with historical trends
- **Alerting:** Slack/Discord notifications for critical regressions
- **Comparison Mode:** Compare PR against specific commits

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

