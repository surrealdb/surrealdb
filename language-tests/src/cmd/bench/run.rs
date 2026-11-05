use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use clap::ArgMatches;
use semver::Version;
use sha2::{Digest, Sha256};
use surrealdb_core::channel;
use surrealdb_core::dbs::Session;
use surrealdb_core::env::VERSION;
use surrealdb_core::kvs::{Builder, Datastore, QueryRequest, QuerySource};
use surrealdb_rpc::capabilities::{Capabilities, Targets};
use surrealdb_types::Datetime;

use crate::cli::{Backend, ColorMode};
use crate::cmd::bench::meta::current_commit;
use crate::cmd::bench::stats::{ComparisonData, MeasurementData};
use crate::cmd::bench::store::{Baseline, BenchMarkRun, StoreConfig, get_store};
use crate::cmd::bench::{DEFAULT_NOISE_THRESHOLD, DEFAULT_SIGNIFICANCE_THRESHOLD};
use crate::cmd::util::ImportFailure;
use crate::cmd::{graphql, util};
use crate::tests::case::Dialect;
use crate::tests::run::{CaseImports, RunConfig, TestRunId};
use crate::tests::schema::{BoolOr, TestConfig};
use crate::tests::{CaseSet, RunSetBuilder, TestRun};

/// The measurement cap `--quick` imposes on every bench. A bench whose single
/// iteration, re-import included, outlasts it can collect only one sample, so
/// it sets `[bench] quick = false`; revisit those benches if this changes.
const QUICK_MAX_TIME: Duration = Duration::from_secs(10);

struct BenchRunConfig {
	/// Key grouping runs whose populated datastore is byte-for-byte
	/// interchangeable, so the dataset is imported once and reused across them.
	/// Computed once per run (after matrix expansion) by [`calc_group_key`].
	group_key: String,
	/// Short label of the dataset variant this run uses (from `[bench].datasets`),
	/// or `None` when the bench runs once against its `[env].imports`.
	variant: Option<String>,
	/// Per-variant import chain to run instead of `case.imports`. `None` falls
	/// back to the case's own resolved imports.
	imports: Option<Vec<Arc<crate::tests::case::TestCase>>>,
}

impl RunConfig for BenchRunConfig {
	fn name(&self, case: &CaseImports) -> String {
		match &self.variant {
			Some(v) => format!("{} [{v}]", case.test.origin.path),
			None => case.test.origin.path.clone(),
		}
	}
}

static HEX: &[u8] = b"0123456789abcdef";

/// The core capabilities baked into the datastore at build time for a test
/// config. Shared by [`builder_from_config`] and [`calc_group_key`] so the group
/// key reflects exactly what the engine is constructed with.
fn resolve_capabilities(config: &TestConfig) -> Capabilities {
	match &config.env.capabilities {
		BoolOr::Bool(true) => Capabilities::all().with_experimental(Targets::All),
		BoolOr::Bool(false) => Capabilities::none(),
		BoolOr::Value(x) => util::core_capabilities_from_test_config(x),
	}
}

/// Key identifying datastores that are byte-for-byte interchangeable, so a single
/// populated datastore can be built once and shared across every read-only bench
/// that maps to the same key (see the grouping in [`run`]). Two benches may share
/// a datastore iff their effective import chain, built capabilities, backend, and
/// target namespace + database all match — everything that determines the
/// populated keyspace and the engine config.
///
/// Deliberately excluded for ordinary benches: auth, dialect,
/// and the bench's own query. These only parameterize the per-bench execute
/// session and statement (built fresh per bench against the shared store), never
/// where imported data lands — imports always run as `Session::owner()`
/// retargeted to `(ns, db)`. Graph-fold benches are the exception: their store
/// layout depends on the statement (see below), so `source` joins the key there.
fn calc_group_key(
	chain: &[Arc<crate::tests::case::TestCase>],
	parsed: &TestConfig,
	backend: Backend,
	source: &str,
	config: &[(String, String)],
) -> String {
	let mut hasher = Sha256::new();
	// Effective import chain: each import's path + mtime, in order.
	for i in chain.iter() {
		hasher.update(i.origin.path.as_bytes());
		if let Ok(epoch) = i.origin.modified.duration_since(SystemTime::UNIX_EPOCH) {
			hasher.update(epoch.as_secs().to_le_bytes());
			hasher.update(epoch.subsec_nanos().to_le_bytes());
		}
	}
	// The capabilities actually baked into the datastore (derived `Debug` is
	// stable), so benches built with different capabilities never share a store.
	hasher.update(format!("{:?}", resolve_capabilities(parsed)).as_bytes());
	// Backend engine/layout — never share a populated store across backends.
	let backend_tag = match backend {
		Backend::Memory => "mem",
		Backend::SurrealKv => "surrealkv",
		Backend::TikV => "tikv",
		Backend::Other => "other",
	};
	hasher.update(backend_tag.as_bytes());
	// The datastore configuration the run was given: a store built under one is not
	// interchangeable with one built under another.
	for (key, value) in config {
		hasher.update(key.as_bytes());
		hasher.update(b"=");
		hasher.update(value.as_bytes());
		hasher.update(b"\x00");
	}
	// Target namespace + database: imports physically land under exactly (ns, db).
	hasher.update(parsed.env.namespace().unwrap_or("").as_bytes());
	hasher.update(b"\x00");
	hasher.update(parsed.env.database().unwrap_or("").as_bytes());
	// Graph adjacency layout: a folded store is not interchangeable with an
	// unfolded one, nor numeric block entries with plain ones. The fold pass
	// primes with the bench's own statement, so which scopes end up folded is
	// statement-dependent: folded benches never share a store unless they run
	// the same statement.
	hasher.update([parsed.bench.graph_fold as u8, parsed.bench.graph_numeric_ids as u8]);
	if parsed.bench.graph_fold || parsed.bench.graph_numeric_ids {
		hasher.update(source.as_bytes());
	}

	let bytes = hasher.finalize();
	let mut res = String::new();
	for b in bytes.iter() {
		res.push(HEX[(b & 0b1111) as usize] as char);
		res.push(HEX[(b >> 4) as usize] as char);
	}
	res
}

struct BenchConfig {
	backend: Backend,
	/// The datastore configuration keys given with `--config`.
	config: Vec<(String, String)>,
	/// Temp directory under which file-backed datastores (surrealkv) are
	/// built; `None` for the in-memory backend. Removed when `run()` returns.
	bench_root: Option<PathBuf>,
}

/// Counter for unique per-datastore subdirectories under `bench_root`.
static DS_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Connection string for the configured backend. File-backed engines get a fresh
/// subdirectory each call, so every `prepare()` builds a clean datastore.
fn datastore_conn(config: &BenchConfig) -> String {
	let scheme = match config.backend {
		Backend::Memory => return "mem://".to_string(),
		Backend::SurrealKv => "surrealkv",
		Backend::TikV => unreachable!("TiKV is rejected before prepare()"),
		Backend::Other => unreachable!("`other` is rejected before prepare()"),
	};
	let root = config.bench_root.as_ref().expect("file backend requires a bench root");
	let id = DS_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
	format!("{scheme}://{}", root.join(format!("store_{id}")).display())
}

/// Removes its directory tree on drop, cleaning up file-backed datastores.
struct CleanupDir(PathBuf);

impl Drop for CleanupDir {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

/// Closes a datastore the run has finished with and reclaims the directory it
/// was built in.
///
/// Dropping the last `Arc<Datastore>` does not close a file-backed engine: its
/// own background threads hold a
/// handle to the store until `shutdown` stops them. A store that is only
/// dropped stays open for the rest of the process, its memory resident, its
/// background work still running and its files still held, and removing its
/// directory then unlinks files it has open without freeing their space. So a
/// file-backed store is shut down first, closes when `dbs` — which must be its
/// last handle — drops, and only then is the temp root emptied. Only one
/// datastore is live at a time, so emptying the whole root is safe.
///
/// On the in-memory backend dropping `dbs` frees the store and there is no
/// directory, so nothing else is done.
async fn retire_datastore(dbs: Arc<Datastore>, config: &BenchConfig) {
	let Some(root) = config.bench_root.as_deref() else {
		return;
	};
	if let Err(e) = dbs.shutdown().await {
		println!("Warning: could not shut down a bench datastore: {e}");
	}
	drop(dbs);
	let _ = std::fs::remove_dir_all(root);
	let _ = std::fs::create_dir_all(root);
}

/// Creates (clean) the temp root for this run's file-backed datastores.
fn make_bench_root() -> Result<PathBuf> {
	let root = std::env::temp_dir().join(format!("surreal_bench_{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&root);
	std::fs::create_dir_all(&root).context("Could not create bench datastore directory")?;
	Ok(root)
}

struct CmdConfig<'a> {
	path: &'a String,
	filter: Option<&'a String>,
	dataset: Option<&'a String>,
	backend: Backend,
	save: bool,
	quick: bool,
	json: Option<&'a String>,
	/// The datastore configuration keys given with `--config`, for every datastore the run
	/// builds.
	config: Vec<(String, String)>,
	store: StoreConfig<'a>,
}

impl<'a> CmdConfig<'a> {
	fn from_matches(parent: &'a ArgMatches, current: &'a ArgMatches) -> Self {
		let path: &String = current.get_one("path").unwrap();

		let filter = current.get_one::<String>("filter");
		let dataset = current.get_one::<String>("dataset");
		let backend = *current.get_one::<Backend>("backend").unwrap();
		let save = current.get_flag("save");
		let quick = current.get_flag("quick");
		let json = current.get_one::<String>("json");
		let config = current
			.get_many::<(String, String)>("config")
			.map(|pairs| pairs.cloned().collect())
			.unwrap_or_default();

		Self {
			path,
			filter,
			dataset,
			backend,
			save,
			quick,
			json,
			config,
			store: StoreConfig::from_matches(parent),
		}
	}
}

/// Classifies a comparison against the baseline into a stable, machine-readable
/// verdict. Kept in sync with the human-readable summary printed in `run()`:
/// a difference that isn't statistically significant, or is significant but
/// inside the noise threshold, is reported as `within-noise`.
fn comparison_verdict(compare: &Comparison) -> &'static str {
	// A bench that returns a different number of rows than its baseline did is
	// not a faster or slower version of the same work — it is different work,
	// and reporting a percentage against it invites the reader to blame the
	// code. This is not hypothetical: #635 changed mock expansion, which changed
	// the seeded `rand::*` draw order, which changed how many rows the scan
	// benches match; the timings moved up to 2.4x with the engine untouched.
	if compare.workload_changed() {
		return "workload-changed";
	}
	let compare = &compare.data;
	if compare.p_value >= DEFAULT_SIGNIFICANCE_THRESHOLD {
		return "within-noise";
	}
	let noise = DEFAULT_NOISE_THRESHOLD;
	if compare.dist_mean.lower_bound < -noise && compare.dist_mean.upper_bound < -noise {
		"improved"
	} else if compare.dist_mean.lower_bound > noise && compare.dist_mean.upper_bound > noise {
		"regressed"
	} else {
		"within-noise"
	}
}

/// A baseline comparison, with enough of the baseline's provenance to say
/// whether the comparison is meaningful.
struct Comparison {
	data: ComparisonData,
	/// Commit the baseline was measured at, when it was recorded.
	baseline_commit: Option<String>,
	/// When the baseline was measured. A baseline that predates the benches it
	/// is compared against reports a difference that no code caused.
	baseline_datetime: Option<Datetime>,
	baseline_rows: Option<i64>,
	rows: Option<i64>,
}

impl Comparison {
	/// Whether this bench returned a different number of rows than the baseline
	/// did. Unknown on either side (a GQL bench, or a baseline written before
	/// `rows` was recorded) counts as unchanged, so an unknown never
	/// manufactures a warning.
	fn workload_changed(&self) -> bool {
		match (self.baseline_rows, self.rows) {
			(Some(base), Some(now)) => base != now,
			_ => false,
		}
	}

	/// Whole days between the baseline measurement and now.
	fn baseline_age_days(&self) -> Option<i64> {
		let taken = self.baseline_datetime.as_ref()?;
		let now = Datetime::now();
		Some((*now - **taken).num_days())
	}
}

/// A baseline older than this is reported alongside every comparison drawn
/// against it. The nightly is meant to refresh these every 24 hours, so
/// anything past a couple of days means the nightly has been failing and the
/// board is measuring against whatever it last managed to store.
const BASELINE_STALE_DAYS: i64 = 2;

/// Per-bench comparison against the fetched baseline. Percentages are relative to
/// the baseline (positive = slower than baseline = regression).
#[derive(serde::Serialize)]
struct JsonComparison {
	/// Median per-iteration time of the baseline (`main`), in seconds.
	base_median_secs: f64,
	change_pct: f64,
	change_lo_pct: f64,
	change_hi_pct: f64,
	p_value: f64,
	verdict: &'static str,
	/// Commit the baseline was measured at, so a comparison can be traced to the
	/// code that produced the number it is drawn against. `None` for rows stored
	/// before the commit was recorded.
	base_commit: Option<String>,
	/// Age of the baseline in whole days. The nightly refreshes these every 24
	/// hours, so anything larger means it has been failing and this comparison
	/// spans every merge since.
	base_age_days: Option<i64>,
	/// Rows the baseline's statement returned, against `JsonBench::rows`. A
	/// difference means the two runs measured different work.
	base_rows: Option<i64>,
}

/// One bench's result in the `--json` report. Times are in seconds.
#[derive(serde::Serialize)]
struct JsonBench {
	name: String,
	median_secs: f64,
	median_lo_secs: f64,
	median_hi_secs: f64,
	mean_secs: f64,
	/// Rows this bench's statement returned, or `None` for a dialect whose
	/// result is not a row list. Recorded so a change in the *workload* is
	/// distinguishable from a change in its speed.
	rows: Option<i64>,
	comparison: Option<JsonComparison>,
}

/// The `--json` report: every measured bench plus its comparison (when a baseline
/// was found in the store). Consumed by the PR-comment renderer in CI.
#[derive(serde::Serialize)]
struct JsonReport {
	quick: bool,
	backend: String,
	benches: Vec<JsonBench>,
}

/// Main subcommand function, runs the actual subcommand.
pub async fn run(color: ColorMode, parent: &ArgMatches, current: &ArgMatches) -> Result<()> {
	if cfg!(debug_assertions) {
		println!(
			"Warning, debug assertions are enabled, it is likely the benchmarking suite is build without optimization"
		)
	}

	// Before any datastore is built, so every HNSW index in the run is built from
	// the same graph seed.
	pin_hnsw_build_seed();

	let mut load_errors = Vec::new();

	let cfg = CmdConfig::from_matches(parent, current);
	if cfg.save && !cfg.config.is_empty() {
		bail!(
			"--config cannot be combined with --save: a run under a datastore configuration of its own would replace the stored baseline. Use --json to compare runs"
		);
	}
	let set = CaseSet::load_test_files(cfg.path, &mut load_errors).await?;

	// Resolve the backend. The in-memory engine needs no storage; file-backed
	// engines (surrealkv) build into a temporary directory (a fresh
	// subdir per datastore). TiKV needs an external cluster, so it's unsupported.
	let bench_root = match cfg.backend {
		Backend::Memory => None,
		// surrealkv is always available under the `bench` feature (kv-surrealkv).
		Backend::SurrealKv => Some(make_bench_root()?),
		Backend::TikV => {
			bail!("TiKV is not supported for benchmarking (it needs an external cluster)")
		}
		Backend::Other => {
			bail!("the `other` backend names a server's engine; benchmarks open their own")
		}
	};
	// Removes the whole temp tree when `run()` returns; declared here so it drops
	// after every datastore created during the run.
	let _bench_root_cleanup = bench_root.clone().map(CleanupDir);

	let config = BenchConfig {
		backend: cfg.backend,
		config: cfg.config.clone(),
		bench_root,
	};

	let mut store = get_store(&cfg.store).await?;

	// Metadata embedded into every stored measurement (and used to key the
	// baseline by transport). Benches currently run embedded; the remote
	// transports populate this differently.
	let meta = crate::cmd::bench::meta::RunMetadata::gather("embedded", None);

	let core_version = Version::parse(VERSION).unwrap();
	let set_builder = RunSetBuilder::new(&set, &mut load_errors)
		// Only run test for which run is enabled.
		.with_filter(|x| x.test.config.parsed.bench.run)
		// Only run benches whose path matches the optional filter.
		.with_filter(|x| {
			cfg.filter.is_none_or(|filter| x.test.origin.path.contains(filter.as_str()))
		})
		// Only run test for this backend.
		.with_filter(|x| {
			let config_backend = &x.test.config.parsed.env.backend;
			config_backend.is_empty() || config_backend.contains(&cfg.backend)
		})
		// Only run for the right version.
		.with_filter(|x| {
			if let Some(x) = &x.test.config.parsed.test.version
				&& !x.matches(&core_version)
			{
				return false;
			}

			if let Some(x) = &x.test.config.parsed.test.importing_version
				&& !x.matches(&core_version)
			{
				return false;
			}

			for i in x.imports.iter() {
				if let Some(x) = &i.config.parsed.test.version
					&& !x.matches(&core_version)
				{
					return false;
				}
			}

			true
		})
		// One config per bench; dataset-matrix expansion happens after build()
		// (below) where the CaseSet is available for resolving import chains.
		.with_expander(|_| {
			vec![BenchRunConfig {
				// Computed after matrix expansion (needs the effective chain).
				group_key: String::new(),
				variant: None,
				imports: None,
			}]
		});

	let built = set_builder.build();

	// Expand each bench that declares `[bench].datasets` into one run per dataset,
	// resolving each dataset's (transitive) import chain. Benches without
	// `datasets` pass through unchanged and use their `[env].imports`.
	let mut runs = Vec::with_capacity(built.len());
	for run in built {
		let datasets = run.case.test.config.parsed.bench.datasets.clone();
		if datasets.is_empty() {
			runs.push(run);
			continue;
		}
		for (name, path) in &datasets {
			// `--dataset` selects a single matrix variant by its (exact) name.
			if let Some(want) = cfg.dataset
				&& name != want.as_str()
			{
				continue;
			}
			let chain = set.resolve_imports(
				std::slice::from_ref(path),
				run.case.test.id,
				&run.case.test.origin,
				&mut load_errors,
			);
			let Some(chain) = chain else {
				continue;
			};
			runs.push(TestRun {
				// Reassigned to a unique value below.
				id: TestRunId::new(0),
				case: run.case.clone(),
				config: BenchRunConfig {
					// Computed after matrix expansion (needs the effective chain).
					group_key: String::new(),
					variant: Some(name.clone()),
					imports: Some(chain),
				},
			});
		}
	}

	// Passthrough runs keep their original `build()` ids while expanded runs are
	// freshly created, so renumber to keep ids unique across the final set.
	for (idx, run) in runs.iter_mut().enumerate() {
		run.id = TestRunId::new(idx);
	}

	// Compute each run's group key from its EFFECTIVE import chain (the per-variant
	// chain when set, else the case's own imports) plus the build-affecting env, so
	// matrix variants with different chains land in distinct groups.
	for run in runs.iter_mut() {
		let chain = run.config.imports.as_deref().unwrap_or(&run.case.imports);
		run.config.group_key = calc_group_key(
			chain,
			&run.case.test.config.parsed,
			cfg.backend,
			&run.case.test.source,
			&cfg.config,
		);
	}

	if runs.is_empty() {
		println!("No benchmarks found, exiting");
		return Ok(());
	}

	// `--quick` leaves out the benches that opt out of it (`[bench] quick =
	// false`), naming each so an explicit quick run of one does not read as a
	// filter that matched nothing. A set they empty still reaches the common
	// finalization, so a requested `--json` report is written (empty) and load
	// errors are reported.
	if cfg.quick {
		runs.retain(|run| {
			let keep = run.case.test.config.parsed.bench.quick;
			if !keep {
				println!(
					"Skipping {} (`quick = false`: one iteration outlasts quick mode's {QUICK_MAX_TIME:?} cap)",
					run.name()
				);
			}
			keep
		});
	}

	// Split runs into read-only (groupable) and mutating (`rebuild = true`). Only
	// `execute` is timed, so a read-only bench's dataset can be imported once and
	// shared; mutating benches rebuild a fresh datastore per iteration.
	let mut readonly = Vec::new();
	let mut rebuild = Vec::new();
	for run in runs.into_iter() {
		if run.case.test.config.parsed.bench.rebuild {
			rebuild.push(run);
		} else {
			readonly.push(run);
		}
	}

	// Order read-only runs so members of a group are contiguous, in the order each
	// group's first member was discovered (a stable sort preserves discovery order
	// within a group). This keeps the printed/stored order close to the original
	// path order rather than the opaque hash, while letting one populated datastore
	// serve a whole contiguous run of benches.
	let mut first_seen = std::collections::HashMap::new();
	for (idx, run) in readonly.iter().enumerate() {
		first_seen.entry(run.config.group_key.clone()).or_insert(idx);
	}
	readonly.sort_by_key(|run| first_seen[&run.config.group_key]);

	let mut measurements: Vec<(String, MeasurementData, Option<i64>, Option<Comparison>)> =
		Vec::new();

	// Read-only benches: build + populate + compact one datastore per group, then
	// run every member against it. The whole group runs on a single runtime so the
	// shared datastore is built and used on the same runtime.
	let mut idx = 0;
	while idx < readonly.len() {
		let mut end = idx + 1;
		while end < readonly.len()
			&& readonly[end].config.group_key == readonly[idx].config.group_key
		{
			end += 1;
		}
		let group = &readonly[idx..end];

		// Fetch baselines on this (the store's) runtime before handing the group to
		// its own runtime. Baseline is keyed on the run name, which includes the
		// dataset-matrix variant, so variants keep separate baseline series.
		let mut baselines = Vec::with_capacity(group.len());
		for run in group {
			baselines.push(
				store
					.fetch_latest(&run.name(), cfg.backend, &meta.transport)
					.await
					.context("Could not fetch latest measurement data")?,
			);
		}

		if group.len() > 1 {
			println!(
				"Importing shared dataset for {} benches (e.g. {})",
				group.len(),
				group[0].name()
			);
		}

		let outcome = thread::scope(|scope| {
			scope
				.spawn(|| {
					tokio::runtime::Builder::new_multi_thread()
						.enable_all()
						.build()
						.unwrap()
						.block_on(run_group(group, &config, cfg.quick, baselines))
				})
				.join()
		})
		.map_err(|e| {
			if let Some(x) = e.downcast_ref::<String>() {
				anyhow!("Measurement thread paniced: {x}")
			} else {
				anyhow!("Measurement thread paniced")
			}
		})??;

		match outcome {
			GroupOutcome::ImportFailed(imp_fail) => {
				println!(
					"Error, import `{}` returned an error: {}",
					imp_fail.path, imp_fail.message
				);
			}
			GroupOutcome::Ran(results) => {
				// Persist each group's results as it finishes, so a run killed partway
				// (job timeout, runner eviction, panic) keeps every completed group
				// instead of discarding the lot. Non-fatal: a transient store error
				// must not sink an otherwise-good run.
				for (run, result) in group.iter().zip(results) {
					match result {
						BenchRunResult::Import(imp_fail) => {
							println!(
								"Error, import `{}` returned an error: {}",
								imp_fail.path, imp_fail.message
							);
						}
						BenchRunResult::InsufficientSamples(collected) => {
							println!(
								"Skipping {}: collected only {collected} sample(s) within max_time (need at least 2 to compute statistics)",
								run.name()
							);
						}
						BenchRunResult::Ok(measurement, rows, compare) => {
							if cfg.save
								&& let Err(e) = store
									.add(BenchMarkRun {
										measurement: measurement.clone(),
										path: run.name(),
										backend: cfg.backend,
										meta: meta.clone(),
										commit: current_commit(),
										rows,
									})
									.await
							{
								eprintln!(
									"Warning: could not store measurement for {}: {e:#}",
									run.name()
								);
							}
							measurements.push((run.name(), measurement, rows, compare));
						}
					}
				}
			}
		}

		release_freed_heap();

		idx = end;
	}

	// Mutating benches: rebuild a fresh datastore each iteration (never grouped).
	// On a file backend that re-imports the dataset to disk every iteration, which
	// is impractically slow for most of them, so they run on the in-memory backend
	// unless the bench sets `rebuild-file-backend` — the opt-in for a bench whose
	// subject is the storage layer, where `mem` measures the wrong thing.
	for run in rebuild.into_iter() {
		if cfg.backend != Backend::Memory && !run.case.test.config.parsed.bench.rebuild_file_backend
		{
			println!(
				"Skipping {} (`rebuild = true` benches run on the `mem` backend unless they set `rebuild-file-backend`)",
				run.name()
			);
			continue;
		}

		let baseline = store
			.fetch_latest(&run.name(), cfg.backend, &meta.transport)
			.await
			.context("Could not fetch latest measurement data")?;

		let measurement = thread::scope(|scope| {
			scope
				.spawn(|| {
					tokio::runtime::Builder::new_multi_thread()
						.enable_all()
						.build()
						.unwrap()
						.block_on(run_bench(&run, &config, baseline, cfg.quick, None))
				})
				.join()
		})
		.map_err(|e| {
			if let Some(x) = e.downcast_ref::<String>() {
				anyhow!("Measurement thread paniced: {x}")
			} else {
				anyhow!("Measurement thread paniced")
			}
		})??;

		match measurement {
			BenchRunResult::Import(imp_fail) => {
				println!(
					"Error, import `{}` returned an error: {}",
					imp_fail.path, imp_fail.message
				);
			}
			BenchRunResult::InsufficientSamples(collected) => {
				println!(
					"Skipping {}: collected only {collected} sample(s) within max_time (need at least 2 to compute statistics)",
					run.name()
				);
			}
			BenchRunResult::Ok(measurement, rows, compare) => {
				// Persist each result the moment its bench finishes, not in a trailing
				// pass over the whole suite, so a run killed partway (job timeout,
				// runner eviction, panic) keeps every completed bench instead of
				// discarding the lot. The store is a per-bench time series, so a
				// partial run is well-formed. Non-fatal: a transient store error must
				// not sink an otherwise-good run.
				if cfg.save
					&& let Err(e) = store
						.add(BenchMarkRun {
							measurement: measurement.clone(),
							path: run.name(),
							backend: cfg.backend,
							meta: meta.clone(),
							commit: current_commit(),
							rows,
						})
						.await
				{
					eprintln!("Warning: could not store measurement for {}: {e:#}", run.name());
				}
				measurements.push((run.name(), measurement, rows, compare));
			}
		}

		release_freed_heap();
	}

	for (name, m, _rows, compare) in &measurements {
		println!(" - {}", name);

		if let Some(compare) = compare {
			// Said before the percentage, because the percentage is not worth
			// reading once either of these holds.
			if compare.workload_changed() {
				println!(
					"       WORKLOAD CHANGED: returns {} rows, baseline returned {} — \
					 this is different work, not a speed difference",
					compare.rows.unwrap_or_default(),
					compare.baseline_rows.unwrap_or_default(),
				);
			}
			if let Some(age) = compare.baseline_age_days()
				&& age > BASELINE_STALE_DAYS
			{
				println!(
					"       STALE BASELINE: measured {age} days ago{} — the comparison \
					 below spans every merge since",
					match &compare.baseline_commit {
						Some(c) => format!(" at {}", c.chars().take(9).collect::<String>()),
						None => String::new(),
					}
				);
			}

			let compare = &compare.data;
			let signficant = compare.p_value < DEFAULT_SIGNIFICANCE_THRESHOLD;
			if !signficant {
				println!("       No change in performance detected")
			} else {
				let noise = DEFAULT_NOISE_THRESHOLD;
				if compare.dist_mean.lower_bound < -noise && compare.dist_mean.upper_bound < -noise
				{
					println!("       Performance has improved")
				} else if compare.dist_mean.lower_bound > noise
					&& compare.dist_mean.upper_bound > noise
				{
					println!("       Performance has regressed")
				} else {
					println!("       Performance difference within noise threshold")
				}
			}

			fn sign(negative: bool) -> &'static str {
				if negative {
					"-"
				} else {
					""
				}
			}

			let lb = Duration::from_secs_f64(compare.dist_mean.lower_bound.abs());
			let lb_sign = sign(compare.dist_mean.lower_bound.is_sign_negative());
			let ub = Duration::from_secs_f64(compare.dist_mean.upper_bound.abs());
			let ub_sign = sign(compare.dist_mean.upper_bound.is_sign_negative());
			let p = Duration::from_secs_f64(compare.dist_mean.point.abs());
			let p_sign = sign(compare.dist_mean.point.is_sign_negative());

			println!(
				" {:>24} : [{}{:?} {}{:?} {}{:?}] (p = {:.2} {} {:.2})",
				"change",
				lb_sign,
				lb,
				p_sign,
				p,
				ub_sign,
				ub,
				compare.p_value,
				if signficant {
					"<"
				} else {
					">"
				},
				DEFAULT_SIGNIFICANCE_THRESHOLD
			);
		};

		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"time",
			Duration::from_secs_f64(m.mean.lower_bound),
			Duration::from_secs_f64(m.mean.point),
			Duration::from_secs_f64(m.mean.upper_bound),
		);
		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"median",
			Duration::from_secs_f64(m.median.lower_bound),
			Duration::from_secs_f64(m.median.point),
			Duration::from_secs_f64(m.median.upper_bound),
		);
		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"std dev",
			Duration::from_secs_f64(m.std_dev.lower_bound),
			Duration::from_secs_f64(m.std_dev.point),
			Duration::from_secs_f64(m.std_dev.upper_bound),
		);
		println!(
			" {:>24} : [{:?} {:?} {:?}]",
			"mad",
			Duration::from_secs_f64(m.abs_dev.lower_bound),
			Duration::from_secs_f64(m.abs_dev.point),
			Duration::from_secs_f64(m.abs_dev.upper_bound),
		);
		let outliers = m.labels.iter().filter(|x| x.is_outlier()).count();
		if outliers != 0 {
			println!(
				"   Found {} outliers, among {} measurements ({:.2}%)",
				outliers,
				m.labels.len(),
				((outliers as f64 / m.labels.len() as f64) * 100.0).round()
			);

			println!(
				"    Low severe  {}",
				m.labels.iter().filter(|x| x.is_low() && x.is_severe()).count()
			);
			println!(
				"    Low mild    {}",
				m.labels.iter().filter(|x| x.is_low() && !x.is_severe()).count()
			);
			println!(
				"    High mild   {}",
				m.labels.iter().filter(|x| x.is_high() && !x.is_severe()).count()
			);
			println!(
				"    High severe {}",
				m.labels.iter().filter(|x| x.is_high() && x.is_severe()).count()
			);
		}
	}

	if let Some(json_path) = cfg.json {
		let benches = measurements
			.iter()
			.map(|(name, m, rows, compare)| JsonBench {
				name: name.clone(),
				median_secs: m.median.point,
				median_lo_secs: m.median.lower_bound,
				median_hi_secs: m.median.upper_bound,
				mean_secs: m.mean.point,
				rows: *rows,
				comparison: compare.as_ref().map(|c| JsonComparison {
					base_median_secs: c.data.base_median,
					change_pct: c.data.dist_mean.point * 100.0,
					change_lo_pct: c.data.dist_mean.lower_bound * 100.0,
					change_hi_pct: c.data.dist_mean.upper_bound * 100.0,
					p_value: c.data.p_value,
					verdict: comparison_verdict(c),
					base_commit: c.baseline_commit.clone(),
					base_age_days: c.baseline_age_days(),
					base_rows: c.baseline_rows,
				}),
			})
			.collect();
		let report = JsonReport {
			quick: cfg.quick,
			backend: cfg.backend.to_string(),
			benches,
		};
		let json =
			serde_json::to_string_pretty(&report).context("Failed to serialize benchmark JSON")?;
		std::fs::write(json_path, json)
			.with_context(|| format!("Failed to write benchmark JSON to {json_path}"))?;
		println!("Wrote benchmark JSON to {json_path}");
	}

	for e in load_errors.iter() {
		e.display(color);
	}

	store.close().await.context("Failed to close benchmark store")?;

	if !load_errors.is_empty() {
		bail!("Could not load all tests")
	}

	Ok(())
}

pub fn builder_from_config(config: &TestConfig, run: &[(String, String)]) -> Result<Builder> {
	let capabilities = resolve_capabilities(config);

	// Quiescent, because a benchmark has to measure the statement rather than
	// whatever the background tasks happen to be doing beside it: compaction,
	// changefeed GC, tombstone reclaim and the cluster heartbeat all run on the
	// same cores as the timed section. The indexes the benches read are still
	// compacted — `build_and_populate` drains the compaction queue itself once
	// the dataset is loaded, so that cost lands before the measurement instead
	// of during it, and the explicit drain no longer contends with the
	// background task for the `IndexCompaction` lease.
	let builder = Datastore::builder().without_maintenance_tasks();
	// Graph-fold benches drop the fold threshold to one so the single priming
	// execution observes every scope it traverses, however small — the
	// post-import fold pass in `build_and_populate` then packs exactly those
	// scopes. The threshold is a builder-level override, not a config key.
	let builder = if config.bench.graph_fold || config.bench.graph_numeric_ids {
		builder.with_graph_fold_threshold(1)
	} else {
		builder
	};
	// The keys of the datastore are one map: the bench's own, then those of the run, which never
	// override the bench's.
	let mut keys = surrealdb_cnf::ConfigMap::empty();
	if config.bench.graph_numeric_ids {
		if run.iter().any(|(key, _)| key == "graph_numeric_ids") {
			bail!("--config graph_numeric_ids collides with the key the bench sets itself");
		}
		keys = keys.with_key_value("graph_numeric_ids", "true");
	}
	for (key, value) in run {
		keys = keys.with_key_value(key.as_str(), value.as_str());
	}
	let builder = if config.bench.graph_numeric_ids || !run.is_empty() {
		builder.with_config(keys)
	} else {
		builder
	};
	let builder = if capabilities.allows_live_query_notifications() {
		let (send, _) = channel::bounded(15_000);
		builder.with_notify(send)
	} else {
		builder
	};
	Ok(builder.with_capabilities(capabilities))
}

#[allow(clippy::large_enum_variant)]
enum BenchRunResult {
	Import(ImportFailure),
	Ok(MeasurementData, Option<i64>, Option<Comparison>),
	/// Too few samples were collected to compute statistics — a bench whose single
	/// iteration exceeds `max_time` collects only one sample, and the stats need at
	/// least two. Reported and skipped instead of panicking.
	InsufficientSamples(usize),
}

/// The executable form of a bench case: raw SurrealQL source, a lowered GQL
/// plan, or a GraphQL request executed against the schema generated for the
/// prepared datastore.
enum BenchStatement {
	SurrealQl,
	PgSql,
	MySql,
	/// A parsed-and-lowered GQL plan. Parse + lowering happen once in
	/// `prepare`, so the timed iterations measure plan execution only (mirroring
	/// the GraphQL arm, which generates its schema up front). The plan is cloned
	/// per iteration because submitting it consumes it by value.
	Gql {
		plan: surrealdb_gql::PreparedGqlQuery,
	},
	GraphQl {
		schema: async_graphql::dynamic::Schema,
		/// The case source with the config comment blanked out, computed once.
		source: String,
		session: Arc<Session>,
		variables: async_graphql::Variables,
		operation: Option<String>,
	},
}

impl BenchStatement {
	/// Builds the statement for a freshly prepared datastore. For GraphQL this
	/// generates the schema and request parts up front — mirroring the
	/// server's schema cache — so the timed iterations measure query execution
	/// only.
	async fn prepare(
		run: &TestRun<BenchRunConfig>,
		dbs: &Arc<Datastore>,
		session: &Session,
	) -> Result<Self> {
		match run.case.test.dialect {
			Dialect::SurrealQl => Ok(Self::SurrealQl),
			Dialect::PgSql => Ok(Self::PgSql),
			Dialect::MySql => Ok(Self::MySql),
			Dialect::Gql => {
				// Parse + lower the `.gql` source once, exactly as the run path
				// does (see `cmd::run::run_test_with_dbs`), so the timed loop
				// measures only execution.
				let settings = surrealdb_gql::GqlParserSettings::default();
				let source = run.case.test.source.as_bytes();
				let plan =
					surrealdb_gql::parse_to_plan_with_settings(&run.case.test.source, settings)
						.map_err(|e| {
							anyhow!(
								"Failed to parse/lower GQL bench statement: {}",
								e.render_on_bytes(source)
							)
						})?;
				Ok(Self::Gql {
					plan,
				})
			}
			Dialect::GraphQl => {
				let schema = graphql::generate_schema(dbs, session)
					.await
					.map_err(|e| anyhow!("Failed to generate GraphQL schema: {e}"))?;
				Ok(Self::GraphQl {
					schema,
					source: graphql::case_source(&run.case.test),
					session: Arc::new(session.clone()),
					variables: graphql::request_variables(&run.case.test)?,
					operation: run.case.test.config.parsed.graphql.operation.clone(),
				})
			}
		}
	}

	/// Executes the bench statement once.
	///
	/// Both SurrealQL and GraphQL carry statement errors in-band rather than
	/// as a failed call, so both are checked here: a bench whose statement
	/// errors every iteration measures the error path, and the cost of
	/// failing is unrelated to — and usually far cheaper than — the work the
	/// bench claims to time.
	/// Returns the number of rows the statement produced, which the caller
	/// records alongside the timing. Two runs of a bench that return different
	/// row counts are not measuring the same work, however comparable their
	/// timings look — see [`JsonBench::rows`].
	async fn execute(
		&self,
		run: &TestRun<BenchRunConfig>,
		dbs: &Arc<Datastore>,
		session: &Session,
	) -> Result<Option<i64>> {
		match self {
			Self::SurrealQl => {
				let results = dbs.execute(&run.case.test.source, session, None).await?;
				let mut rows = 0i64;
				for (idx, result) in results.iter().enumerate() {
					match &result.result {
						Err(error) => bail!("bench statement {idx} returned an error: {error}"),
						// One row for a non-array result, so a bench selecting a
						// single record still counts as work rather than as zero.
						Ok(value) => {
							rows += value.as_array().map_or(1, |a| a.len() as i64);
						}
					}
				}
				return Ok(Some(rows));
			}
			Self::PgSql => {
				let results = dbs
					.run(QueryRequest::new(QuerySource::pgsql(&run.case.test.source), session))
					.await?;
				let mut rows = 0i64;
				for (idx, result) in results.iter().enumerate() {
					match &result.result {
						Err(error) => bail!("bench statement {idx} returned an error: {error}"),
						Ok(value) => {
							rows += value.as_array().map_or(1, |a| a.len() as i64);
						}
					}
				}
				return Ok(Some(rows));
			}
			Self::MySql => {
				let results = dbs
					.run(QueryRequest::new(QuerySource::mysql(&run.case.test.source), session))
					.await?;
				let mut rows = 0i64;
				for (idx, result) in results.iter().enumerate() {
					match &result.result {
						Err(error) => bail!("bench statement {idx} returned an error: {error}"),
						Ok(value) => {
							rows += value.as_array().map_or(1, |a| a.len() as i64);
						}
					}
				}
				return Ok(Some(rows));
			}
			Self::Gql {
				plan,
			} => {
				// Submitting the plan takes it by value, so each iteration
				// executes a fresh clone of the once-lowered plan.
				let _ = dbs
					.run(QueryRequest::new(QuerySource::Plan(vec![plan.clone().0]), session))
					.await?;
			}
			Self::GraphQl {
				schema,
				source,
				session,
				variables,
				operation,
			} => {
				let mut request = async_graphql::Request::new(source.clone())
					.data(Arc::clone(dbs))
					.data(Arc::clone(session));
				request.variables = variables.clone();
				if let Some(operation) = operation {
					request = request.operation_name(operation);
				}
				let response = schema.execute(request).await;
				if let Some(error) = response.errors.first() {
					bail!("GraphQL bench statement returned an error: {}", error.message);
				}
			}
		}
		// GQL and GraphQL results are not shaped as a row list, so they carry no
		// row count and are compared on timing alone.
		Ok(None)
	}
}

/// Seed used to make benchmark datasets deterministic. Each dataset build
/// reseeds the engine RNG (see `surrealdb_expr::val::rnd`) so `rand::*` values and
/// `|record:N|` ids are identical across runs and independent of benchmark
/// order. Override with `SURREAL_RAND_SEED` to sanity-check results against a
/// different, but still fixed, dataset draw; the variable is parsed once by
/// `surrealdb_cnf::RAND_SEED` (which warns on a malformed value).
fn dataset_seed() -> u64 {
	const DEFAULT_DATASET_SEED: u64 = 0x5EED_B0A7;
	surrealdb_cnf::RAND_SEED.unwrap_or(DEFAULT_DATASET_SEED)
}

/// Pin HNSW graph construction to a fixed seed for the whole run.
///
/// HNSW assigns each element a random layer at insert time, so an unseeded build
/// produces a different graph on every run and the same kNN query then traverses
/// a different number of nodes. That variance dwarfs any regression the suite is
/// meant to catch, which makes an unseeded ANN bench useless as a guardrail.
///
/// The harness owns this default rather than the CI workflows so a local
/// `cargo make bench` and a nightly run build the same graph without anyone
/// having to remember an environment variable. `SURREAL_HNSW_BUILD_SEED` still
/// takes precedence (see [`surrealdb_cnf::hnsw_build_seed`]), so a single run can
/// be re-seeded to check that a result is not an artefact of this one graph.
///
/// Changing the constant re-shapes every HNSW graph in the suite, so ANN
/// measurements taken before the change are no longer comparable with ones taken
/// after it.
fn pin_hnsw_build_seed() {
	const DEFAULT_HNSW_BUILD_SEED: u64 = 0x5EED_11A5;
	surrealdb_cnf::set_default_hnsw_build_seed(DEFAULT_HNSW_BUILD_SEED);
}

/// Builds a fresh datastore, reseeds the engine RNG, runs the (effective) import
/// chain, and performs index compaction — leaving a populated datastore ready to
/// execute timed read-only statements.
///
/// Returns `Ok(Err(..))` when an import fails so the caller can surface it as an
/// [`BenchRunResult::Import`] / [`GroupOutcome::ImportFailed`]. A datastore that
/// fails to populate, through an import failure or an error, is retired here, so
/// neither path leaves it open.
///
/// Shared by the grouped read-only path (built once per dataset group, then reused
/// across the group's benches) and, via [`prepare`], the per-iteration rebuild
/// path. The reseed makes generated data identical on every build and independent
/// of benchmark order, so nightly comparisons reflect code changes rather than the
/// luck of the dataset draw.
async fn build_and_populate(
	run: &TestRun<BenchRunConfig>,
	config: &BenchConfig,
	token: &tokio_util::sync::CancellationToken,
) -> Result<std::result::Result<Arc<Datastore>, ImportFailure>> {
	let dbs = builder_from_config(&run.case.test.config.parsed, &config.config)?
		.build_with_path(&datastore_conn(config))
		.await?;
	match populate(run, token, &dbs).await {
		Ok(None) => Ok(Ok(dbs)),
		Ok(Some(e)) => {
			retire_datastore(dbs, config).await;
			Ok(Err(e))
		}
		Err(e) => {
			retire_datastore(dbs, config).await;
			Err(e)
		}
	}
}

/// Everything [`build_and_populate`] does once the datastore exists: reseed,
/// import, compact and, for a graph-fold bench, fold. Returns the failure when
/// an import fails.
async fn populate(
	run: &TestRun<BenchRunConfig>,
	token: &tokio_util::sync::CancellationToken,
	dbs: &Arc<Datastore>,
) -> Result<Option<ImportFailure>> {
	// The import session only targets the namespace/database — data lands as owner
	// (see `run_imports_list`), so it is independent of the bench's own auth. The
	// timed statement gets its own session per bench.
	let session = util::session_from_test_config(&run.case.test.config.parsed);

	surrealdb_expr::val::rnd::reseed(dataset_seed());

	// Use the per-variant dataset import chain (from `[bench].datasets`) when one
	// was selected, otherwise fall back to the bench's own resolved imports.
	let imports = run.config.imports.as_deref().unwrap_or(&run.case.imports);
	let compatibility = run.case.test.config.parsed.env.compatibility;
	if let Some(e) = util::run_imports_list(imports, session, compatibility, dbs).await? {
		return Ok(Some(e));
	}

	Datastore::index_compaction(dbs.clone(), Duration::from_secs(1), token.clone()).await?;

	// Graph-fold benches: execute the statement once as the priming pass — the
	// fold queue is observation-driven, so exactly the scopes the timed
	// statement traverses get folded — then drain the queue with the same
	// routine the background task runs. Both land here, before the warmup, so
	// the measured iterations read the folded layout without paying for it.
	let bench = &run.case.test.config.parsed.bench;
	if bench.graph_fold || bench.graph_numeric_ids {
		if bench.rebuild {
			bail!(
				"`graph-fold` / `graph-numeric-ids` require `rebuild = false`: the fold \
				 priming pass would execute a mutating statement an extra, unmeasured time"
			);
		}
		if !matches!(run.case.test.dialect, Dialect::SurrealQl) {
			bail!("`graph-fold` / `graph-numeric-ids` require a SurrealQL bench statement");
		}
		let session = util::session_from_test_config(&run.case.test.config.parsed);
		let results = dbs.execute(&run.case.test.source, &session, None).await?;
		for (idx, result) in results.iter().enumerate() {
			if let Err(error) = &result.result {
				bail!("graph-fold priming statement {idx} returned an error: {error}");
			}
		}
		// A single pass drains the queue as it stood when the pass began; a
		// very large scope can leave residual entries behind, so run passes
		// until one reports no work. The pass count is bounded so a scope
		// that keeps re-enqueuing itself fails with a diagnostic instead of
		// spinning until the job timeout.
		const MAX_FOLD_DRAIN_PASSES: usize = 100;
		let mut residual = usize::MAX;
		for _ in 0..MAX_FOLD_DRAIN_PASSES {
			let (iterations, errors) =
				Datastore::graph_fold(dbs.clone(), Duration::from_secs(1), token.clone()).await?;
			if errors > 0 {
				bail!("the graph-fold pass reported {errors} scope errors");
			}
			residual = iterations;
			if iterations == 0 {
				break;
			}
		}
		if residual != 0 {
			bail!(
				"the graph-fold drain did not quiesce after {MAX_FOLD_DRAIN_PASSES} passes \
				 ({residual} scopes still queued on the last pass)"
			);
		}
	}

	Ok(None)
}

/// Prepares the bench statement against a datastore [`prepare`] just built and
/// runs it once, returning the rows it produced and how long the statement took
/// — the preparation is not timed.
///
/// The statement is dropped before this returns, whatever the outcome: a GraphQL
/// statement's schema holds handles to the datastore, and the caller retires
/// `dbs` next, which needs the last one.
async fn execute_on_fresh(
	run: &TestRun<BenchRunConfig>,
	dbs: &Arc<Datastore>,
	session: &Session,
) -> Result<(Option<i64>, Duration)> {
	let statement = BenchStatement::prepare(run, dbs, session).await?;
	let start = Instant::now();
	let produced = statement.execute(run, dbs, session).await?;
	Ok((produced, start.elapsed()))
}

/// Builds a populated datastore plus the per-bench execute session. Used by the
/// `rebuild = true` path, which rebuilds a fresh datastore every iteration.
async fn prepare(
	run: &TestRun<BenchRunConfig>,
	config: &BenchConfig,
	token: &tokio_util::sync::CancellationToken,
) -> Result<std::result::Result<(Arc<Datastore>, Session), ImportFailure>> {
	match build_and_populate(run, config, token).await? {
		Err(e) => Ok(Err(e)),
		Ok(dbs) => {
			let session = util::session_from_test_config(&run.case.test.config.parsed);
			Ok(Ok((dbs, session)))
		}
	}
}

/// Outcome of running one dataset group's read-only benches against a shared,
/// already-populated datastore (see [`run_group`]).
#[allow(clippy::large_enum_variant)]
enum GroupOutcome {
	/// Building/populating the group's shared dataset failed.
	ImportFailed(ImportFailure),
	/// Per-member result, in group order. Each is `Ok` or `InsufficientSamples`;
	/// a grouped read-only bench imports nothing of its own, so it cannot fail to
	/// import.
	Ran(Vec<BenchRunResult>),
}

/// Builds the group's dataset once, then runs every read-only bench in the group
/// against the one shared datastore. `baselines` is per-member, in group order.
///
/// All members share the same `group_key`, so they resolve to an identical
/// populated datastore; the per-bench session and statement are still built
/// individually against the shared `dbs`.
async fn run_group(
	group: &[TestRun<BenchRunConfig>],
	config: &BenchConfig,
	quick: bool,
	baselines: Vec<Option<Baseline>>,
) -> Result<GroupOutcome> {
	let token = tokio_util::sync::CancellationToken::new();

	// Any member is a valid template for building the (identical) dataset.
	let dbs = match build_and_populate(&group[0], config, &token).await? {
		Ok(dbs) => dbs,
		Err(e) => return Ok(GroupOutcome::ImportFailed(e)),
	};

	// Each member runs against the shared `dbs`; its per-bench result (`Ok` or
	// `InsufficientSamples`) is returned to `run()` for storing/printing.
	let ran = async {
		let mut results = Vec::with_capacity(group.len());
		for (run, baseline) in group.iter().zip(baselines) {
			results.push(run_bench(run, config, baseline, quick, Some(dbs.clone())).await?);
		}
		anyhow::Ok(results)
	}
	.await;
	// Every member's handle dropped with its `run_bench`, so this is the last.
	// Retired before any member's error propagates, so a failed run leaves no
	// store open.
	retire_datastore(dbs, config).await;

	Ok(GroupOutcome::Ran(ran?))
}

/// Emits a sentinel line on stderr delimiting the measured region of a bench,
/// so an external profiler can attach for only the timed statements and skip the
/// dataset import + warmup (see `scripts/bench/profile.sh`). No-op unless the
/// `BENCH_MARKERS` env var is set, to keep normal `bench run` output clean.
fn bench_marker(name: &str) {
	if std::env::var_os("BENCH_MARKERS").is_some() {
		eprintln!("{name}");
	}
}

async fn run_bench(
	run: &TestRun<BenchRunConfig>,
	config: &BenchConfig,
	baseline: Option<Baseline>,
	quick: bool,
	shared_dbs: Option<Arc<Datastore>>,
) -> Result<BenchRunResult> {
	println!(
		"Running bench {}{}",
		run.name(),
		if quick {
			" [quick]"
		} else {
			""
		}
	);
	println!("Warming up");

	let bench_config = &run.case.test.config.parsed.bench;

	// Quick mode shrinks every timing knob ~10x for a fast, coarse comparison: it
	// reliably surfaces large regressions but is too low-powered for small drift.
	// Otherwise honour the per-bench `[bench]` config.
	let warmup_time = if quick {
		Duration::from_millis(250)
	} else {
		bench_config.warmup.0
	};
	let sample_size = if quick {
		10
	} else {
		bench_config.sample_size
	};
	let measurement_time = if quick {
		Duration::from_secs(2)
	} else {
		bench_config.measurement_time.0
	};
	let max_time = if quick {
		QUICK_MAX_TIME
	} else {
		bench_config.max_time.0
	};
	let token = tokio_util::sync::CancellationToken::new();

	// Read-only benches (`rebuild = false`, the default) run against the group's
	// already-populated datastore (`shared_dbs`), reused across every warmup and
	// measured iteration so we time only the statement against a stable dataset.
	// Mutating benches (`rebuild = true`) get `shared_dbs = None` and rebuild a
	// fresh datastore per iteration below.
	//
	// IMPORTANT: a read-only bench must NOT mutate the shared datastore — its query
	// runs against the same store as every other bench in its group, so a write
	// would corrupt their results. Any bench whose query creates/updates/deletes
	// data MUST set `rebuild = true`.
	let shared = match shared_dbs {
		Some(dbs) => {
			let session = util::session_from_test_config(&run.case.test.config.parsed);
			let statement = BenchStatement::prepare(run, &dbs, &session).await?;
			Some((dbs, session, statement))
		}
		None => None,
	};

	// Reseed before timing so any `rand::*` drawn in the bench query (e.g. the
	// vector-KNN query vector) is deterministic and independent of how many benches
	// shared this datastore before it. (The rebuild path reseeds again inside each
	// per-iteration `prepare`; harmless.)
	surrealdb_expr::val::rnd::reseed(dataset_seed());

	let before_warmup = Instant::now();
	let mut count = 0usize;
	loop {
		if let Some((dbs, session, statement)) = shared.as_ref() {
			statement.execute(run, dbs, session).await?;
		} else {
			let (dbs, session) = match prepare(run, config, &token).await? {
				Ok(prepared) => prepared,
				Err(e) => return Ok(BenchRunResult::Import(e)),
			};
			let executed = execute_on_fresh(run, &dbs, &session).await;
			retire_datastore(dbs, config).await;
			executed?;
		}

		count += 1;

		if before_warmup.elapsed() > warmup_time {
			break;
		}
	}
	let measured_warmup_time = before_warmup.elapsed();

	let expected_iteration_time = measured_warmup_time.as_secs_f64() / count as f64;

	let iterations_per_samples =
		((measurement_time.as_secs_f64() / expected_iteration_time / sample_size as f64).ceil()
			as u64)
			.max(1);

	if iterations_per_samples == 1 {
		println!(
			"Could not complete {} samples in set measurement_time of {:?}",
			sample_size, measurement_time
		);
	}

	let estimate = expected_iteration_time * iterations_per_samples as f64 * sample_size as f64;

	println!(
		"Completed {count} iterations in {warmup_time:?}, estimated execution time is {:?}",
		Duration::from_secs_f64(estimate)
	);

	// Mark the measured region so a profiler attaching mid-run (after the dataset
	// import + warmup) samples only the timed statements. The in-process
	// profiler (`bench-pprof` feature + `BENCH_PPROF`) covers the same region.
	bench_marker("__BENCH_MEASURE_START__");
	let profiler = super::pprof::RegionProfiler::start();

	let measure_start = Instant::now();
	let mut iterations = Vec::new();
	let mut samples = Vec::new();
	// Row count of the last measured iteration. Every iteration runs the same
	// statement against the same data, so any one of them describes the
	// workload; taking the last avoids an extra untimed execution.
	let mut rows: Option<i64> = None;
	for _ in 0..sample_size {
		let mut sample_duration = 0.0;
		for _ in 0..iterations_per_samples {
			if let Some((dbs, session, statement)) = shared.as_ref() {
				let start = Instant::now();
				let produced = statement.execute(run, dbs, session).await?;
				sample_duration += start.elapsed().as_secs_f64();
				rows = produced;
			} else {
				let (dbs, session) = match prepare(run, config, &token).await? {
					Ok(prepared) => prepared,
					Err(e) => return Ok(BenchRunResult::Import(e)),
				};
				let executed = execute_on_fresh(run, &dbs, &session).await;
				// Outside the timed region, as the import is: closing a file-backed
				// store flushes it and drains its background work, so none of that
				// work runs into the next iteration's measurement. Retired before an
				// error propagates, so a failed run leaves no store open.
				retire_datastore(dbs, config).await;
				let (produced, elapsed) = executed?;
				sample_duration += elapsed.as_secs_f64();
				rows = produced;
			}
		}
		iterations.push(iterations_per_samples as f64);
		samples.push(sample_duration);

		// Wall-clock backstop: a bench whose real per-iteration cost dwarfs the
		// warmup estimate would otherwise collect every sample no matter how long
		// it takes, so one mis-scoped bench can consume the whole run. Stop once we
		// pass the cap; the sample we're in always completes, so we keep at least one.
		if measure_start.elapsed() >= max_time {
			println!(
				"Exceeded max_time of {max_time:?}; stopping after {} of {} samples",
				samples.len(),
				sample_size
			);
			break;
		}
	}

	profiler.finish();
	bench_marker("__BENCH_MEASURE_END__");

	// A bench whose single iteration exceeds `max_time` collects only one sample,
	// which is too few for the statistics (they need at least two). Report it as
	// skipped rather than panicking inside `Sample::new`.
	let collected = samples.len();
	let Some(measurement) = MeasurementData::from_iteration_times(iterations, samples) else {
		return Ok(BenchRunResult::InsufficientSamples(collected));
	};
	let comp = baseline.map(|baseline| Comparison {
		data: ComparisonData::compare(&baseline.measurement, &measurement),
		baseline_commit: baseline.commit,
		baseline_datetime: baseline.datetime,
		baseline_rows: baseline.rows,
		rows,
	});

	Ok(BenchRunResult::Ok(measurement, rows, comp))
}

/// Returns pages freed by a bench's datastore to the OS.
///
/// Called only where a datastore has just been dropped — a read-only dataset
/// group finishing, or a mutating bench finishing — so its allocations are
/// already free as far as the process is concerned, but glibc keeps them
/// resident until something trims. Trimming holds the resident floor a later
/// bench measures under at a repeatable level rather than at whatever the
/// heaviest preceding dataset happened to leave behind.
///
/// This does not lower the process's peak, which is reached inside a group
/// while its dataset is being imported and nothing has yet been freed. It is
/// never called from inside a measured region, so its `madvise` cost cannot
/// land in a reported median.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn release_freed_heap() {
	unsafe extern "C" {
		fn malloc_trim(pad: usize) -> i32;
	}
	// SAFETY: takes no pointers, borrows nothing, and is callable at any point
	// in the life of a process.
	unsafe { malloc_trim(0) };
}

/// No-op on targets whose allocator exposes no equivalent: musl has no
/// `malloc_trim`, and macOS would need `malloc_zone_pressure_relief`.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn release_freed_heap() {}

#[cfg(test)]
mod tests {
	use super::*;

	fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
		pairs.iter().map(|(key, value)| ((*key).to_owned(), (*value).to_owned())).collect()
	}

	/// A key given with `--config` reaches the datastore a bench builds.
	#[tokio::test]
	async fn a_morsel_key_reaches_the_datastore() {
		let run = pairs(&[
			("morsel_split_rows", "1"),
			("morsel_target_bytes", "1"),
			("morsel_places", "0"),
			("morsel_steal_always", "true"),
		]);
		let ds = builder_from_config(&TestConfig::default(), &run)
			.expect("a builder")
			.build_with_path("mem://")
			.await
			.expect("a datastore");
		let held = ds.morsel_knobs();
		assert!(held.contains(&("morsel_split_rows", "1".to_owned())), "{held:?}");
		assert!(held.contains(&("morsel_target_bytes", "1".to_owned())), "{held:?}");
		assert!(held.contains(&("morsel_places", "0".to_owned())), "{held:?}");
		assert!(held.contains(&("morsel_steal_always", "true".to_owned())), "{held:?}");
	}

	/// A run that gives no key builds the datastore the bench alone configures.
	#[tokio::test]
	async fn no_key_leaves_the_defaults() {
		let ds = builder_from_config(&TestConfig::default(), &[])
			.expect("a builder")
			.build_with_path("mem://")
			.await
			.expect("a datastore");
		let held = ds.morsel_knobs();
		assert!(!held.contains(&("morsel_split_rows", "1".to_owned())), "{held:?}");
	}

	/// The keys of a run fold beside the keys a bench sets itself, and a key the bench sets is
	/// never overridden by the run: a run that gives it is refused.
	#[test]
	fn a_key_the_bench_sets_is_not_overridden() {
		let mut config = TestConfig::default();
		config.bench.graph_numeric_ids = true;
		assert!(builder_from_config(&config, &pairs(&[("morsel_places", "0")])).is_ok());
		assert!(builder_from_config(&config, &pairs(&[("graph_numeric_ids", "false")])).is_err());
	}

	/// Stores built under different keys are never shared.
	#[test]
	fn the_group_key_holds_the_keys_of_the_run() {
		let config = TestConfig::default();
		let key =
			|run: &[(&str, &str)]| calc_group_key(&[], &config, Backend::Memory, "", &pairs(run));
		assert_eq!(key(&[]), key(&[]));
		assert_ne!(key(&[]), key(&[("morsel_split_rows", "1")]));
		assert_ne!(key(&[("morsel_split_rows", "1")]), key(&[("morsel_split_rows", "2")]));
	}
}
