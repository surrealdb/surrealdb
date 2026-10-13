mod binaries;
mod process;
mod protocol;

use std::collections::HashMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use process::{SurrealConnection, SurrealProcess};
use protocol::{ProxyObject, ProxyValue};
use semver::{Version, VersionReq};
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Array, Number, Value as SurValue, Variables};
use tokio::task::JoinSet;

use crate::cli::{ColorMode, DsVersion, ResultsMode, UpgradeBackend};
use crate::version::version_matches;
use crate::format::Progress;
use crate::tests::report::{ChainHopResult, TestGrade, TestReport, TestTaskResult};
use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::schema::{
	BoolOr, Capabilities, ENV_DEFAULT_DATABASE, ENV_DEFAULT_NAMESPACE, SchemaTarget,
};
use crate::tests::{CaseSet, RunSetBuilder, TestRun};
use crate::util::TempDir;

pub struct UpgradeTestConfig {
	from: DsVersion,
	to: DsVersion,
	/// The `to` version already resolved by `run()`. Re-deriving it per task
	/// would spawn `cargo metadata` for every scheduled run of a path-version.
	to_version: Version,
	port: u16,
	path: String,
}

impl RunConfig for UpgradeTestConfig {
	fn name(&self, case: &CaseImports) -> String {
		format!("{} {} => {}", case.test.origin.path, self.from, self.to)
	}
}

/// One cumulative walk of a single test through every version in the chain.
///
/// Unlike [`UpgradeTestConfig`], which stands up a fresh store per run, this
/// carries one data directory through the whole list: each version's binary
/// opens the store its predecessor left behind.
pub struct ChainTestConfig {
	/// Every version to walk, in order, each paired with the version it resolved
	/// to. One vector rather than two, so the pairing cannot drift.
	versions: Arc<Vec<(DsVersion, Version)>>,
	/// Index into `versions` of the first version satisfying `since`, resolved
	/// during expansion so a test with no supporting version is never scheduled.
	setup_at: usize,
	port: u16,
	path: String,
}

impl RunConfig for ChainTestConfig {
	fn name(&self, case: &CaseImports) -> String {
		format!(
			"{} chain {} => {}",
			case.test.origin.path,
			self.versions[self.setup_at].0,
			self.versions.last().expect("a chain always has a last version").0
		)
	}
}

/// The port a scheduled run should bind, so the scheduling loop can hand out
/// and recycle ports without knowing which mode it is driving.
trait UpgradeRun: RunConfig {
	fn set_port(&mut self, port: u16);
	fn port(&self) -> u16;
}

impl UpgradeRun for UpgradeTestConfig {
	fn set_port(&mut self, port: u16) {
		self.port = port;
	}

	fn port(&self) -> u16 {
		self.port
	}
}

impl UpgradeRun for ChainTestConfig {
	fn set_port(&mut self, port: u16) {
		self.port = port;
	}

	fn port(&self) -> u16 {
		self.port
	}
}

pub struct Config {
	jobs: u32,
	download_permission: bool,
	backend: UpgradeBackend,
	keep_files: bool,
	/// Canonical directory holding the built Surrealism module, when a selected
	/// test asked for one. Spawned binaries get it on their bucket allowlist and
	/// scripts get its path bound as `$module_backend`.
	module_fixture: Option<PathBuf>,
}

impl Config {
	pub fn from_matches(matches: &ArgMatches) -> Self {
		Config {
			backend: *matches.get_one::<UpgradeBackend>("backend").unwrap(),
			download_permission: matches.get_one("allow-download").copied().unwrap_or(false),
			jobs: matches.get_one::<u32>("jobs").copied().unwrap_or_else(|| {
				thread::available_parallelism().map(|x| x.get() as u32).unwrap_or(1)
			}),
			keep_files: matches.get_one::<bool>("keep-files").copied().unwrap_or(false),
			module_fixture: None,
		}
	}
}

/// Filters every upgrade run shares, whichever mode is driving.
///
/// `no_wip`/`no_results` carry the flags' own meaning: `--no-wip` excludes
/// work-in-progress tests, and `--no-results` keeps only the tests that have no
/// expectations yet, which is what makes it useful while writing new ones.
/// What the shared filters need beyond the case set itself.
struct FilterContext<'a> {
	filter: String,
	no_wip: bool,
	no_results: bool,
	chain_mode: bool,
	/// Repair-scan failures, collected here because a filter cannot borrow the
	/// load-error list the builder already holds.
	scan: &'a std::cell::RefCell<Vec<(Arc<crate::tests::Origin>, String)>>,
	/// Every version this invocation resolved, for the repair-window overlap check.
	versions: &'a [Version],
	/// False when the Surrealism fixture could not be built, in which case the
	/// tests that need it cannot run.
	have_module_fixture: bool,
}

fn base_builder<'set, 'error, 'a, T: RunConfig>(
	case_set: &'set CaseSet,
	load_errors: &'error mut Vec<crate::tests::TestLoadError>,
	ctx: FilterContext<'a>,
) -> RunSetBuilder<'set, 'error, 'a, T> {
	let FilterContext {
		filter,
		no_wip,
		no_results,
		chain_mode,
		scan,
		versions,
		have_module_fixture,
	} = ctx;
	RunSetBuilder::new(case_set, load_errors)
		.with_filter(|x| x.test.config.parsed.test.run)
		.with_filter(|x| x.test.config.parsed.test.upgrade)
		// Upgrade tests send the raw source over the SurrealQL query endpoint,
		// so only SurrealQL test cases can run here.
		.with_filter(|x| x.test.dialect == crate::tests::case::Dialect::SurrealQl)
		.with_filter(move |x| x.test.origin.path.contains(&filter))
		.with_filter(move |x| !no_wip || !x.test.config.parsed.test.wip)
		.with_filter(move |x| {
			if no_results {
				x.test.config.parsed.test.results.is_none()
			} else {
				x.test.config.parsed.test.results.is_some()
			}
		})
		// A chain run walks one store through many versions; a pairwise run
		// stands up an isolated pair. A test declares which shape it is built
		// for, and running it under the other one asserts something it was
		// never written to assert.
		.with_filter(move |x| x.test.config.parsed.test.chain.is_some() == chain_mode)
		.with_filter(move |x| have_module_fixture || !needs_module_fixture(x))
		// Cases that failed validation were already recorded as load errors,
		// which fail the whole run; dropping them here only stops them from
		// being executed as well.
		.with_filter(move |x| validate_upgrade_case(&x.test, versions).is_ok())
		.with_filter(move |x| match undeclared_repair(x).or_else(|| misplaced_legacy_syntax(x)) {
			None => true,
			Some(problem) => {
				scan.borrow_mut().push((x.test.origin.clone(), problem));
				false
			}
		})
}

/// Main subcommand function, runs the actual subcommand.
pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let config = Config::from_matches(matches);
	let mut config = Arc::new(config);
	let mut load_errors = Vec::new();

	let path: &String = matches.get_one("path").unwrap();
	let results_mode = matches.get_one::<ResultsMode>("results").unwrap();
	let filter = matches.get_one::<String>("filter").cloned().unwrap_or_default();
	let no_wip = matches.get_flag("no-wip");
	let no_results = matches.get_flag("no-results");

	// `--chain` and `--from`/`--to` are mutually exclusive at the CLI layer, so
	// exactly one of these is populated.
	let chain_versions: Vec<DsVersion> =
		matches.get_many::<DsVersion>("chain").map(|x| x.cloned().collect()).unwrap_or_default();
	let from_versions: Vec<DsVersion> =
		matches.get_many::<DsVersion>("from").map(|x| x.cloned().collect()).unwrap_or_default();
	let to_versions: Vec<DsVersion> =
		matches.get_many::<DsVersion>("to").map(|x| x.cloned().collect()).unwrap_or_default();

	let mut all_versions: Vec<_> = chain_versions
		.iter()
		.cloned()
		.chain(from_versions.iter().cloned())
		.chain(to_versions.iter().cloned())
		.collect();
	all_versions.sort_unstable();
	all_versions.dedup();

	// Check if the backend is supported by the enabled features.
	match config.backend {
		#[cfg(feature = "backend-rocksdb")]
		UpgradeBackend::RocksDb => {}
		#[cfg(not(feature = "backend-rocksdb"))]
		UpgradeBackend::RocksDb => bail!("RocksDb backend feature is not enabled"),
		#[cfg(feature = "backend-surrealkv")]
		UpgradeBackend::SurrealKv => {}
		#[cfg(not(feature = "backend-surrealkv"))]
		UpgradeBackend::SurrealKv => bail!("SurrealKV backend feature is not enabled"),
	}

	if UpgradeBackend::SurrealKv == config.backend
		&& let Some(DsVersion::Version(v)) =
			all_versions.iter().find(|x| **x < DsVersion::Version(Version::new(2, 0, 0)))
	{
		bail!(
			"Cannot run with backend surrealkv and version {v}, surrealkv was not yet available on this version"
		)
	}

	let mut actual_version = HashMap::new();
	println!("Preparing used versions of surrealdb");
	// `actual_version` resolution is cheap (string parse for tagged versions,
	// a single `cargo metadata` call for path-versions) so we do it serially.
	let mut resolved = Vec::with_capacity(all_versions.len());
	for v in &all_versions {
		resolved.push((v.clone(), binaries::actual_version(v.clone()).await?));
	}

	// `prepare()` may download tens of megabytes per version. When the user
	// has waived the interactive prompt we can fan the downloads out in
	// parallel; otherwise stay serial so prompts don't clobber each other.
	if config.download_permission {
		let mut prepares = JoinSet::new();
		for (v, _) in &resolved {
			let v = v.clone();
			prepares.spawn(async move { binaries::prepare(v, true).await });
		}
		while let Some(r) = prepares.join_next().await {
			r.context("prepare() task panicked")??;
		}
	} else {
		for (v, _) in &resolved {
			binaries::prepare(v.clone(), false).await?;
		}
	}

	for (v, actual) in resolved {
		actual_version.insert(v, actual);
	}

	let temp_dir = TempDir::new("surreal_upgrade_tests")
		.await
		.context("Failed to create temporary directory for datastore")?;

	let case_set = CaseSet::load_surrealql_files(path.as_str(), &mut load_errors).await?;

	let chain_mode = !chain_versions.is_empty();

	// Every version this invocation could ever match a `since` against.
	let all_resolved: Vec<Version> = actual_version.values().cloned().collect();

	// Built once, and only when a selected case actually asks for Surrealism —
	// the build needs a wasm toolchain, so paying for it unconditionally would
	// break every run on a machine that has none.
	// Any case at all, helpers included: the binding is used by the import that
	// opens the bucket, not by the graded body.
	let wants_modules = case_set.iter().any(|c| c.source.contains("$module_backend"));
	if wants_modules {
		match ensure_module_fixture().await {
			Ok(dir) => {
				println!("Using module fixture at '{}'", dir.display());
				Arc::get_mut(&mut config)
					.expect("config is not yet shared with any task")
					.module_fixture = Some(dir);
			}
			// Building needs a wasm toolchain that not every developer machine
			// has. Dropping the module tests keeps the rest of the suite usable
			// there, but CI sets LANG_UPGRADE_REQUIRE_MODULES so the coverage
			// cannot go missing without the job failing.
			Err(e) if std::env::var("LANG_UPGRADE_REQUIRE_MODULES").is_err() => {
				eprintln!(
					"\nWARNING: skipping every test that needs a Surrealism module — the \
					 fixture could not be built.\n{e:#}\n"
				);
			}
			Err(e) => return Err(e.context(
				"LANG_UPGRADE_REQUIRE_MODULES is set, so a missing module fixture is a failure",
			)),
		}
	}

	// Validate every upgrade case up front. A configuration mistake that merely
	// removed the test from the run set would be invisible — the suite would go
	// green having tested less — so these become load errors, which fail the run.
	for case in case_set.iter() {
		let cfg = &case.config.parsed;
		// Both keys are only ever read by this runner, and this runner only ever
		// looks at `upgrade` cases — so without `upgrade = true` the test runs
		// nowhere and every other guard declines to notice.
		if !cfg.test.upgrade && (cfg.test.chain.is_some() || !cfg.test.repair.is_empty()) {
			load_errors.push(crate::tests::TestLoadError {
				origin: case.origin.clone(),
				error: anyhow::anyhow!(
					"declares `[test.chain]` or `[[test.repair]]` without `upgrade = true`, so it \
					 would run in neither runner"
				),
			});
			continue;
		}
		if !cfg.test.upgrade {
			continue;
		}
		if let Err(error) = validate_upgrade_case(case, &all_resolved) {
			load_errors.push(crate::tests::TestLoadError {
				origin: case.origin.clone(),
				error,
			});
			continue;
		}
	}

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.expect("failed to create datastore for running matching expressions");

	let mut session = surrealdb_core::dbs::Session::default();
	ds.process_use(None, &mut session, Some("match".to_string()), Some("match".to_string()))
		.await
		.unwrap();

	let store_dir = || temp_dir.sub_dir_path().to_str().expect("Paths should be utf-8").to_owned();

	// Collected during expansion, which cannot borrow `load_errors` — the run-set
	// builder already holds it.
	let chain_conflicts: std::cell::RefCell<Vec<(Arc<crate::tests::Origin>, String)>> =
		Default::default();
	// Populated from a filter, which is the first place the imports and chain
	// scripts exist in resolved form; resolving them again here would duplicate
	// every "could not find import" diagnostic the builder already reports.
	let repair_scan: std::cell::RefCell<Vec<(Arc<crate::tests::Origin>, String)>> =
		Default::default();

	let (reports, run_count) = if chain_mode {
		let chain_actual: Vec<Version> =
			chain_versions.iter().map(|v| actual_version.get(v).unwrap().clone()).collect();
		let versions = Arc::new(
			chain_versions.iter().cloned().zip(chain_actual.iter().cloned()).collect::<Vec<_>>(),
		);

		let run_set = base_builder(
			&case_set,
			&mut load_errors,
			FilterContext {
				filter,
				no_wip,
				no_results,
				chain_mode,
				scan: &repair_scan,
				versions: &all_resolved,
				have_module_fixture: config.module_fixture.is_some(),
			},
		)
			.with_expander(|x| {
				let chain = x
					.test
					.config
					.parsed
					.test
					.chain
					.as_ref()
					.expect("filtered to chain tests above");

				// The feature does not exist before this version, so the walk
				// starts where it does. A chain containing no such version
				// cannot test the feature at all and is not scheduled.
				// `since` is the walk-specific override; otherwise the feature's
				// introducing release is exactly when setup can first run.
				let since = chain
					.since
					.as_ref()
					.or(x.test.config.parsed.test.introduced.as_ref());
				let Some(setup_at) = since
					.map(|req| chain_actual.iter().position(|v| version_matches(req, v)))
					.unwrap_or(Some(0))
				else {
					let req = since.expect("position() is only consulted when `since` is set");
					// Nothing in the walk can create the feature, so the test
					// would contribute no runs at all. Recorded rather than
					// dropped: a green job that quietly tested less is the
					// failure this suite exists to remove.
					chain_conflicts.borrow_mut().push((
						x.test.origin.clone(),
						format!(
							"no version in the walk satisfies `{req}`, so its setup could never \
							 run. Narrow the walk with the positional path filter if this is a \
							 deliberately short local chain rather than the full CI list"
						),
					));
					return Vec::new();
				};

				// A walk must not cross a version that demands a manual repair.
				// The repair replaces the structure the walk is measuring, so
				// every later version would read what the repair wrote rather
				// than what the original writer left — and `REBUILD INDEX` on an
				// index first written before 3.2.1 never returns, so the walk
				// would hang to its timeout rather than fail. Repairs belong to
				// the pairwise axis, which brackets them properly.
				if let Some(repair) = x.test.config.parsed.test.repair.iter().find(|r| {
					chain_actual[setup_at..].iter().any(|v| version_matches(&r.since, v))
				}) {
					chain_conflicts.borrow_mut().push((
						x.test.origin.clone(),
						format!(
							"declares `[[test.repair]] since = \"{}\"`, which the walked \
							 versions satisfy; a repair mid-walk replaces the structure the \
							 chain follows. Cover the repair with a pairwise test, or keep the \
							 repairing release out of LANG_UPGRADE_CHAIN",
							repair.since
						),
					));
					return Vec::new();
				}

				// A single-version "walk" spawns one binary and hands nothing
				// over, yet every hop-relative matcher holds trivially at
				// $hop == $hops == 1. It would report a cross-version property
				// that was never crossed.
				if chain_actual.len() - setup_at < 2 {
					chain_conflicts.borrow_mut().push((
						x.test.origin.clone(),
						format!(
							"the walk reaches it only at its last version ({}), so setup, \
							 mutate and the body would all run on one binary with no handover",
							chain_actual[setup_at]
						),
					));
					return Vec::new();
				}

				vec![ChainTestConfig {
					versions: versions.clone(),
					setup_at,
					// Set later.
					port: 0,
					path: store_dir(),
				}]
			})
			.build();

		let count = run_set.len();
		println!("Using directory '{}' as a store directory", temp_dir.path().display());
		println!(
			"Walking {} chain runs for {} test cases through {} versions",
			count,
			case_set.len(),
			versions.len()
		);
		let reports =
			execute_runs(run_set, config.clone(), color, &ds, run_chain_task).await;
		(reports, count)
	} else {
		let run_set = base_builder(
			&case_set,
			&mut load_errors,
			FilterContext {
				filter,
				no_wip,
				no_results,
				chain_mode,
				scan: &repair_scan,
				versions: &all_resolved,
				have_module_fixture: config.module_fixture.is_some(),
			},
		)
			.with_expander(|x| {
				let mut res = Vec::new();

				for from in from_versions.iter() {
					for to in to_versions.iter() {
						let from_v = actual_version.get(from).unwrap();
						let to_v = actual_version.get(to).unwrap();

						if let Some(ver_req) = x.test.config.parsed.test.version.as_ref()
							&& !version_matches(ver_req, to_v)
						{
							continue;
						}

						if let Some(ver_req) = x.test.config.parsed.test.importing_version.as_ref()
							&& !version_matches(ver_req, from_v)
						{
							continue;
						}

						// The writing binary has to be able to create the state in
						// the first place.
						if let Some(ver_req) = x.test.config.parsed.test.introduced.as_ref()
							&& !version_matches(ver_req, from_v)
						{
							continue;
						}

						if x.imports.iter().any(|imp| {
							imp.config
								.parsed
								.test
								.version
								.as_ref()
								.map(|req| !version_matches(req, from_v))
								.unwrap_or(false)
						}) {
							continue;
						}

						res.push(UpgradeTestConfig {
							from: from.clone(),
							to: to.clone(),
							to_version: to_v.clone(),
							// Set later.
							port: 0,
							path: store_dir(),
						})
					}
				}
				res
			})
			.build();

		let count = run_set.len();
		println!("Using directory '{}' as a store directory", temp_dir.path().display());
		println!("Running {} test runs for {} test cases", count, case_set.len());
		let reports =
			execute_runs(run_set, config.clone(), color, &ds, run_task).await;
		(reports, count)
	};

	if config.keep_files {
		temp_dir.keep();
	} else if let Err(e) = temp_dir.cleanup().await {
		println!();
		println!("Failed to clean up temporary directory:{e}");
	}

	println!();

	for v in reports.iter() {
		v.display(color);
	}

	println!();
	// A chain test excluded from the walk is a configuration mistake, not a
	// result: without this the job goes green having scheduled nothing.
	for (origin, reason) in chain_conflicts.into_inner() {
		load_errors.push(crate::tests::TestLoadError {
			origin,
			error: anyhow::anyhow!("Chain test not scheduled: {reason}"),
		});
	}
	for (origin, problem) in repair_scan.into_inner() {
		load_errors.push(crate::tests::TestLoadError {
			origin,
			error: anyhow::anyhow!("{problem}"),
		});
	}

	// Report test case loading errors.
	for e in load_errors.iter() {
		e.display(color)
	}

	match *results_mode {
		ResultsMode::Default => {}
		ResultsMode::Accept => {
			for report in reports.iter().filter(|x| x.is_unspecified_test() && !x.is_wip()) {
				report.update_config_results(path).await?;
			}
		}
		ResultsMode::Overwrite => {
			for report in reports.iter().filter(|x| {
				matches!(x.grade(), TestGrade::Failed | TestGrade::Warning) && !x.is_wip()
			}) {
				report.update_config_results(path).await?;
			}
		}
	}

	if !load_errors.is_empty() {
		bail!("Could not load all tests")
	}

	// A chain or version list that selects nothing is the quietest way this
	// suite can stop testing: every assertion still "passes" because none ran.
	// Only `--no-results`, whose whole purpose is to list tests awaiting
	// expectations, may legitimately come back empty.
	if run_count == 0 && !no_results {
		bail!(
			"No test runs were scheduled. Check the version list and the `filter` argument \
			 — an empty run set passes vacuously and is never what an upgrade job wants."
		)
	}

	if reports.iter().any(|x| x.grade() == TestGrade::Failed) {
		bail!("Not all tests were successfull")
	}

	Ok(())
}

/// Run the test body, honouring any `[[test.repair]]` that applies to `target`.
///
/// With no applicable declaration this is just the body. With one, it is a
/// bracket: run the body, require the damage the declaration claims, apply the
/// repair, then run the body again and hand *that* back for grading.
///
/// The pre-repair run is the part that matters. A repair statement written
/// straight into a test body proves nothing, because it rebuilds the structure
/// from the base records before anything reads it — the assertion that follows
/// can only see bytes the new binary just wrote. Requiring the declared damage
/// first means the test still fails if the requirement quietly goes away, so a
/// stale "you must rebuild" cannot sit in the suite pretending to be coverage.
async fn run_with_repairs(
	connection: &mut SurrealConnection,
	case: &CaseImports,
	target: &Version,
) -> anyhow::Result<TestTaskResult> {
	let cfg = &case.test.config.parsed;

	let applicable = cfg
		.test
		.repair
		.iter()
		.enumerate()
		.find(|(_, r)| version_matches(&r.since, target));

	let Some((index, repair)) = applicable else {
		return connection.query(&case.test.source).await;
	};

	let before = connection.query(&case.test.source).await?;

	let scripts = case.repairs.get(index).map(|x| x.as_slice()).unwrap_or(&[]);
	if let Some(failure) = run_scripts(connection, scripts, "repair", "").await {
		return Ok(failure);
	}

	let after = connection.query(&case.test.source).await?;

	// Whether the "before" run really shows the declared damage can only be
	// decided against the test's own expectations, which live in the reporter.
	Ok(TestTaskResult::Repair {
		index,
		since: repair.since.to_string(),
		reason: repair.reason.clone(),
		unrepaired: repair.unrepaired,
		target: target.to_string(),
		before: Box::new(before),
		after: Box::new(after),
	})
}

/// Whether two `[[test.repair]]` windows can be satisfied by one upgrade target.
///
/// `VersionReq` cannot be intersected, so the windows are compared over the
/// actual versions this invocation resolved — the whole population a `since` can
/// ever be matched against. A hand-written probe list would be a second copy of
/// the version matrix, and would silently stop covering the releases it omitted.
fn repair_windows_overlap(a: &VersionReq, b: &VersionReq, versions: &[Version]) -> bool {
	versions.iter().any(|v| version_matches(a, v) && version_matches(b, v))
}

/// Whether a capability list differs from what the schema defaults it to.
///
/// The defaults are not uniform — the `allow-*` lists default to `Bool(true)`
/// and the `deny-*` lists to `Bool(false)` — so "did the author set this?" can
/// only be answered against the default. Every default is a `Bool`, so any
/// explicit list of targets differs by construction.
fn differs<T>(field: &BoolOr<Vec<SchemaTarget<T>>>, default: &BoolOr<Vec<SchemaTarget<T>>>) -> bool {
	match (field, default) {
		(BoolOr::Bool(a), BoolOr::Bool(b)) => a != b,
		_ => true,
	}
}

/// Experimental capability targets a test asks for, as the server's own
/// `SURREAL_CAPS_ALLOW_EXPERIMENTAL` spells them.
///
/// Returns `None` when the test names no targets, which is the only shape the
/// runner can honour besides an absent capabilities block.
fn experimental_targets(caps: &crate::tests::schema::Capabilities) -> Option<Vec<String>> {
	match caps.allow_experimental.as_ref() {
		BoolOr::Bool(_) => None,
		BoolOr::Value(targets) => Some(targets.iter().map(|t| t.0.to_string()).collect()),
	}
}

/// The experimental targets to spawn a binary with for this case.
fn case_experimental(case: &CaseImports) -> Vec<String> {
	match case.test.config.parsed.env.capabilities.as_ref() {
		BoolOr::Value(caps) => experimental_targets(caps).unwrap_or_default(),
		BoolOr::Bool(_) => Vec::new(),
	}
}

/// A stamp over the module's sources, so the cache invalidates when they change.
///
/// Modification times rather than content hashes: the build is the expensive
/// part, and a spurious rebuild is far cheaper than testing a stale object.
fn module_source_stamp(dir: &Path) -> Result<String> {
	fn walk(dir: &Path, out: &mut Vec<String>) -> Result<()> {
		for entry in std::fs::read_dir(dir)? {
			let path = entry?.path();
			if path.file_name().is_some_and(|n| n == "target") {
				continue;
			}
			if path.is_dir() {
				walk(&path, out)?;
			} else {
				let modified = path.metadata()?.modified()?;
				out.push(format!("{}:{modified:?}", path.display()));
			}
		}
		Ok(())
	}
	let mut parts = Vec::new();
	walk(dir, &mut parts).context("Failed to stat the Surrealism module sources")?;
	parts.sort();
	Ok(parts.join("\n"))
}

/// Build the Surrealism demo module once and return the canonical directory
/// holding it.
///
/// A module is referenced through a file-backed bucket, so the directory has to
/// be on the spawned binary's allowlist and nameable from the test. The path is
/// canonicalised because on macOS `/var` is a symlink to `/private/var` and the
/// uncanonicalised form is rejected by the allowlist.
///
/// Building needs the `wasm32-wasip2` target and a C toolchain that can emit
/// wasm; Apple's clang cannot, so a machine without one gets an error naming the
/// environment variables that fix it rather than a compiler backtrace.
async fn ensure_module_fixture() -> Result<PathBuf> {
	let cache = Path::new(".module_cache");
	let surli = cache.join("demo.surli");
	let stamp = cache.join("demo.stamp");
	// Keyed on the module's sources, so editing the demo rebuilds it rather than
	// silently testing the previous object.
	let want = module_source_stamp(Path::new("../surrealism/demo"))?;
	let fresh = stamp.exists()
		&& surli.exists()
		&& tokio::fs::read_to_string(&stamp).await.ok().as_deref() == Some(want.as_str());
	if !fresh {
		tokio::fs::create_dir_all(cache).await.context("Failed to create the module cache")?;
		let status = tokio::process::Command::new("cargo")
			.env("RUST_LOG", "warn")
			.args(["run", "--locked", "--quiet", "--bin", "surreal", "--"])
			.args(["module", "build", "surrealism/demo", "--out"])
			// Built to a temp name and renamed on success: a build that writes
			// its output and then fails would otherwise leave a truncated file
			// that every later run accepts. The name still ends in `.surli`,
			// which the packer requires of its output.
			.arg(Path::new("language-tests").join(cache.join("demo.partial.surli")))
			.current_dir("..")
			.status()
			.await
			.context("Failed to spawn the module build")?;
		if !status.success() {
			bail!(
				"Building the Surrealism module fixture failed. It needs the wasm32-wasip2 \
				 target (`rustup target add wasm32-wasip2`) and a C toolchain that can emit \
				 wasm — Apple's clang cannot, so on macOS export \
				 CC_wasm32_wasip2=/opt/homebrew/opt/llvm/bin/clang and \
				 AR_wasm32_wasip2=/opt/homebrew/opt/llvm/bin/llvm-ar."
			)
		}
		tokio::fs::rename(cache.join("demo.partial.surli"), &surli)
			.await
			.context("Failed to move the built module into the cache")?;
		tokio::fs::write(&stamp, want).await.context("Failed to record the module stamp")?;
	}
	std::fs::canonicalize(cache).context("Failed to canonicalize the module cache path")
}

/// `LET` binding for scripts that define the bucket a module is loaded from.
///
/// Bound as a complete `file:` backend URL rather than a bare path, because that
/// is what `DEFINE BUCKET ... BACKEND` takes. Empty when no fixture is in play.
fn module_prelude(dir: Option<&Path>) -> String {
	match dir {
		Some(dir) => format!("LET $module_backend = 'file:{}';\n", dir.display()),
		None => String::new(),
	}
}

/// Whether a case needs the built Surrealism module, as opposed to merely the
/// capability.
///
/// Keyed on the binding the fixture provides rather than on the capability: a
/// test can define and read module *definitions* without any wasm object
/// existing, and only one that opens a bucket over the fixture directory needs
/// the build to have succeeded.
fn needs_module_fixture(case: &CaseImports) -> bool {
	std::iter::once(&case.test)
		.chain(case.imports.iter())
		.chain(case.chain_setup.iter())
		.chain(case.chain_mutate.iter())
		.any(|script| script.source.contains("$module_backend"))
}

/// Reject configuration the upgrade runner cannot honour.
///
/// The spawned binary takes its capabilities, credentials and endpoint from the
/// runner's own fixed command line, so a test that sets any of these is not
/// getting what it asked for. Silently ignoring them is worse than refusing:
/// the author writes an expectation for a session that never existed, and the
/// test passes for the wrong reason.
fn validate_upgrade_case(
	case: &Arc<crate::tests::case::TestCase>,
	versions: &[Version],
) -> Result<()> {
	let cfg = &case.config.parsed;

	let unused = cfg.unused_keys();
	if !unused.is_empty() {
		bail!("Unknown configuration key(s): {}", unused.join(", "))
	}

	// `allow-experimental` is the only capability that reaches a spawned binary,
	// through SURREAL_CAPS_ALLOW_EXPERIMENTAL. Every other field is compared
	// against the default and refused when set, because accepting it would leave
	// the test asserting against capabilities the binary never had.
	if let BoolOr::Value(caps) = cfg.env.capabilities.as_ref() {
		let defaults = Capabilities::default();
		// Every field except `allow-experimental`, each named so the error can
		// point at the one that was set rather than at the block as a whole.
		let set: Vec<&str> = [
			("scripting", caps.scripting != defaults.scripting),
			("quest-access", caps.quest_access != defaults.quest_access),
			(
				"live-query-notifications",
				caps.live_query_notifications != defaults.live_query_notifications,
			),
			("allow-functions", differs(&caps.allow_functions, &defaults.allow_functions)),
			("deny-functions", differs(&caps.deny_functions, &defaults.deny_functions)),
			("allow-net", differs(&caps.allow_net, &defaults.allow_net)),
			("deny-net", differs(&caps.deny_net, &defaults.deny_net)),
			("allow-rpc", differs(&caps.allow_rpc, &defaults.allow_rpc)),
			("deny-rpc", differs(&caps.deny_rpc, &defaults.deny_rpc)),
			("allow-http", differs(&caps.allow_http, &defaults.allow_http)),
			("deny-http", differs(&caps.deny_http, &defaults.deny_http)),
			("deny-experimental", differs(&caps.deny_experimental, &defaults.deny_experimental)),
			("allow-arbitrary-query", differs(&caps.allow_arbitrary_query, &defaults.allow_arbitrary_query)),
			("deny-arbitrary-query", differs(&caps.deny_arbitrary_query, &defaults.deny_arbitrary_query)),
			("allow-eval-query", differs(&caps.allow_eval_query, &defaults.allow_eval_query)),
			("deny-eval-query", differs(&caps.deny_eval_query, &defaults.deny_eval_query)),
		]
		.into_iter()
		.filter_map(|(name, changed)| changed.then_some(name))
		.collect();
		if !set.is_empty() {
			bail!(
				"`[env.capabilities]` key(s) {} have no effect in upgrade tests: the spawned \
				 binary takes its capabilities from the runner's own fixed command line, and \
				 only `allow-experimental` can be handed to it. Remove them, or the test \
				 asserts against capabilities the binary never had.",
				set.join(", ")
			)
		}
		match experimental_targets(caps) {
			Some(t) if !t.is_empty() => {}
			_ => bail!(
				"`[env.capabilities] allow-experimental` must be an explicit non-empty list of \
				 targets, such as `[\"files\"]`. The value is handed to the spawned binary as \
				 SURREAL_CAPS_ALLOW_EXPERIMENTAL=<comma-list>, so the shorthand boolean form \
				 has nothing to pass on."
			),
		}
	}

	let mut ignored = Vec::new();
	if cfg.env.auth.is_some() {
		ignored.push("env.auth");
	}
	if cfg.env.signin.is_some() {
		ignored.push("env.signin");
	}
	if cfg.env.signup.is_some() {
		ignored.push("env.signup");
	}
	if cfg.env.versioned {
		ignored.push("env.versioned");
	}
	if !cfg.env.backend.is_empty() {
		ignored.push("env.backend");
	}
	if let BoolOr::Value(_) = cfg.env.timeout {
		ignored.push("env.timeout");
	}
	// A walk builds its fixture with `[test.chain] setup`; imports are replayed
	// only on the pairwise path, so a chain test declaring them would walk a
	// store nothing ever wrote to.
	if cfg.test.chain.is_some() && !cfg.env.imports.is_empty() {
		ignored.push("env.imports (a chain test builds its fixture with `[test.chain] setup`)");
	}
	if !ignored.is_empty() {
		bail!(
			"Configuration key(s) {} have no effect in upgrade tests: the runner always \
			 connects as root over the spawned binary's own endpoint. Remove them or the \
			 test asserts against a session it never had.",
			ignored.join(", ")
		)
	}

	// Only one declaration is ever applied, so two matching the same target would
	// leave the store half-repaired and grade it as though it were whole.
	for (i, a) in cfg.test.repair.iter().enumerate() {
		for b in cfg.test.repair.iter().skip(i + 1) {
			if repair_windows_overlap(&a.since, &b.since, versions) {
				bail!(
					"`[[test.repair]]` declarations `{}` and `{}` can both match one upgrade \
					 target. Only the first would be applied, so the second's requirement would \
					 go unexercised while the run still graded green. Make the ranges disjoint.",
					a.since,
					b.since
				)
			}
		}
	}

	if cfg.test.chain.is_some() && cfg.test.results.is_none() {
		bail!("A `[test.chain]` test must declare `[[test.results]]`: the body is the per-hop check")
	}

	Ok(())
}

/// Refuse a repair statement that no `[[test.repair]]` declares.
///
/// The body is not the only place one can hide: a chain `mutate` runs on the new
/// binary before the body at *every* hop, so a repair there launders exactly the
/// bytes the walk exists to read, and `setup` does the same once.
///
/// `[env] imports` are deliberately not scanned. They replay against the *old*
/// binary during the import phase, so a rebuild there leaves the index in the
/// old format — which is the state the test then reads. There is nothing to
/// launder.
///
/// Declaring one repair does not exempt the rest of the test: the scripts named
/// by `[[test.repair]]` are skipped by identity, and a repair statement anywhere
/// else is still refused.
fn undeclared_repair(case: &CaseImports) -> Option<String> {
	let declared: Vec<&str> = case
		.repairs
		.iter()
		.flatten()
		.map(|script| script.origin.path.as_str())
		.collect();

	let mut sources: Vec<(String, &str, &str)> =
		vec![("Test body".to_owned(), case.test.origin.path.as_str(), body_of(&case.test))];
	for (label, scripts) in [("Chain setup", &case.chain_setup), ("Chain mutate", &case.chain_mutate)]
	{
		for script in scripts {
			sources.push((
				format!("{label} `{}`", script.origin.path),
				script.origin.path.as_str(),
				body_of(script),
			));
		}
	}

	for (label, path, source) in sources {
		if declared.contains(&path) {
			continue;
		}
		for stmt in REPAIR_STATEMENTS {
			if source.to_uppercase().contains(stmt) {
				return Some(format!(
					"{label} contains `{stmt}`, which no `[[test.repair]]` of this test \
					 declares. A repair statement hides whatever the upgrade did to the data \
					 the old binary wrote — in a chain `mutate` it launders those bytes before \
					 every single hop. Move it into a script named by a `[[test.repair]]`, with \
					 `since`, `unrepaired` and a `reason`, so the runner can check the claim \
					 holds and stops passing once it does not."
				));
			}
		}
	}
	None
}

/// Refuse `[test] legacy-syntax` on a script the *new* binary executes.
///
/// The flag says a script is written in a released binary's SurrealQL, and it
/// takes the file out of the current parser's language-test corpus check. That
/// is only true of a script replayed against a spawned release — an `[env]
/// imports` entry, or a `[test.chain] setup`. A test body runs on the upgrade
/// target, and a chain `mutate` runs on every version at or after the setup, so
/// both are read by the current parser and must stay within what it accepts.
fn misplaced_legacy_syntax(case: &CaseImports) -> Option<String> {
	let mut offenders = Vec::new();
	if case.test.config.parsed.test.legacy_syntax {
		offenders.push("Test body".to_owned());
	}
	for script in &case.chain_mutate {
		if script.config.parsed.test.legacy_syntax {
			offenders.push(format!("Chain mutate `{}`", script.origin.path));
		}
	}
	if offenders.is_empty() {
		return None;
	}
	Some(format!(
		"{} declares `[test] legacy-syntax`, which only an upgrade helper replayed against a \
		 spawned release may do. This script runs on the upgrade target, so it has to be written \
		 in the SurrealQL that build parses; the flag would only hide it from the parser's \
		 corpus check.",
		offenders.join(", ")
	))
}

/// A case file's SurrealQL, with its `/** … */` config block removed so prose in
/// a `reason` cannot trip a statement scan.
///
/// Sliced by the parsed config range rather than by searching for the delimiters:
/// several files in this tree open with a blank line, so the block does not
/// start at byte zero.
fn body_of(case: &crate::tests::case::TestCase) -> &str {
	match case.config.range.as_ref() {
		Some(range) => &case.source[range.end.min(case.source.len())..],
		None => &case.source,
	}
}

/// Statements that repopulate durable state from the base records, erasing the
/// difference between what an old binary wrote and what the new one can read.
const REPAIR_STATEMENTS: &[&str] = &["REBUILD INDEX"];

/// Schedule runs across `config.jobs` workers, recycling ports as they finish.
async fn execute_runs<T, F, Fut>(
	run_set: Vec<TestRun<T>>,
	config: Arc<Config>,
	color: ColorMode,
	ds: &Datastore,
	task: F,
) -> Vec<TestReport>
where
	T: UpgradeRun + Send + 'static,
	F: Fn(TestRun<T>, Arc<Config>) -> Fut,
	Fut: Future<Output = (TestRun<T>, TestTaskResult)> + Send + 'static,
{
	let mut progress = Progress::from_stderr(run_set.len(), color);
	let mut reports = Vec::new();

	// Port distribution variables.
	let mut start_port = 9000u16;
	let mut reuse_port = Vec::<u16>::new();

	// The join set to spawn futures into and await.
	let mut join_set = JoinSet::new();
	let mut run_iter = run_set.into_iter();

	loop {
		while join_set.len() < config.jobs as usize {
			// Schedule new tasks.
			match run_iter.next() {
				Some(mut run) => {
					// find a port.
					let port = reuse_port.pop().or_else(|| {
						while let Some(x) = start_port.checked_add(1) {
							start_port = x;
							if is_port_available(x) {
								return Some(x);
							}
						}
						None
					});

					let Some(port) = port else {
						// wait for some more ports to be available.
						break;
					};

					run.config.set_port(port);

					let id = run.id;
					let name = run.name();
					join_set.spawn(task(run, config.clone()));
					progress.start_item(id, &name).unwrap();
				}
				_ => {
					break;
				}
			}
		}

		let Some(res) = join_set.join_next().await else {
			break;
		};

		let (run, result) = res.unwrap();
		reuse_port.push(run.config.port());

		let id = run.id;
		let report = TestReport::from_test_result(run, result, ds).await;
		let grade = report.grade();
		reports.push(report);
		progress.finish_item(id, grade).unwrap();
	}

	reports
}

fn is_port_available(port: u16) -> bool {
	std::net::TcpListener::bind(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), port)).is_ok()
}

/// Select the namespace and database, authenticate as root, and make sure both
/// exist.
///
/// `phase` names the side of the upgrade in error messages ("importing",
/// "upgrading", or the version being walked), which is the only thing that
/// tells a reader which binary refused.
///
/// The namespace and database are created explicitly rather than left to the
/// first write. That is not dependable across the released binaries the chain
/// spans, and when it does not happen every following statement fails with
/// "The namespace 'test' does not exist" against a store nothing ever wrote to.
/// Provisioning runs after `signin` because defining a namespace requires root.
async fn establish_session(
	connection: &mut SurrealConnection,
	namespace: Option<&str>,
	database: Option<&str>,
	phase: &str,
) -> anyhow::Result<()> {
	let mut params = Vec::new();
	if let Some(ns) = namespace {
		params.push(ProxyValue::from(ns))
	}
	if let Some(db) = database {
		params.push(ProxyValue::from(db))
	}

	if !params.is_empty() {
		let mut req = ProxyObject::default();
		req.insert("method".to_owned(), ProxyValue::from("use"));
		req.insert("params".to_owned(), ProxyValue::from(params));

		let resp = connection
			.request(req)
			.await
			.with_context(|| format!("Failed to set namespace/database on {phase} database"))?;
		if let Err(e) = resp.result {
			bail!("Failed to set namespace/database on {phase} database: {}", e.message())
		}
	}

	let mut credentials = ProxyObject::default();
	credentials.insert("user".to_owned(), ProxyValue::from("root"));
	credentials.insert("pass".to_owned(), ProxyValue::from("root"));

	let mut req = ProxyObject::default();
	req.insert("method".to_owned(), ProxyValue::from("signin"));
	req.insert("params".to_owned(), ProxyValue::from(vec![ProxyValue::from(credentials)]));

	let resp = connection
		.request(req)
		.await
		.with_context(|| format!("Failed to authenticate on {phase} database"))?;
	if let Err(e) = resp.result {
		bail!("Failed to authenticate on {phase} database: {}", e.message())
	}

	let mut provision = String::new();
	if let Some(ns) = namespace {
		provision.push_str(&format!("DEFINE NAMESPACE IF NOT EXISTS `{ns}`;"));
	}
	if let Some(db) = database {
		provision.push_str(&format!("DEFINE DATABASE IF NOT EXISTS `{db}`;"));
	}
	if !provision.is_empty() {
		match connection.query(&provision).await {
			Ok(TestTaskResult::Results {
				res,
				..
			}) => {
				if let Some(e) = res.iter().find_map(|x| x.as_ref().err()) {
					bail!("Failed to provision namespace/database on {phase} database: {e}")
				}
			}
			Ok(TestTaskResult::RunningError(e)) => {
				bail!("Failed to provision namespace/database on {phase} database: {e:?}")
			}
			Err(e) => {
				bail!("Failed to provision namespace/database on {phase} database: {e:?}")
			}
			_ => {}
		}
	}

	Ok(())
}

/// Replay a list of scripts, stopping at the first statement that errors.
///
/// Returns `Some` describing the offending script when one fails. A completed
/// round-trip still carries one result per statement, any of which may be an
/// error: without inspecting them a script whose statements all failed counts
/// as clean and only resurfaces later as an unrelated "namespace does not
/// exist", with the real cause never reported.
async fn run_scripts(
	connection: &mut SurrealConnection,
	scripts: &[Arc<crate::tests::case::TestCase>],
	label: &str,
	prelude: &str,
) -> Option<TestTaskResult> {
	for script in scripts {
		let source = if prelude.is_empty() {
			script.source.clone()
		} else {
			format!("{prelude}{}", script.source)
		};
		match connection.query(&source).await {
			Ok(TestTaskResult::RunningError(e)) => {
				return Some(TestTaskResult::Import(
					script.origin.path.clone(),
					format!("Failed to run {label}: {e:?}"),
				));
			}
			Ok(TestTaskResult::Results {
				res,
				..
			}) => {
				if let Some(e) = res.iter().find_map(|x| x.as_ref().err()) {
					return Some(TestTaskResult::Import(
						script.origin.path.clone(),
						format!("A {label} statement returned an error: {e}"),
					));
				}
			}
			Err(e) => {
				return Some(TestTaskResult::Import(
					script.origin.path.clone(),
					format!("Failed to run {label}: {e:?}."),
				));
			}
			_ => {}
		}
	}
	None
}

async fn run_imports(
	run: &TestRun<UpgradeTestConfig>,
	process: &mut SurrealProcess,
	namespace: Option<&str>,
	database: Option<&str>,
	prelude: &str,
) -> anyhow::Result<Option<TestTaskResult>> {
	let mut connection = process
		.open_connection()
		.await
		.context("Failed to open connection to the importing database")?;

	process
		.assert_running_while(async {
			establish_session(&mut connection, namespace, database, "importing").await?;
			Ok(run_scripts(&mut connection, &run.case.imports, "import", prelude).await)
		})
		.await?
}

async fn run_upgrade_test(
	run: &TestRun<UpgradeTestConfig>,
	mut process: SurrealProcess,
	namespace: Option<&str>,
	database: Option<&str>,
) -> anyhow::Result<TestTaskResult> {
	let mut connection = match process.open_connection().await {
		Ok(x) => x,
		Err(e) => {
			let e = if let Some(out) = process.quit_with_output().await? {
				e.context(format!("Failed to open connection to upgrading database\n\n{}", out))
			} else {
				e.context("Failed to open connection to upgrading database")
			};
			return Err(e);
		}
	};

	process
		.assert_running_while(async {
			establish_session(&mut connection, namespace, database, "upgrading").await?;
			run_with_repairs(&mut connection, &run.case, &run.config.to_version).await
		})
		.await?
}

/// Ceiling on a single run: spawn the source binary, replay its imports, restart
/// on the target binary and run the test.
///
/// Nothing in the run is otherwise bounded — `assert_running_while` waits as long
/// as the spawned process is alive, so a statement that never returns stalls that
/// run forever and, with enough of them, the whole job. A run that reaches this
/// limit is reported as a failure. The value is far above what any test legitimately
/// needs; hops complete 45 runs in seconds.
const RUN_TIMEOUT: Duration = Duration::from_secs(300);

/// Ceiling on a whole chain walk.
///
/// A walk spawns one binary per version in sequence, so its budget has to be
/// the per-hop budget times the number of hops. Anything tighter turns a slow
/// but healthy walk into a spurious failure as the release list grows.
fn chain_timeout(hops: usize) -> Duration {
	RUN_TIMEOUT * hops.max(1) as u32
}

async fn run_chain_task(
	run: TestRun<ChainTestConfig>,
	config: Arc<Config>,
) -> (TestRun<ChainTestConfig>, TestTaskResult) {
	let budget = chain_timeout(run.config.versions.len() - run.config.setup_at);
	match tokio::time::timeout(budget, run_chain_task_inner(&run, config)).await {
		Ok(Ok(x)) => (run, x),
		Ok(Err(e)) => (run, TestTaskResult::RunningError(e)),
		Err(_) => (
			run,
			TestTaskResult::RunningError(anyhow::anyhow!(
				"Chain did not finish within {}s; a version in the walk did not return from a statement",
				budget.as_secs()
			)),
		),
	}
}

/// Walk one datastore through every version in the chain.
///
/// The feature is created once, by the first version that supports it, and from
/// there every later version opens the store its predecessor closed: it applies
/// `mutate` — so each release writes *through* a structure an older release
/// created — and then runs the body, which is graded identically at every stop.
/// The result is the property the pairwise mode cannot express: not "3.2.4 wrote
/// this and 3.3 can read it", but "3.1.0 wrote this, six releases maintained it,
/// and today's build still agrees about what is in there".
async fn run_chain_task_inner(
	run: &TestRun<ChainTestConfig>,
	config: Arc<Config>,
) -> Result<TestTaskResult> {
	let dir = &run.config.path;
	tokio::fs::create_dir(dir).await.context("Failed to create tempory directory for datastore")?;

	let cfg = &run.case.test.config.parsed;
	let namespace = cfg.env.namespace.as_ref().map(|x| x.as_ref()).into_value(ENV_DEFAULT_NAMESPACE);
	let database = cfg.env.database.as_ref().map(|x| x.as_ref()).into_value(ENV_DEFAULT_DATABASE);
	if database.is_some() && namespace.is_none() {
		bail!("Cannot have a database set but not a namespace.")
	}

	let experimental = case_experimental(&run.case);
	// Only a case that opens a bucket over the fixture gets the grants it needs.
	let module_dir =
		needs_module_fixture(&run.case).then(|| config.module_fixture.as_deref()).flatten();
	let versions = &run.config.versions;
	let walked = &versions[run.config.setup_at..];
	let hops = walked.len();

	if config.keep_files {
		let listing =
			walked.iter().map(|(v, _)| v.to_string()).collect::<Vec<_>>().join(" -> ");
		tokio::fs::write(Path::new(dir).join("test.info"), format!("chain {listing}")).await?;
	}

	let mut results = Vec::with_capacity(hops);

	for (offset, (version, actual)) in walked.iter().enumerate() {
		let hop = offset + 1;
		let phase = format!("chain hop {hop}/{hops} ({version})");

		// Spawning is retried once, as the pairwise path does. A binary can be
		// slow to accept connections under load, and the port a worker was handed
		// can be taken between the availability check and the child's bind. A
		// whole walk is far too expensive to lose to either.
		let mut retried = false;
		let (mut process, mut connection) = loop {
			let mut process = process::SurrealProcess::new(
				&config,
				version,
				dir,
				run.config.port,
				&experimental,
				module_dir,
			)
			.await
			.with_context(|| format!("Failed to spawn at {phase}"))?;
			match process.open_connection().await {
				Ok(connection) => break (process, connection),
				Err(e) => {
					let output = process.quit_with_output().await?;
					if retried {
						let e = match output {
							Some(out) => e.context(format!(
								"Failed to open connection at {phase}\n\n{out}"
							)),
							None => e.context(format!("Failed to open connection at {phase}")),
						};
						return Err(e);
					}
					retried = true;
				}
			}
		};

		// Bindings a matcher expression can use to state a hop-relative
		// expectation, so a body whose data grows under `mutate` can still
		// assert exactly rather than settling for a `>=` floor.
		// Setup and mutate are not graded, so the bindings can simply be `LET`
		// statements in front of the script. The body *is* graded, and gets the
		// same values as matcher variables instead.
		let prelude = format!(
			"{}LET $hop = {hop}; LET $hops = {hops}; LET $version = '{actual}';\n",
			module_prelude(module_dir)
		);

		let mut vars = Variables::default();
		vars.insert("hop".to_owned(), SurValue::Number(Number::Int(hop as i64)));
		vars.insert("hops".to_owned(), SurValue::Number(Number::Int(hops as i64)));
		vars.insert("version".to_owned(), SurValue::String(actual.to_string()));
		vars.insert(
			"chain".to_owned(),
			SurValue::Array(Array::from(
				walked.iter().map(|(v, _)| SurValue::String(v.to_string())).collect::<Vec<_>>(),
			)),
		);

		let outcome = process
			.assert_running_while(async {
				establish_session(&mut connection, namespace, database, &phase).await?;

				// The very first version in the walk is the one that supports
				// the feature; it creates it, and everything after inherits it.
				if offset == 0
					&& let Some(failure) =
						run_scripts(&mut connection, &run.case.chain_setup, "chain setup", &prelude).await
				{
					return Ok(Err(failure));
				}

				if let Some(failure) =
					run_scripts(&mut connection, &run.case.chain_mutate, "chain mutate", &prelude).await
				{
					return Ok(Err(failure));
				}

				run_with_repairs(&mut connection, &run.case, actual).await.map(Ok)
			})
			.await
			.with_context(|| format!("The binary quit during {phase}"))?
			.with_context(|| format!("A statement failed at {phase}"))?;

		// SIGTERM rather than a drop-kill: the next version has to open this
		// store, and a walk that hard-kills every binary would be testing
		// crash recovery instead of upgrade.
		process.quit().await.with_context(|| format!("Failed to stop the binary at {phase}"))?;

		match outcome {
			// A setup, mutate or repair script failed. Nothing later in the
			// walk can mean anything, so stop and report this hop.
			Err(failure) => {
				results.push(ChainHopResult {
					version: version.to_string(),
					hop,
					hops,
					vars,
					result: failure,
				});
				break;
			}
			Ok(result) => results.push(ChainHopResult {
				version: version.to_string(),
				hop,
				hops,
				vars,
				result,
			}),
		}
	}

	if !config.keep_files {
		let _ = tokio::fs::remove_dir_all(dir).await;
	}

	Ok(TestTaskResult::Chain(results))
}

async fn run_task(
	run: TestRun<UpgradeTestConfig>,
	config: Arc<Config>,
) -> (TestRun<UpgradeTestConfig>, TestTaskResult) {
	// Dropping the timed-out future drops the `SurrealProcess` handles, which are
	// `kill_on_drop`, so the spawned binaries do not outlive the run.
	match tokio::time::timeout(RUN_TIMEOUT, run_task_inner(&run, config)).await {
		Ok(Ok(x)) => (run, x),
		Ok(Err(e)) => (run, TestTaskResult::RunningError(e)),
		Err(_) => (
			run,
			TestTaskResult::RunningError(anyhow::anyhow!(
				"Run did not finish within {}s; the database did not return from a statement",
				RUN_TIMEOUT.as_secs()
			)),
		),
	}
}

async fn run_task_inner(
	run: &TestRun<UpgradeTestConfig>,
	config: Arc<Config>,
) -> Result<TestTaskResult> {
	let dir = &run.config.path;
	tokio::fs::create_dir(dir).await.context("Failed to create tempory directory for datastore")?;

	// no need to write the info if it is going to be deleted anyway.
	if config.keep_files {
		// write some info to the test directory usefull for later debugging.
		tokio::fs::write(
			Path::new(dir).join("test.info"),
			format!("{} => {}", run.config.from, run.config.to),
		)
		.await?;
	}

	let namespace = run
		.case
		.test
		.config
		.parsed
		.env
		.namespace
		.as_ref()
		.map(|x| x.as_ref())
		.into_value(ENV_DEFAULT_NAMESPACE);
	let database = run
		.case
		.test
		.config
		.parsed
		.env
		.database
		.as_ref()
		.map(|x| x.as_ref())
		.into_value(ENV_DEFAULT_DATABASE);

	if database.is_some() && namespace.is_none() {
		bail!("Cannot have a database set but not a namespace.")
	}

	let experimental = case_experimental(&run.case);
	// Only a case that opens a bucket over the fixture gets the grants it needs.
	let module_dir =
		needs_module_fixture(&run.case).then(|| config.module_fixture.as_deref()).flatten();
	let prelude = module_prelude(module_dir);

	// run imports
	let mut process =
		process::SurrealProcess::new(
			&config,
			&run.config.from,
			dir,
			run.config.port,
			&experimental,
			module_dir,
		)
			.await?;
	match run_imports(run, &mut process, namespace, database, &prelude).await {
		Ok(Some(x)) => return Ok(x),
		Ok(None) => {}
		Err(e) => {
			let e = if let Some(out) = process.quit_with_output().await? {
				e.context(out)
			} else {
				e
			};
			return Err(e.context("failed to run imports"));
		}
	};
	process.quit().await?;

	// run tests on existing dataset.
	// Loop is a workaround for some cases where the start up of the database suddenly becomes
	// excessivly slow, so we just retry once in this case.
	let mut retried = false;
	let result = loop {
		let process =
			process::SurrealProcess::new(
			&config,
			&run.config.to,
			dir,
			run.config.port,
			&experimental,
			module_dir,
		)
			.await?;
		match run_upgrade_test(run, process, namespace, database).await {
			Ok(x) => break x,
			Err(e) => {
				if retried || !format!("{:#?}", e).contains("open connection") {
					return Err(e);
				} else {
					retried = true;
					continue;
				}
			}
		}
	};

	if !config.keep_files {
		let _ = tokio::fs::remove_dir_all(dir).await;
	}

	Ok(result)
}

#[cfg(test)]
mod version_matches_tests {
	use super::*;

	fn check(req: &str, version: &str) -> bool {
		version_matches(&VersionReq::parse(req).unwrap(), &Version::parse(version).unwrap())
	}

	#[test]
	fn plain_requirement_spans_releases_and_prereleases() {
		assert!(check(">=3.0.1", "3.0.5"));
		assert!(check(">=3.0.1", "3.1.0-beta.1"));
		assert!(!check(">=3.0.1", "3.0.0"));
	}

	#[test]
	fn prerelease_requirement_still_gates_its_own_release() {
		// The reason the strip-and-retry is withheld at all: within 3.1.0 the
		// prerelease identifier is what is being compared.
		assert!(!check(">=3.1.0-beta.3", "3.1.0-beta.1"));
		assert!(check(">=3.1.0-beta.3", "3.1.0-beta.3"));
		assert!(check(">=3.1.0-beta.3", "3.1.0"));
	}

	#[test]
	fn prerelease_requirement_does_not_gate_later_releases() {
		// Withholding the retry here is what silently removed DISKANN coverage
		// from the final hop once a 3.3 beta entered the chain.
		assert!(check(">=3.1.0-beta.3", "3.3.0-beta.3"));
		assert!(check(">=3.1.0-beta.3", "3.4.0-nightly"));
		assert!(check(">=3.1.0-beta.3", "3.2.4"));
	}

	#[test]
	fn bounded_window_excludes_the_release_that_ends_it() {
		// The shape `[[test.repair]]` uses: applies to the 3.3.0 betas, and must
		// not leak onto the current checkout, where no repair is required.
		assert!(check(">=3.3.0-beta.1, <3.4.0", "3.3.0-beta.3"));
		assert!(check(">=3.3.0-beta.1, <3.4.0", "3.3.0"));
		assert!(!check(">=3.3.0-beta.1, <3.4.0", "3.4.0-nightly"));
		assert!(!check(">=3.3.0-beta.1, <3.4.0", "3.2.4"));
	}

	#[test]
	fn nightly_gate_selects_only_the_current_checkout() {
		assert!(check(">=3.4.0-a", "3.4.0-nightly"));
		assert!(!check(">=3.4.0-a", "3.3.0-beta.3"));
		assert!(!check(">=3.4.0-a", "3.2.4"));
	}
}

/// Every upgrade test's filename must open with the release that introduced the
/// feature it covers, zero-padded to two digits per component so a plain
/// lexicographic listing is chronological (`3.01.01` < `3.02.00` < `3.10.00`).
///
/// The prefix is a mirror of `[test] introduced`, and a mirror that nothing
/// checks is exactly how a filename convention drifts from the code — this
/// suite already lost a documented `*_plan.surql` naming to that within a few
/// commits. So the two are compared here, and a helper (`_import`, `_setup`,
/// `_mutate`, `_rebuild`) is required to carry its parent test's prefix so the
/// pair stays adjacent in a listing.
///
/// `3.00.00` means the feature predates the oldest binary the matrix tests, and
/// is what an absent `introduced` resolves to.
#[cfg(test)]
mod filename_prefix_tests {
	use std::path::Path;

	use semver::VersionReq;

	const UPGRADE_TESTS: &str = "tests/upgrade";

	/// Earliest stable release satisfying `req`, formatted as a filename prefix.
	/// A prerelease bound resolves to the release it leads to: DISKANN arrived in
	/// 3.1.0-beta.3, and the walk first creates it on 3.1.0.
	fn prefix_for(req: &VersionReq) -> Option<String> {
		// The lower bound, whichever order the window was written in: an upper
		// bound names the release the feature stops applying to, not the one
		// that introduced it.
		let c = req.comparators.iter().find(|c| {
			matches!(
				c.op,
				semver::Op::GreaterEq
					| semver::Op::Greater | semver::Op::Caret
					| semver::Op::Tilde | semver::Op::Exact
			)
		})?;
		Some(format!("{}.{:02}.{:02}", c.major, c.minor.unwrap_or(0), c.patch.unwrap_or(0)))
	}

	fn parent_test_of(name: &str) -> String {
		for suffix in ["_import", "_setup", "_mutate", "_rebuild"] {
			if let Some(stem) = name.strip_suffix(".surql")
				&& let Some(base) = stem.strip_suffix(suffix)
			{
				return format!("{base}.surql");
			}
		}
		name.to_owned()
	}

	fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
		for entry in std::fs::read_dir(dir).expect("upgrade test directory is readable") {
			let path = entry.expect("readable dir entry").path();
			if path.is_dir() {
				walk(&path, out);
			} else if path.extension().is_some_and(|e| e == "surql") {
				out.push(path);
			}
		}
	}

	#[test]
	fn every_upgrade_test_is_prefixed_with_its_introducing_release() {
		let mut files = Vec::new();
		walk(Path::new(UPGRADE_TESTS), &mut files);
		assert!(!files.is_empty(), "found no upgrade tests to check");

		let mut problems = Vec::new();
		for path in &files {
			let name = path.file_name().unwrap().to_str().unwrap();
			let Some((prefix, rest)) = name.split_once('_') else {
				problems.push(format!("{}: no `<version>_` prefix", path.display()));
				continue;
			};
			// Every component a number, and minor and patch two digits wide, or
			// the listing stops sorting chronologically the first time a minor
			// reaches double figures.
			let parts: Vec<&str> = prefix.split('.').collect();
			let shaped = parts.len() == 3
				&& parts.iter().all(|p| !p.is_empty() && p.parse::<u64>().is_ok())
				&& parts[1].len() == 2
				&& parts[2].len() == 2;
			if !shaped {
				problems.push(format!(
					"{}: prefix `{prefix}` is not `<major>.<NN>.<NN>` with numeric, \
					 two-digit minor and patch",
					path.display()
				));
				continue;
			}

			// A helper takes its parent's prefix; only the test itself declares
			// the fact.
			let parent = parent_test_of(rest);
			let owner = path.with_file_name(format!("{prefix}_{parent}"));
			let source = std::fs::read_to_string(&owner).unwrap_or_else(|_| {
				problems.push(format!(
					"{}: no parent test `{}` beside it",
					path.display(),
					owner.display()
				));
				String::new()
			});
			if source.is_empty() {
				continue;
			}

			let declared = source
				.lines()
				.find_map(|l| l.trim().strip_prefix("introduced = \""))
				.and_then(|l| l.split('"').next())
				.map(|r| VersionReq::parse(r).expect("`introduced` parses as a VersionReq"));

			let expected = match declared.as_ref() {
				None => "3.00.00".to_owned(),
				Some(req) => match prefix_for(req) {
					Some(p) => p,
					None => {
						problems.push(format!(
							"{}: `introduced = \"{req}\"` has no lower bound to name a prefix \
							 after",
							path.display()
						));
						continue;
					}
				},
			};
			if prefix != expected {
				problems.push(format!(
					"{}: prefix is `{prefix}` but `introduced` says `{expected}`",
					path.display()
				));
			}
		}

		assert!(problems.is_empty(), "filename prefixes out of step:\n  {}", problems.join("\n  "));
	}
}
