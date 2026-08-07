use std::fmt::Write;
use std::io::IsTerminal;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use futures::FutureExt as _;
use provisioner::Provisioner;
use semver::Version;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::ExperimentalTarget;
use surrealdb_core::env::VERSION;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::{gql, syn};
use surrealdb_types::Value as SurValue;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::cli::{Backend, ColorMode, ResultsMode};
use crate::cmd::run::provisioner::CanReuse;
use crate::cmd::{graphql, util};
use crate::format::{IndentFormatter, Progress, ansi};
use crate::runner::Schedular;
use crate::tests::case::Dialect;
use crate::tests::report::{TestGrade, TestReport, TestTaskResult};
use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::schema::{ENV_DEFAULT_TIMEOUT, NewPlannerStrategyConfig};
use crate::tests::{CaseSet, RunSetBuilder, TestRun};

mod provisioner;

#[derive(Debug)]
pub struct TestRunConfig {
	pub planner_config: NewPlannerStrategyConfig,
	pub backend: Backend,
}

impl RunConfig for TestRunConfig {
	fn name(&self, case: &CaseImports) -> String {
		format!("{} on {} [{}]", case.test.origin.path, self.backend, self.planner_config)
	}
}

pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let mut load_errors = Vec::new();

	let path: &String = matches.get_one("path").unwrap();
	let set = CaseSet::load_surrealql_files(path, &mut load_errors).await?;

	let backend = *matches.get_one::<Backend>("backend").unwrap();
	let core_version = Version::parse(VERSION).unwrap();

	// Check if the backend is supported by the enabled features.
	match backend {
		// backend memory is always enabled as we needs it to run match expressions.
		Backend::Memory => {}
		#[cfg(feature = "backend-rocksdb")]
		Backend::RocksDb => {}
		#[cfg(not(feature = "backend-rocksdb"))]
		Backend::RocksDb => bail!("RocksDb backend feature is not enabled"),
		#[cfg(feature = "backend-surrealkv")]
		Backend::SurrealKv => {}
		#[cfg(not(feature = "backend-surrealkv"))]
		Backend::SurrealKv => bail!("SurrealKV backend feature is not enabled"),
		#[cfg(feature = "backend-tikv")]
		Backend::TikV => {}
		#[cfg(not(feature = "backend-tikv"))]
		Backend::TikV => bail!("TiKV backend feature is not enabled"),
	}

	let set_builder = RunSetBuilder::new(&set, &mut load_errors)
		// Only run test for which run is enabled.
		.with_filter(|x| x.test.config.parsed.test.run)
		// Only run test for this backend.
		.with_filter(|x| {
			let config_backend = &x.test.config.parsed.env.backend;
			config_backend.is_empty() || config_backend.contains(&backend)
		})
		// The memory backend no longer supports versioned (MVCC time-travel)
		// queries after the surrealmx 0.23 upgrade; versioned coverage runs on
		// the rocksdb and surrealkv backends, which retain native versioning.
		.with_filter(|x| !x.test.config.parsed.env.versioned || !matches!(backend, Backend::Memory))
		// Run for all config the test has configured.
		.with_expander(|x| {
			x.test
				.config
				.parsed
				.env
				.planner_strategy
				.iter()
				.map(|x| TestRunConfig {
					planner_config: *x,
					backend,
				})
				.collect()
		});

	let set_builder = if let Some(name_filter) = matches.get_one::<String>("filter") {
		set_builder.with_filter(move |x| x.test.origin.path.contains(name_filter))
	} else {
		set_builder
	};

	let set_builder = if matches.get_flag("no-wip") {
		set_builder.with_filter(|x| !x.test.config.parsed.test.wip)
	} else {
		set_builder
	};

	let set_builder = if matches.get_flag("no-results") {
		set_builder.with_filter(|x| x.test.config.parsed.test.results.is_none())
	} else {
		set_builder
	};

	// Filter out test which cannot run on the current version.
	let set_builder = set_builder.with_filter(|x| {
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
	});

	let runs = set_builder.build();

	let num_jobs = matches
		.get_one::<u32>("jobs")
		.copied()
		.unwrap_or_else(|| thread::available_parallelism().map(|x| x.get() as u32).unwrap_or(8));

	let failure_mode = matches.get_one::<ResultsMode>("results").unwrap();

	println!(" Running with {num_jobs} jobs");
	let mut schedular = Schedular::new(num_jobs);

	// all reports are collected into the channel before processing.
	// So unbounded is required.
	let (report_send, mut report_recv) = mpsc::unbounded_channel();

	let mut provisioner = Provisioner::new(num_jobs as usize, backend).await?;

	println!("Found {} tests ", runs.len());

	let mut reports = Vec::new();
	let mut progress = Progress::from_stderr(runs.len(), color);

	// spawn all tests -- one task per (test, strategy) combination.
	for run in runs {
		progress.start_item(run.id, &run.name()).unwrap();
		schedule_run(run, &mut schedular, &mut provisioner, report_send.clone()).await;

		// Handle possible done reports.
		while let Ok(report) = report_recv.try_recv() {
			let grade = report.grade();
			progress.finish_item(report.id, grade).unwrap();
			reports.push(report);
		}
	}

	// when the report channel quits we can be sure we are done. since the report task has quit
	// meaning the test tasks have all quit.
	std::mem::drop(report_send);
	while let Some(report) = report_recv.recv().await {
		let grade = report.grade();
		progress.finish_item(report.id, grade).unwrap();
		reports.push(report);
	}

	// Wait for all the tasks to finish.
	schedular.join_all().await;

	println!();

	// Shutdown all the stores.
	if let Err(e) = provisioner.shutdown().await {
		println!("Shutdown error: {e:?}");
		println!();
		println!();
	}

	// done, report the results.
	for v in reports.iter() {
		v.display(color)
	}

	for e in load_errors.iter() {
		e.display(color);
	}

	let use_color = match color {
		ColorMode::Always => true,
		ColorMode::Never => false,
		ColorMode::Auto => std::io::stdout().is_terminal(),
	};

	let mut buffer = String::new();
	let mut f = IndentFormatter::new(&mut buffer, 2);
	f.indent(|f| {
		for c in set.iter() {
			let mut first = true;
			for k in c.config.parsed.unused_keys() {
				if first {
					first = false;
					if use_color {
						writeln!(
							f,
							ansi!(
								" ==> ",
								yellow,
								"Warning",
								reset_format,
								" for ",
								bold,
								"{}",
								reset_format,
								":"
							),
							c.origin.path
						)?;
					} else {
						writeln!(f, " ==> Warning for {}", c.origin.path)?;
					}
				}
				f.indent(|f| writeln!(f, "> Test config contains unused key: {}", k))?;
			}
		}
		Ok(())
	})
	.unwrap();

	// Print summary line.
	// passed/failed/warned are per-run counts (one report per test-strategy pair),
	// while skipped is a per-test count (tests excluded before strategy expansion).
	let passed = reports.iter().filter(|r| r.grade() == TestGrade::Success).count();
	let failed = reports.iter().filter(|r| r.grade() == TestGrade::Failed).count();
	let warned = reports.iter().filter(|r| r.grade() == TestGrade::Warning).count();
	if use_color {
		print!(ansi!(green, " {} runs passed", reset_format), passed);
		if failed > 0 {
			print!(ansi!(", ", red, "{} failed", reset_format), failed);
		}
		if warned > 0 {
			print!(ansi!(", ", yellow, "{} warnings", reset_format), warned);
		}
		println!();
	} else {
		print!(" {passed} runs passed");
		if failed > 0 {
			print!(", {failed} failed");
		}
		if warned > 0 {
			print!(", {warned} warnings");
		}
		println!();
	}
	println!();

	// possibly update test configs with acquired results.
	match failure_mode {
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

	if reports.iter().any(|x| x.grade() == TestGrade::Failed) {
		bail!("Not all tests were successful")
	}

	if !load_errors.is_empty() {
		bail!("Could not load all tests")
	}

	Ok(())
}

pub async fn schedule_run(
	run: TestRun<TestRunConfig>,
	schedular: &mut Schedular,
	provisioner: &mut Provisioner,
	report_sender: UnboundedSender<TestReport>,
) {
	let permit = provisioner.obtain(&run.case.test.config.parsed.env).await;
	let sequential = run.case.test.config.parsed.env.sequential;

	let future = async move {
		let res = permit
			.with(async |ds, grade_ds| {
				let fut = run_test_with_dbs(&run, ds);
				let res = AssertUnwindSafe(fut).catch_unwind().await;

				match res {
					Ok(Ok(x)) => (
						CanReuse::Reusable,
						Ok(TestReport::from_test_result(run, x, grade_ds).await),
					),
					Ok(Err(e)) => (CanReuse::Reusable, Err(e)),
					Err(e) => (CanReuse::Reset, Ok(TestReport::from_panic(run, e))),
				}
			})
			.await;

		let res = match res {
			Ok(Ok(x)) => x,
			Ok(Err(e)) | Err(e) => {
				eprintln!("Task returned an error!: {e}");
				return;
			}
		};

		report_sender.send(res).expect("Channel closed too early");
	};

	if sequential {
		schedular.spawn_sequential(future).await
	} else {
		schedular.spawn(future).await
	}
}

/// Checks for keys retained in the datastore after clean up which should not be there.
async fn check_retained_keys(dbs: &Datastore) -> Result<Vec<Vec<u8>>> {
	// `/!tl` are task-lease keys (e.g. written when the tombstone-reclaim task
	// acquires its lease during cleanup); like `/!ic` they are background-task
	// infrastructure that legitimately persists, not leaked test data.
	const ALLOWED_KEY_PREFIXES: &[&[u8]] = &[b"/!ni", b"/!nh", b"/!nd", b"/!ic", b"/!tl"];

	let txn = dbs.transaction(surrealdb_core::kvs::TransactionType::Read).await?;
	// The point of this check is to find keys the declared keyspace does *not*
	// describe, which is the one scan no key bound can express.
	let range = surrealdb_core::key::RawRange::every_key();
	let res = txn.keys_raw(range, 1000, 0, None).await?;
	txn.cancel().await?;
	Ok(res
		.into_iter()
		.filter(|key| !ALLOWED_KEY_PREFIXES.iter().any(|allowed| key.starts_with(allowed)))
		.collect())
}

/// The outcome of running a test body via [`run_test_body`], before the
/// datastore is cleaned up.
enum BodyOutcome {
	/// The body exited before the query executed (failed import / signup /
	/// signin / parse / lower). This is the final result for the test — the query
	/// never ran, so no retained-key check is performed.
	Early(TestTaskResult),
	/// The query executed. The result still needs the post-run retained-key check
	/// before it is finalised into a [`TestTaskResult`].
	Executed {
		did_timeout: bool,
		result: Result<Vec<Result<SurValue, String>>, anyhow::Error>,
	},
}

async fn run_test_with_dbs(
	run: &TestRun<TestRunConfig>,
	dbs: &Arc<Datastore>,
) -> Result<TestTaskResult> {
	let config = &run.case.test.config.parsed;

	let mut session = util::session_from_test_config(config, run.config.planner_config.into());

	// Run the test body, capturing any early-exit result. The cleanup below must
	// run on *every* exit path — otherwise seed data from an importing test that
	// fails before the query runs (e.g. a rejection test that also declares
	// `[env] imports`, or a failed signin) is left behind on the reused base
	// datastore and breaks every later test that imports the same schema.
	let outcome = run_test_body(run, dbs, &mut session).await;

	// Always clean up, then surface a body error (if any) ahead of a cleanup error.
	let cleanup = cleanup_environment(dbs, &session).await;
	let outcome = outcome?;
	cleanup?;

	let (did_timeout, result) = match outcome {
		BodyOutcome::Early(result) => return Ok(result),
		BodyOutcome::Executed {
			did_timeout,
			result,
		} => (did_timeout, result),
	};

	// If the test was not a clean test it should ensure that the datastore is reset for the next
	// test.
	if !config.env.clean {
		let keys = check_retained_keys(dbs).await?;
		if !keys.is_empty() {
			return Ok(TestTaskResult::BadCleanup(keys));
		}
	}

	match result {
		Ok(res) => Ok(TestTaskResult::Results {
			did_timeout,
			res,
		}),
		Err(e) => Ok(TestTaskResult::RunningError(e)),
	}
}

/// Runs a single test's body — imports, optional signup/signin, then the query
/// under test — without touching datastore cleanup.
///
/// Any failure that happens before the query executes is returned as
/// [`BodyOutcome::Early`]; that result is the test's outcome. Once the query has
/// run the outcome is [`BodyOutcome::Executed`]. Cleanup is intentionally left to
/// the caller ([`run_test_with_dbs`]) so it can run on every exit path, including
/// these early returns.
async fn run_test_body(
	run: &TestRun<TestRunConfig>,
	dbs: &Arc<Datastore>,
	session: &mut Session,
) -> Result<BodyOutcome> {
	let config = &run.case.test.config.parsed;

	if let Some(x) = util::run_imports(run, session.clone(), dbs).await? {
		return Ok(BodyOutcome::Early(TestTaskResult::Import(x.path, x.message)));
	}

	let timeout_duration =
		config.env.timeout.map(|x| x.0).into_value(ENV_DEFAULT_TIMEOUT).unwrap_or(Duration::MAX);

	if let Some(signup_vars) = config.env.signup.as_ref()
		&& let Err(e) =
			surrealdb_core::iam::signup::signup(dbs, session, signup_vars.0.clone().into()).await
	{
		return Ok(BodyOutcome::Early(TestTaskResult::SignupError(e)));
	}

	if let Some(signin_vars) = config.env.signin.as_ref()
		&& let Err(e) =
			surrealdb_core::iam::signin::signin(dbs, session, signin_vars.0.clone().into()).await
	{
		return Ok(BodyOutcome::Early(TestTaskResult::SigninError(e)));
	}

	let (did_timeout, result) = match run.case.test.dialect {
		Dialect::SurrealQl => {
			let settings = syn::parser::ParserSettings {
				files_enabled: dbs
					.get_capabilities()
					.allows_experimental(&ExperimentalTarget::Files),
				surrealism_enabled: dbs
					.get_capabilities()
					.allows_experimental(&ExperimentalTarget::Surrealism),
				..Default::default()
			};

			let source = &run.case.test.source.as_bytes();
			let mut parser = syn::parser::Parser::new_with_settings(source, settings);
			let mut stack = reblessive::Stack::new();

			let query = match stack.enter(|stk| parser.parse_query(stk)).finish() {
				Ok(x) => {
					if let Err(e) = parser.assert_finished() {
						return Ok(BodyOutcome::Early(TestTaskResult::ParserError(
							e.render_on_bytes(source),
						)));
					}
					x
				}
				Err(e) => {
					return Ok(BodyOutcome::Early(TestTaskResult::ParserError(
						e.render_on_bytes(source),
					)));
				}
			};

			let start = Instant::now();
			let result = dbs.process(query, &*session, None).await;
			let did_timeout = start.elapsed() > timeout_duration;
			let result = result
				.map(|x| x.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect())
				.map_err(|e| anyhow::anyhow!(e));
			(did_timeout, result)
		}
		Dialect::Gql => {
			// GQL has no capability-gated syntax; the default recursion
			// limit matches `syn::parser::ParserSettings::default()` above.
			// Lowering produces a `PreparedGqlQuery` (a `MatchPlan` embedded
			// in a logical plan); it executes through the streaming engine via
			// `process_gql`, so `.gql` cases default away from the
			// compute-only strategy (see `Dialect::Gql` default in the
			// schema's planner-strategy seam).
			let settings = gql::GqlParserSettings::default();
			let source = &run.case.test.source.as_bytes();
			let query = match gql::parse_to_plan_with_settings(&run.case.test.source, settings)
			{
				Ok(x) => x,
				Err(e) => {
					return Ok(BodyOutcome::Early(TestTaskResult::ParserError(
						e.render_on_bytes(source),
					)));
				}
			};

			let start = Instant::now();
			let result = dbs.process_gql(query, &*session, None).await;
			let did_timeout = start.elapsed() > timeout_duration;
			let result = result
				.map(|x| x.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect())
				.map_err(|e| anyhow::anyhow!(e));
			(did_timeout, result)
		}
		Dialect::GraphQl => {
			// Schema generation reads the catalog like the server does on a
			// cache miss; it is setup, not the query under test, so it stays
			// outside the timed window. Failures (e.g. GraphQL not configured)
			// are part of the testable surface and become the single result.
			match graphql::generate_schema(dbs, &*session).await {
				Ok(schema) => {
					let request = graphql::build_request(&run.case.test, dbs, &*session)?;
					let start = Instant::now();
					let response = schema.execute(request).await;
					let did_timeout = start.elapsed() > timeout_duration;
					(did_timeout, Ok(vec![graphql::response_to_result(response)]))
				}
				Err(e) => (false, Ok(vec![Err(e)])),
			}
		}
	};

	Ok(BodyOutcome::Executed {
		did_timeout,
		result,
	})
}

/// Removes the namespace, database, and root configs a test (or its imports) may
/// have created, returning the reused base datastore to a clean state for the
/// next test.
///
/// Every statement is `IF EXISTS`, so this is safe to call on any exit path —
/// including early failures where the query never ran. It must run regardless of
/// how the body exited: skipping it (as the old early-return paths did) leaks
/// seed data from importing tests that fail before execution onto the shared
/// datastore, breaking later tests.
async fn cleanup_environment(dbs: &Arc<Datastore>, session: &Session) -> Result<()> {
	if let Some(ref ns) = session.ns {
		if let Some(ref db) = session.db {
			let session = Session::owner().with_ns(ns);
			dbs.execute(&format!("REMOVE DATABASE IF EXISTS `{db}`;"), &session, None)
				.await
				.context("failed to remove test database")?;
		}

		let session = Session::owner();
		dbs.execute(&format!("REMOVE NAMESPACE IF EXISTS `{ns}`;"), &session, None)
			.await
			.context("failed to remove used test namespace")?;
	}

	// Clean up configs that may have been created during the test.
	let session = Session::owner();
	dbs.execute(
		"REMOVE CONFIG IF EXISTS GRAPHQL; REMOVE CONFIG IF EXISTS API; REMOVE CONFIG IF EXISTS DEFAULT;",
		&session,
		None,
	)
	.await
	.context("failed to remove root config")?;

	// Remove any root-level users and accesses the test (or its imports) defined.
	// Unlike namespaces and configs, these carry test-chosen names, so enumerate
	// them via `INFO FOR ROOT` and remove each. This matters on shared physical
	// backends (e.g. TiKV) where every datastore aliases one cluster: a `clean`
	// test that runs `DEFINE USER ... ON ROOT` would otherwise leave the key
	// behind for the next test, which the retained-key check then flags.
	let mut info = dbs
		.execute("INFO FOR ROOT;", &session, None)
		.await
		.context("failed to read root info during cleanup")?;
	if let Some(SurValue::Object(info)) = info.pop().and_then(|r| r.result.ok()) {
		for (field, kind) in [("users", "USER"), ("accesses", "ACCESS")] {
			if let Some(SurValue::Object(entries)) = info.get(field) {
				for name in entries.keys() {
					dbs.execute(
						&format!("REMOVE {kind} IF EXISTS `{name}` ON ROOT;"),
						&session,
						None,
					)
					.await
					.with_context(|| format!("failed to remove root {field} during cleanup"))?;
				}
			}
		}
	}

	// `REMOVE NAMESPACE`/`REMOVE DATABASE`/`REMOVE INDEX` defer their data
	// deletion to a background reclaim task, which is not otherwise running inside the
	// test harness. Drain it last (after every REMOVE above) so deferred removals
	// actually reclaim their data and queue entries, leaving the reused datastore
	// clean for the retained-key check and the next test.
	// Grace `ZERO`: drain immediately. The harness is single-threaded per
	// datastore with no concurrent readers, so the snapshot-safety grace that
	// production uses is unnecessary here and would leave residue for the
	// retained-key check.
	Datastore::reclaim_tombstones(
		Arc::clone(dbs),
		std::time::Duration::from_secs(1),
		std::time::Duration::ZERO,
		tokio_util::sync::CancellationToken::new(),
	)
	.await
	.context("failed to drain tombstone reclaim queue")?;

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::SystemTime;

	use surrealdb_core::dbs::Capabilities;
	use surrealdb_core::dbs::capabilities::Targets;
	use surrealdb_core::kvs::Datastore;

	use super::*;
	use crate::tests::case::{CaseId, Dialect, Origin, TestCase};
	use crate::tests::run::{CaseImports, TestRunId};
	use crate::tests::schema::NewPlannerStrategyConfig;

	/// Builds a datastore equivalent to the reused base-environment datastore the
	/// provisioner hands out (all capabilities + experimental targets, auth on).
	async fn base_datastore() -> Arc<Datastore> {
		let ds = Datastore::builder()
		.without_maintenance_tasks()
			.with_capabilities(Capabilities::all().with_experimental(Targets::All))
			.with_auth(true)
			.build_with_path("memory")
			.await
			.unwrap();
		ds.bootstrap().await.unwrap();
		ds
	}

	fn case_from_source(id: usize, path: &str, source: &str) -> Arc<TestCase> {
		let origin = Arc::new(Origin {
			path: path.to_string(),
			modified: SystemTime::UNIX_EPOCH,
			subset: None,
			line_offset: None,
		});
		Arc::new(
			TestCase::from_source_origin_id(
				CaseId::new(id),
				origin,
				source.to_string(),
				Dialect::SurrealQl,
			)
			.unwrap(),
		)
	}

	fn make_run(test: Arc<TestCase>, imports: Vec<Arc<TestCase>>) -> TestRun<TestRunConfig> {
		TestRun {
			id: TestRunId::new(0),
			case: Arc::new(CaseImports {
				test,
				imports,
			}),
			config: TestRunConfig {
				planner_config: NewPlannerStrategyConfig::ComputeOnly,
				backend: Backend::Memory,
			},
		}
	}

	/// Regression test for the harness control-flow bug where an early return from
	/// `run_test_with_dbs` (parse/lower/signin failure) skipped datastore cleanup.
	///
	/// An importing test that fails to parse used to leave its seed data on the
	/// reused base datastore; the next test importing the same schema then failed
	/// with "Database record person:1 already exists". With cleanup hoisted onto
	/// every exit path, the first test cleans up after itself and the second runs
	/// cleanly — both pass even when sharing a single datastore (i.e. `--jobs 1`).
	#[tokio::test]
	async fn importing_test_failing_to_parse_cleans_up_for_next_test() {
		let dbs = base_datastore().await;

		// Seed data both tests pull in via `[env] imports`.
		let import = case_from_source(0, "import_seed", "CREATE person:1 SET name = 'a';");

		// Test A: a base-environment test that imports the seed data and then fails
		// to parse. This is the historical leak: imports ran, the early return
		// skipped cleanup, and `person:1` was left behind.
		let case_a = case_from_source(
			1,
			"reject_with_import",
			"/**\n[env]\nnamespace = true\ndatabase = true\nauth = { level = \"owner\" }\n*/\nCREATE person:2 SET name = ;",
		);
		let run_a = make_run(case_a, vec![import.clone()]);
		let result_a = run_test_with_dbs(&run_a, &dbs).await.unwrap();
		assert!(
			matches!(result_a, TestTaskResult::ParserError(_)),
			"test A should fail to parse, got {result_a:?}",
		);

		// The fix: cleanup ran on the early-return path, so nothing is left behind
		// on the reused datastore.
		let retained = check_retained_keys(&dbs).await.unwrap();
		assert!(
			retained.is_empty(),
			"importing test that failed to parse leaked keys onto the reused datastore: {retained:?}",
		);

		// Test B: imports the same schema on the same datastore. Before the fix this
		// failed because `person:1` already existed.
		let case_b = case_from_source(
			2,
			"accept_with_import",
			"/**\n[env]\nnamespace = true\ndatabase = true\nauth = { level = \"owner\" }\n*/\nSELECT name FROM person;",
		);
		let run_b = make_run(case_b, vec![import]);
		let result_b = run_test_with_dbs(&run_b, &dbs).await.unwrap();
		assert!(
			!matches!(result_b, TestTaskResult::Import(..)),
			"test B's import should succeed after A cleaned up, got {result_b:?}",
		);
		assert!(
			matches!(result_b, TestTaskResult::Results { .. }),
			"test B should produce query results, got {result_b:?}",
		);
	}
}
