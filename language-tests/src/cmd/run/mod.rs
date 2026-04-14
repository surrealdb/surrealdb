use std::fmt::Write;
use std::io::IsTerminal;
use std::panic::AssertUnwindSafe;
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
use surrealdb_core::syn;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::cli::{Backend, ColorMode, ResultsMode};
use crate::cmd::run::provisioner::CanReuse;
use crate::format::{IndentFormatter, Progress, ansi};
use crate::runner::Schedular;
use crate::tests::report::{TestGrade, TestReport, TestTaskResult};
use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::schema::{ENV_DEFAULT_TIMEOUT, NewPlannerStrategyConfig};
use crate::tests::{CaseSet, RunSetBuilder, TestRun};

mod provisioner;
mod util;

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
	const ALLOWED_KEY_PREFIXES: &[&[u8]] = &[b"/!ni", b"/!nh", b"/!nd", b"/!ic"];

	let txn = dbs
		.transaction(
			surrealdb_core::kvs::TransactionType::Read,
			surrealdb_core::kvs::LockType::Pessimistic,
		)
		.await?;
	let res = txn.keys(vec![0]..vec![0xff], 1000, 0, None).await?;
	txn.cancel().await?;
	Ok(res
		.into_iter()
		.filter(|key| !ALLOWED_KEY_PREFIXES.iter().any(|allowed| key.starts_with(allowed)))
		.collect())
}

async fn run_test_with_dbs(
	run: &TestRun<TestRunConfig>,
	dbs: &Datastore,
) -> Result<TestTaskResult> {
	let config = &run.case.test.config.parsed;

	let mut session = util::session_from_test_config(config, run.config.planner_config.into());

	if let Some(ref x) = session.ns {
		let db = session.db.take();
		dbs.execute(&format!("DEFINE NAMESPACE `{x}`"), &session, None).await?;
		session.db = db;
	}

	if let Some(ref x) = session.db {
		dbs.execute(&format!("DEFINE DATABASE `{x}`"), &session, None).await?;
	}

	let timeout_duration = config.env.timeout.into_value(ENV_DEFAULT_TIMEOUT).unwrap_or(u64::MAX);
	let timeout_duration = Duration::from_millis(timeout_duration);

	let mut import_session = Session::owner();
	dbs.process_use(None, &mut import_session, session.ns.clone(), session.db.clone()).await?;

	for import in run.case.imports.iter() {
		match dbs.execute(&import.source, &import_session, None).await {
			Err(e) => {
				return Ok(TestTaskResult::Import(
					import.origin.path.clone(),
					format!("Failed to run import: `{e}`"),
				));
			}
			Ok(results) => {
				// Check if any import result contains an error.
				// Without this, errors within transaction blocks (e.g. constraint
				// violations, write conflicts) are silently ignored, causing
				// subsequent test queries to see empty data.
				for result in &results {
					if let Err(ref e) = result.result {
						return Ok(TestTaskResult::Import(
							import.origin.path.clone(),
							format!("Import produced an error: `{e}`"),
						));
					}
				}
			}
		}
	}

	if let Some(signup_vars) = config.env.signup.as_ref()
		&& let Err(e) =
			surrealdb_core::iam::signup::signup(dbs, &mut session, signup_vars.0.clone().into())
				.await
	{
		return Ok(TestTaskResult::SignupError(e));
	}

	if let Some(signin_vars) = config.env.signin.as_ref()
		&& let Err(e) =
			surrealdb_core::iam::signin::signin(dbs, &mut session, signin_vars.0.clone().into())
				.await
	{
		return Ok(TestTaskResult::SigninError(e));
	}

	let settings = syn::parser::ParserSettings {
		files_enabled: dbs.get_capabilities().allows_experimental(&ExperimentalTarget::Files),
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
				return Ok(TestTaskResult::ParserError(e.render_on_bytes(source)));
			}
			x
		}
		Err(e) => return Ok(TestTaskResult::ParserError(e.render_on_bytes(source))),
	};

	let start = Instant::now();
	let result = dbs.process(query, &session, None).await;
	let did_timeout = start.elapsed() > timeout_duration;

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
	{
		let session = Session::owner();
		dbs.execute(
			"REMOVE CONFIG IF EXISTS GRAPHQL; REMOVE CONFIG IF EXISTS API; REMOVE CONFIG IF EXISTS DEFAULT;",
			&session,
			None,
		)
		.await
		.context("failed to remove root config")?;
	}

	// If the test was not a clean test it should ensure that the datastore is reset for the next
	// test.
	if !run.case.test.config.parsed.env.clean {
		let keys = check_retained_keys(dbs).await?;
		if !keys.is_empty() {
			return Ok(TestTaskResult::BadCleanup(keys));
		}
	}

	match result {
		Ok(x) => {
			let x = x.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect();
			Ok(TestTaskResult::Results {
				did_timeout,
				res: x,
			})
		}
		Err(e) => Ok(TestTaskResult::RunningError(anyhow::anyhow!(e))),
	}
}
