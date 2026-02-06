use std::collections::HashSet;
use std::time::Duration;
use std::{io, mem, str, thread};

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use provisioner::{Permit, PermitError, Provisioner};
use semver::Version;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::ExperimentalTarget;
use surrealdb_core::env::VERSION;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::syn;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::{select, time};

use crate::cli::{Backend, ColorMode, ResultsMode};
use crate::format::Progress;
use crate::runner::Schedular;
use crate::tests::TestSet;
use crate::tests::report::{TestGrade, TestReport, TestTaskResult};
use crate::tests::set::TestId;

mod provisioner;
mod util;

use util::core_capabilities_from_test_config;

pub struct TestTaskContext {
	pub id: TestId,
	pub testset: TestSet,
	pub ds: Permit,
	pub result: Sender<(TestId, TestTaskResult)>,
	pub backend: Backend,
}

fn try_collect_reports<W: io::Write>(
	reports: &mut Vec<TestReport>,
	channel: &mut UnboundedReceiver<TestReport>,
	progress: &mut Progress<TestId, W>,
) {
	while let Ok(x) = channel.try_recv() {
		let grade = x.grade();
		progress.finish_item(x.test_id(), grade).unwrap();
		reports.push(x);
	}
}

fn filter_testset_from_arguments(testset: TestSet, matches: &ArgMatches) -> TestSet {
	let subset = if let Some(x) = matches.get_one::<String>("filter") {
		testset.filter_map(|name, _| name.contains(x))
	} else {
		testset
	};

	let subset = if matches.get_flag("no-wip") {
		subset.filter_map(|_, set| !set.config.is_wip())
	} else {
		subset
	};

	// Filter tests based on backend specification in their environment configuration.
	// Tests are included if:
	// - They have no env config (run on all backends)
	// - They have an empty backend list (run on all backends)
	// - Their backend list contains the current backend
	let subset = if let Some(backend) = matches.get_one::<Backend>("backend") {
		subset.filter_map(|_, test| {
			if let Some(env) = &test.config.env {
				if !env.backend.is_empty() {
					env.backend.contains(&backend.to_string())
				} else {
					true
				}
			} else {
				true
			}
		})
	} else {
		subset
	};

	if matches.get_flag("no-results") {
		subset.filter_map(|_, set| {
			!set.config.test.as_ref().map(|x| x.results.is_some()).unwrap_or(false)
		})
	} else {
		subset
	}
}

pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let path: &String = matches.get_one("path").unwrap();
	let (testset, load_errors) = TestSet::collect_directory(path).await?;
	let backend = *matches.get_one::<Backend>("backend").unwrap();

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

	let subset = filter_testset_from_arguments(testset, matches);

	// check for unused keys in tests
	for t in subset.iter() {
		for k in t.config.unused_keys() {
			println!("Test `{}` contained unused key `{k}` in config", t.path);
		}
	}

	let num_jobs = matches
		.get_one::<u32>("jobs")
		.copied()
		.unwrap_or_else(|| thread::available_parallelism().map(|x| x.get() as u32).unwrap_or(8));

	let failure_mode = matches.get_one::<ResultsMode>("results").unwrap();

	let core_version = Version::parse(VERSION).unwrap();

	// filter tasks.
	let tasks: Vec<_> = subset
		.iter_ids()
		.filter_map(|(id, test)| {
			if test.contains_error {
				return None;
			}

			let config = test.config.clone();

			// Ensure this test can run on this version.
			if let Some(version_req) = config.test.as_ref().and_then(|x| x.version.as_ref()) {
				if !version_req.matches(&core_version) {
					return None;
				}
			}

			// Ensure this test imports can run on this version as specified by the test itself.
			if let Some(version_req) =
				config.test.as_ref().and_then(|x| x.importing_version.as_ref())
			{
				if !version_req.matches(&core_version) {
					return None;
				}
			}

			let mut set = test.imports.iter().copied().collect::<HashSet<_>>();
			let mut import_stack = test.imports.clone();

			while let Some(import) = import_stack.pop() {
				if let Some(version_req) =
					subset[import].config.test.as_ref().and_then(|x| x.version.as_ref())
				{
					if !version_req.matches(&core_version) {
						return None;
					}
				}

				for import in subset[import].imports.iter().copied() {
					if set.insert(import) {
						import_stack.push(import);
					}
				}
			}

			if !config.should_run() {
				return None;
			}

			Some(id)
		})
		.collect();

	println!(" Running with {num_jobs} jobs");
	let mut schedular = Schedular::new(num_jobs);

	// give the result channel some slack to catch up to tasks.
	let (res_send, res_recv) = mpsc::channel(num_jobs as usize * 4);
	// all reports are collected into the channel before processing.
	// So unbounded is required.
	let (report_send, mut report_recv) = mpsc::unbounded_channel();

	let mut provisioner = Provisioner::new(num_jobs as usize, backend).await?;

	println!(" Found {} tests", subset.len());

	tokio::spawn(grade_task(subset.clone(), res_recv, report_send));

	let mut reports = Vec::new();
	let mut progress = Progress::from_stderr(tasks.len(), color);

	// spawn all tests.
	for id in tasks {
		let config = subset[id].config.as_ref();
		progress.start_item(id, subset[id].path.as_str()).unwrap();

		let ds = if config.can_use_reusable_ds() {
			provisioner.obtain().await
		} else {
			provisioner.create()
		};

		let context = TestTaskContext {
			id,
			testset: subset.clone(),
			result: res_send.clone(),
			ds,
			backend,
		};
		let future = async move {
			let name = context.testset[context.id].path.as_str().to_owned();
			let future = test_task(context);

			if let Err(e) = future.await {
				println!("Error: {:?}", e.context(format!("Failed to run test '{name}'")))
			}
		};

		if config.should_run_sequentially() {
			schedular.spawn_sequential(future).await;
		} else {
			schedular.spawn(future).await;
		}

		// Try to collect reports to give quick feedback on test completion.
		try_collect_reports(&mut reports, &mut report_recv, &mut progress);
	}

	// all test are running.
	// drop the result sender so that tasks properly quit when the channel does.
	mem::drop(res_send);

	// when the report channel quits we can be sure we are done. since the report task has quit
	// meaning the test tasks have all quit.
	while let Some(x) = report_recv.recv().await {
		let grade = x.grade();
		progress.finish_item(x.test_id(), grade).unwrap();
		reports.push(x);
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
		v.display(&subset, color)
	}

	for e in load_errors.iter() {
		e.display(color);
	}

	// possibly update test configs with acquired results.
	match failure_mode {
		ResultsMode::Default => {}
		ResultsMode::Accept => {
			for report in reports.iter().filter(|x| x.is_unspecified_test() && !x.is_wip()) {
				report.update_config_results(&subset).await?;
			}
		}
		ResultsMode::Overwrite => {
			for report in reports.iter().filter(|x| {
				matches!(x.grade(), TestGrade::Failed | TestGrade::Warning) && !x.is_wip()
			}) {
				report.update_config_results(&subset).await?;
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

pub async fn grade_task(
	set: TestSet,
	mut results: Receiver<(TestId, TestTaskResult)>,
	sender: UnboundedSender<TestReport>,
) {
	let ds = Datastore::new("memory")
		.await
		.expect("failed to create datastore for running matching expressions");

	let mut session = surrealdb_core::dbs::Session::default();
	ds.process_use(None, &mut session, Some("match".to_string()), Some("match".to_string()))
		.await
		.unwrap();

	loop {
		let Some((id, res)) = results.recv().await else {
			break;
		};

		let report = TestReport::from_test_result(id, &set, res, &ds, None).await;
		sender.send(report).expect("report channel quit early");
	}
}

pub async fn test_task(context: TestTaskContext) -> Result<()> {
	let config = &context.testset[context.id].config;
	let capabilities = core_capabilities_from_test_config(config);
	let backend_str = context.backend.to_string();

	let context_timeout_duration = config
		.env
		.as_ref()
		.map(|x| {
			x.context_timeout(Some(&backend_str))
				.map(Duration::from_millis)
				.unwrap_or(Duration::MAX)
		})
		.unwrap_or(Duration::from_secs(3));

	let backend = context.backend;
	let res = context
		.ds
		.with(
			move |ds| {
				ds.with_capabilities(capabilities)
					.with_query_timeout(Some(context_timeout_duration))
			},
			async |ds| run_test_with_dbs(context.id, &context.testset, ds, backend).await,
		)
		.await;

	let res = match res {
		Ok(x) => x?,
		Err(PermitError::Other(e)) => return Err(e),
		Err(PermitError::Panic(e)) => TestTaskResult::Panicked(e),
	};

	context.result.send((context.id, res)).await.expect("result channel quit early");

	Ok(())
}

async fn run_test_with_dbs(
	id: TestId,
	set: &TestSet,
	dbs: &mut Datastore,
	backend: Backend,
) -> Result<TestTaskResult> {
	let config = &set[id].config;
	let backend_str = backend.to_string();

	let mut session = util::session_from_test_config(config);

	if let Some(ref x) = session.ns {
		let db = session.db.take();
		dbs.execute(&format!("DEFINE NAMESPACE `{x}`"), &session, None).await?;
		session.db = db;
	}

	if let Some(ref x) = session.db {
		dbs.execute(&format!("DEFINE DATABASE `{x}`"), &session, None).await?;
	}

	let timeout_duration = config
		.env
		.as_ref()
		.map(|x| x.timeout(Some(&backend_str)).map(Duration::from_millis).unwrap_or(Duration::MAX))
		.unwrap_or(Duration::from_secs(2));

	let mut import_stack = Vec::new();
	let mut import_set = HashSet::new();

	for i in set[id].imports.iter() {
		if import_set.insert(*i) {
			import_stack.push(*i);
		}
	}

	// run all imports recursively in post order traversal.
	while let Some(import) = import_stack.last().copied() {
		let mut added_imports = false;
		for i in set[import].imports.iter() {
			if import_set.insert(*i) {
				import_stack.push(*i);
				added_imports = true;
			}
		}

		if added_imports {
			continue;
		}

		let mut import_session = util::session_from_test_config(&set[id].config);
		dbs.process_use(None, &mut import_session, session.ns.clone(), session.db.clone()).await?;

		if let Some(signup_vars) = config.env.as_ref().and_then(|x| x.signup.as_ref()) {
			if let Err(e) = surrealdb_core::iam::signup::signup(
				dbs,
				&mut import_session,
				signup_vars.0.clone().into(),
			)
			.await
			{
				return Ok(TestTaskResult::SignupError(e));
			}
		}

		if let Some(signin_vars) = config.env.as_ref().and_then(|x| x.signin.as_ref()) {
			if let Err(e) = surrealdb_core::iam::signin::signin(
				dbs,
				&mut import_session,
				signin_vars.0.clone().into(),
			)
			.await
			{
				return Ok(TestTaskResult::SigninError(e));
			}
		}

		let Ok(source) = str::from_utf8(&set[import].source) else {
			return Ok(TestTaskResult::Import(
				set[import].path.clone(),
				"Import file was not valid utf-8.".to_string(),
			));
		};

		if let Err(e) = dbs.execute(source, &import_session, None).await {
			return Ok(TestTaskResult::Import(
				set[import].path.clone(),
				format!("Failed to run import: `{e}`"),
			));
		}

		import_stack.pop();
	}

	if let Some(signup_vars) = config.env.as_ref().and_then(|x| x.signup.as_ref()) {
		if let Err(e) =
			surrealdb_core::iam::signup::signup(dbs, &mut session, signup_vars.0.clone().into())
				.await
		{
			return Ok(TestTaskResult::SignupError(e));
		}
	}

	if let Some(signin_vars) = config.env.as_ref().and_then(|x| x.signin.as_ref()) {
		if let Err(e) =
			surrealdb_core::iam::signin::signin(dbs, &mut session, signin_vars.0.clone().into())
				.await
		{
			return Ok(TestTaskResult::SigninError(e));
		}
	}

	let source = &set[id].source;
	let settings = syn::parser::ParserSettings {
		define_api_enabled: dbs
			.get_capabilities()
			.allows_experimental(&ExperimentalTarget::DefineApi),
		files_enabled: dbs.get_capabilities().allows_experimental(&ExperimentalTarget::Files),
		surrealism_enabled: dbs
			.get_capabilities()
			.allows_experimental(&ExperimentalTarget::Surrealism),
		..Default::default()
	};

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

	let mut process_future = Box::pin(dbs.process(query, &session, None));
	let timeout_future = time::sleep(timeout_duration);

	let mut did_timeout = false;
	let result = select! {
		_ = timeout_future => {
			did_timeout = true;


			// Ideally still need to finish the future cause it might panic otherwise.
			select!{
				_ = time::sleep(Duration::from_secs(10)) => {
					// Test doesn't want to quit. Time to force it with a bit of hack to avoid a
					// panic
					std::thread::scope(|scope|{
						scope.spawn(move ||{
							std::mem::drop(process_future)
						});
					});
				}
			   _ = process_future.as_mut() => {}
			}

			None
		}
		x = process_future.as_mut() => {
			Some(x)
		}
	};

	if did_timeout {
		return Ok(TestTaskResult::Timeout);
	};

	let Some(result) = result else {
		unreachable!()
	};

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

	match result {
		Ok(x) => {
			let x = x.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect();
			Ok(TestTaskResult::Results(x))
		}
		Err(e) => Ok(TestTaskResult::RunningError(anyhow::anyhow!(e))),
	}
}
