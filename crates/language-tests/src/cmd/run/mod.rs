use crate::{
	cli::{Backend, ColorMode, ResultsMode},
	format::Progress,
	runner::Schedular,
	tests::{
		TestSet,
		report::{TestGrade, TestReport, TestTaskResult},
		set::TestId,
	},
};

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use provisioner::{Permit, PermitError, Provisioner};
use semver::Version;
use std::{io, mem, str, thread, time::Duration};
use surrealdb_core::{
	dbs::{capabilities::ExperimentalTarget, Session},
	env::VERSION,
	kvs::{Datastore, LockType, TransactionType},
	syn,
};
use tokio::{
	select,
	sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
	time,
};

mod provisioner;
mod util;

use util::core_capabilities_from_test_config;

pub struct TestTaskContext {
	pub id: TestId,
	pub testset: TestSet,
	pub ds: Permit,
	pub result: Sender<(TestId, TestTaskResult)>,
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
		#[cfg(any(feature = "backend-foundation-7_1", feature = "backend-foundation-7_1"))]
		Backend::Foundation => {}
		#[cfg(not(any(feature = "backend-foundation-7_1", feature = "backend-foundation-7_1")))]
		Backend::Foundation => bail!("FoundationDB backend features is not enabled"),
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
			if let Some(version_req) = config.test.as_ref().map(|x| &x.version) {
				if !version_req.matches(&core_version) {
					return None;
				}
			}

			// Ensure this test imports can run on this version as specified by the test itself.
			if let Some(version_req) = config.test.as_ref().map(|x| &x.importing_version) {
				if !version_req.matches(&core_version) {
					return None;
				}
			}

			// Ensure this test imports can run on this version as specified by the imports.
			for import in test.imports.iter() {
				if let Some(version_req) =
					subset[import.id].config.test.as_ref().map(|x| &x.version)
				{
					if !version_req.matches(&core_version) {
						return None;
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

	let txn = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	txn.ensure_ns_db("match", "match", false).await.unwrap();
	txn.commit().await.unwrap();

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

	let timeout_duration = config
		.env
		.as_ref()
		.map(|x| x.timeout().map(Duration::from_millis).unwrap_or(Duration::MAX))
		.unwrap_or(Duration::from_secs(1));

	let strict = config.env.as_ref().map(|x| x.strict).unwrap_or(false);

	let res = context
		.ds
		.with(
			move |ds| {
				ds.with_capabilities(capabilities)
					.with_query_timeout(Some(timeout_duration))
					.with_strict_mode(strict)
			},
			async |ds| run_test_with_dbs(context.id, &context.testset, ds).await,
		)
		.await;

	let res = match res {
		Ok(x) => x?,
		Err(PermitError::Other(e)) => return Err(e),
		Err(PermitError::Panic(e)) => TestTaskResult::Paniced(e),
	};

	context.result.send((context.id, res)).await.expect("result channel quit early");

	Ok(())
}

async fn run_test_with_dbs(
	id: TestId,
	set: &TestSet,
	dbs: &mut Datastore,
) -> Result<TestTaskResult> {
	let config = &set[id].config;

	let mut session = util::session_from_test_config(config);

	let timeout_duration = config
		.env
		.as_ref()
		.map(|x| x.timeout().map(Duration::from_millis).unwrap_or(Duration::MAX))
		.unwrap_or(Duration::from_secs(2));

	let mut import_session = Session::owner();
	if let Some(ns) = session.ns.as_ref() {
		import_session = import_session.with_ns(ns);
		let txn = dbs.transaction(TransactionType::Write, LockType::Optimistic).await?;
		txn.get_or_add_ns(ns, false).await?;
		txn.commit().await?;

		if let Some(db) = session.db.as_ref() {
			import_session = import_session.with_db(db);
			let txn = dbs.transaction(TransactionType::Write, LockType::Optimistic).await?;
			txn.get_or_add_db(ns, db, false).await?;
			txn.commit().await?;
		};

	};

	for import in set[id].config.imports() {
		let Some(test) = set.find_all(import) else {
			return Ok(TestTaskResult::Import(
				import.to_string(),
				"Could not find import.".to_string(),
			));
		};

		let Ok(source) = str::from_utf8(&set[test].source) else {
			return Ok(TestTaskResult::Import(
				import.to_string(),
				"Import file was not valid utf-8.".to_string(),
			));
		};

		if let Err(e) = dbs.execute(source, &import_session, None).await {
			return Ok(TestTaskResult::Import(
				import.to_string(),
				format!("Failed to run import: `{e}`"),
			));
		}
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
		references_enabled: dbs
			.get_capabilities()
			.allows_experimental(&ExperimentalTarget::RecordReferences),
		bearer_access_enabled: dbs
			.get_capabilities()
			.allows_experimental(&ExperimentalTarget::BearerAccess),
		define_api_enabled: dbs
			.get_capabilities()
			.allows_experimental(&ExperimentalTarget::DefineApi),
		files_enabled: dbs.get_capabilities().allows_experimental(&ExperimentalTarget::Files),
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
