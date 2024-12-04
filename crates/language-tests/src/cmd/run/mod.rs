mod cmp;
mod progress;
mod report;
mod util;

use core::str;
use std::{io, mem, pin::pin, thread, time::Duration};

use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use clap::ArgMatches;
use progress::Progress;
use report::{TestGrade, TestReport};
use surrealdb_core::{
	dbs::{Capabilities, Response, Session},
	err::Error as CoreError,
	kvs::Datastore,
	syn,
};
use syn::error::RenderedError;
use tokio::{
	select,
	sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
	time::{self},
};
use util::core_capabilities_from_test_config;

use crate::{
	cli::{ColorMode, FailureMode},
	runner::Schedular,
	tests::{testset::TestId, TestSet},
};

#[derive(Debug)]
pub enum TestTaskResult {
	ParserError(RenderedError),
	RunningError(CoreError),
	Timeout,
	Results(Vec<Response>),
}

pub struct TestTaskContext {
	pub id: TestId,
	pub testset: TestSet,
	pub ds: Option<(Datastore, Sender<Datastore>)>,
	pub result: Sender<(TestId, TestTaskResult)>,
}

async fn fill_datastores(sender: &Sender<Datastore>, num_jobs: usize) -> Result<()> {
	for _ in 0..num_jobs {
		let db = Datastore::new("memory")
			.await?
			.with_capabilities(Capabilities::all())
			.with_notifications();

		db.bootstrap().await?;

		sender.send(db).await.unwrap();
	}
	Ok(())
}

fn try_collect_reports<W: io::Write>(
	reports: &mut Vec<TestReport>,
	testset: &TestSet,
	channel: &mut UnboundedReceiver<TestReport>,
	progress: &mut Progress<W>,
) {
	while let Some(x) = channel.try_recv().ok() {
		let grade = x.grade();
		let name = testset[x.test_id()].path.as_str();
		progress.finish_item(name, grade).unwrap();
		reports.push(x);
	}
}

pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let path: &String = matches.get_one("path").unwrap();
	let testset = TestSet::collect_directory(Utf8Path::new(&path)).await?;

	let subset = if let Some(x) = matches.get_one::<String>("filter") {
		testset.filter(x)
	} else {
		testset
	};

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

	let failure_mode = matches.get_one::<FailureMode>("failure").unwrap();

	println!(" Running with {num_jobs} jobs");
	let mut schedular = Schedular::new(num_jobs);

	let (ds_send, mut ds_recv) = mpsc::channel(num_jobs as usize);
	// give the result channel some slack to catch up to tasks.
	let (res_send, res_recv) = mpsc::channel(num_jobs as usize * 4);
	// all reports are collected into the channel before processing.
	// So unbounded is required.
	let (report_send, mut report_recv) = mpsc::unbounded_channel();

	fill_datastores(&ds_send, num_jobs as usize)
		.await
		.context("Failed to create datastores for running tests")?;

	println!(" Found {} tests", subset.len());

	tokio::spawn(grade_task(subset.clone(), res_recv, report_send));

	let mut reports = Vec::new();
	let mut progress = progress::Progress::from_stderr(subset.len(), color);

	// spawn all tests.
	for (id, test) in subset.iter_ids() {
		let config = test.config.clone();

		progress.start_item(test.path.as_str()).unwrap();

		if !config.should_run() {
			progress.finish_item(subset[id].path.as_str(), TestGrade::Success).unwrap();
			continue;
		}

		let ds = if config.can_use_reusable_ds() {
			let ds = ds_recv.recv().await.context("datastore return channel closed early")?;
			Some((ds, ds_send.clone()))
		} else {
			None
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
		try_collect_reports(&mut reports, &subset, &mut report_recv, &mut progress);
	}

	// all test are running.
	// drop the result sender so that tasks properly quit when the channel does.
	mem::drop(res_send);
	mem::drop(ds_send);

	// when the report channel quits we can be sure we are done. since the report task has quit
	// meaning the test tasks have all quit.
	while let Some(x) = report_recv.recv().await {
		let grade = x.grade();
		let name = subset[x.test_id()].path.as_str();
		progress.finish_item(name, grade).unwrap();
		reports.push(x);
	}

	while let Some(x) = ds_recv.recv().await {
		x.shutdown().await.context("Datastore failed to shutdown properly")?;
	}

	println!();

	// done, report the results.
	for v in reports.iter() {
		v.display(&subset, color)
	}

	// possibly update test configs with acquired results.
	match failure_mode {
		FailureMode::Fail => {}
		FailureMode::Accept => {
			for report in reports.iter().filter(|x| x.is_unspecified_test() && !x.is_wip()) {
				report.update_config_results(&subset).await?;
			}
		}
		FailureMode::Overwrite => {
			for report in reports.iter().filter(|x| {
				matches!(x.grade(), TestGrade::Failed | TestGrade::Warning) && !x.is_wip()
			}) {
				report.update_config_results(&subset).await?;
			}
		}
	}

	if reports.iter().any(|x| x.grade() == TestGrade::Failed) {
		bail!("Not all tests where successfull")
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

	loop {
		let Some((id, res)) = results.recv().await else {
			break;
		};

		let report = TestReport::from_test_result(id, &set, res, &ds).await;
		sender.send(report).expect("report channel quit early");
	}
}

pub async fn test_task(context: TestTaskContext) -> Result<()> {
	let return_channel;

	let config = &context.testset[context.id].config;
	let capabilities = core_capabilities_from_test_config(config);

	let ds = if let Some((ds, channel)) = context.ds {
		return_channel = Some(channel);
		ds.with_capabilities(capabilities)
	} else {
		return_channel = None;
		let ds =
			Datastore::new("memory").await?.with_capabilities(capabilities).with_notifications();

		ds.bootstrap().await?;

		ds
	};

	let timeout_duration = config
		.env
		.as_ref()
		.map(|x| x.timeout().map(Duration::from_millis).unwrap_or(Duration::MAX))
		.unwrap_or(Duration::from_secs(1));

	let mut ds = ds.with_query_timeout(Some(timeout_duration));

	let res = run_test_with_dbs(context.id, &context.testset, &mut ds).await;

	if let Some(return_channel) = return_channel {
		return_channel.send(ds).await.expect("datastore return channel quit early");
	} else {
		ds.shutdown().await?;
	}

	let res = res?;

	context.result.send((context.id, res)).await.expect("result channel quit early");

	Ok(())
}

async fn run_test_with_dbs(
	id: TestId,
	set: &TestSet,
	dbs: &mut Datastore,
) -> Result<TestTaskResult> {
	let mut session = Session::owner();

	let config = &set[id].config;
	let timeout_duration = config
		.env
		.as_ref()
		.map(|x| x.timeout().map(Duration::from_millis).unwrap_or(Duration::MAX))
		.unwrap_or(Duration::from_secs(1));

	if let Some(ns) = config.namespace() {
		session = session.with_ns(ns)
	}
	if let Some(db) = config.database() {
		session = session.with_db(db)
	}

	for import in set[id].config.imports() {
		let Some(test) = set.find_all(import) else {
			bail!("Could not find import import `{import}`");
		};

		let source = str::from_utf8(&set[test].source)
			.with_context(|| format!("Import `{import}` was not valid utf8"))?;

		if let Err(e) = dbs.execute(source, &session, None).await {
			bail!("Failed to run import `{import}`: {e}");
		}
	}

	let source = &set[id].source;
	let mut parser = syn::parser::Parser::new(source);
	let mut stack = reblessive::Stack::new();

	let query = match stack.enter(|stk| parser.parse_query(stk)).finish() {
		Ok(x) => x,
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
	}

	if let Some(ref ns) = session.ns {
		let session = Session::owner();
		dbs.execute(&format!("REMOVE NAMESPACE IF EXISTS `{ns}`;"), &session, None)
			.await
			.context("failed to remove used test namespace")?;
	}

	match result {
		Ok(x) => Ok(TestTaskResult::Results(x)),
		Err(e) => Ok(TestTaskResult::RunningError(e)),
	}
}
