mod cmp;
mod progress;
mod report;
mod util;

use core::str;
use std::{pin::pin, thread, time::Duration};

use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use clap::ArgMatches;
use report::{TestGrade, TestReport};
use surrealdb_core::{
	dbs::{capabilities::Targets, Capabilities, Response, Session},
	err::Error as CoreError,
	kvs::Datastore,
	syn,
};
use syn::error::RenderedError;
use tokio::{
	select,
	sync::mpsc::{self, Receiver, Sender},
	time::{self},
};
use util::core_capabilities_from_test_config;

use crate::{
	cli::{ColorMode, FailureMode},
	runner::Schedular,
	tests::{
		schema::{BoolOr, SchemaTarget},
		testset::TestId,
		TestSet,
	},
};

#[derive(Debug)]
pub enum TestJobResult {
	ParserError(RenderedError),
	RunningError(CoreError),
	Timeout,
	Results(Vec<Response>),
}

pub struct TestCoordinator {
	reusable_dbs: Receiver<Datastore>,
	results: Receiver<(TestId, TestJobResult)>,
}

impl TestCoordinator {
	pub async fn shutdown(mut self) -> Result<()> {
		while let Ok(x) = self.reusable_dbs.try_recv() {
			x.shutdown().await?;
		}
		Ok(())
	}
}

#[derive(Clone)]
pub struct TestRunner {
	set: TestSet,
	dbs_sender: Sender<Datastore>,
	result_sender: Sender<(TestId, TestJobResult)>,
}

impl TestRunner {
	pub fn new(num_jobs: usize, set: TestSet) -> (TestCoordinator, TestRunner) {
		let (send, recv) = mpsc::channel(num_jobs);
		let (res_send, res_recv) = mpsc::channel(num_jobs * 2);

		(
			TestCoordinator {
				reusable_dbs: recv,
				results: res_recv,
			},
			TestRunner {
				set,
				dbs_sender: send,
				result_sender: res_send,
			},
		)
	}

	pub async fn fill_datastores(&self, num_jobs: usize) -> Result<()> {
		for _ in 0..num_jobs {
			let db = Datastore::new("memory")
				.await?
				.with_capabilities(Capabilities::all())
				.with_notifications();

			db.bootstrap().await?;

			self.dbs_sender.send(db).await.unwrap();
		}
		Ok(())
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

	let mut schedular = Schedular::new(num_jobs);
	let (mut coordinator, runner) = TestRunner::new(num_jobs as usize, subset);

	println!("Running with {num_jobs} jobs");

	runner.fill_datastores(num_jobs as usize).await?;

	println!("Found {} tests", runner.set.len());

	let mut reports = Vec::new();
	let mut progress = progress::Progress::from_stderr(runner.set.len(), color);

	// spawn all tests.
	for (id, test) in runner.set.iter_ids() {
		let config = test.config.clone();

		progress.start_item(test.path.as_str()).unwrap();

		if !config.should_run() {
			progress.finish_item(runner.set[id].path.as_str(), TestGrade::Success).unwrap();
			continue;
		}

		let db = if config.can_use_reusable_ds() {
			Some(coordinator.reusable_dbs.recv().await.unwrap())
		} else {
			None
		};

		let runner_clone = runner.clone();
		let name_clone = test.path.clone();
		let future = async move {
			let future = run_test(id, runner_clone, db);

			if let Err(e) = future.await {
				println!("Error: {:?}", e.context(format!("Failed to run test '{name_clone}'")))
			}
		};

		if config.should_run_sequentially() {
			schedular.spawn_sequential(future).await;
		} else {
			schedular.spawn(future).await;
		}

		while let Ok((id, result)) = coordinator.results.try_recv() {
			let report = TestReport::new(id, &runner.set, result);
			progress.finish_item(runner.set[id].path.as_str(), report.grade()).unwrap();
			reports.push(report)
		}
	}

	// all test are running.
	// Time to wait for all tasks to finish.
	let mut join = pin!(schedular.join());
	loop {
		select! {
			_ = futures::future::poll_fn(|ctx| {
				join.as_mut().poll(ctx)
			}) => { break }
			x = coordinator.results.recv() => {
				// handle results while tasks are finishing.
				let (id,result) = x.unwrap();

				let report = TestReport::new(id, &runner.set, result);
				progress.finish_item(runner.set[id].path.as_str(), report.grade()).unwrap();
				reports.push(report)

			}
		}
	}

	// All tasks are finished. Process all final results.
	while let Ok((id, result)) = coordinator.results.try_recv() {
		let report = TestReport::new(id, &runner.set, result);
		progress.finish_item(runner.set[id].path.as_str(), report.grade()).unwrap();
		reports.push(report)
	}

	// all results handled, shutdown the remaining datasores.
	coordinator.shutdown().await?;

	println!();

	for v in reports.iter().filter(|x| x.has_warning()) {
		v.display(&runner.set, color)
	}

	println!();

	for v in reports.iter().filter(|x| x.has_failed()) {
		v.display(&runner.set, color)
	}

	// possibly update test configs with acquired results.
	match failure_mode {
		FailureMode::Fail => {}
		FailureMode::Accept => {
			for x in reports.iter().filter(|x| x.has_missing_results()) {
				x.update_config_results(&runner.set).await?;
			}
		}
		FailureMode::Overwrite => {
			for x in reports.iter().filter(|x| !x.succeeded() && !x.has_error() && !x.is_wip()) {
				x.update_config_results(&runner.set).await?;
			}
		}
	}

	if reports.iter().any(|x| x.has_failed()) {
		bail!("Not all tests where successfull")
	}
	Ok(())
}

pub async fn run_test(id: TestId, runner: TestRunner, dbs: Option<Datastore>) -> Result<()> {
	let should_return;

	let config = &runner.set[id].config;
	let capabilities = core_capabilities_from_test_config(config);

	let dbs = if let Some(dbs) = dbs {
		should_return = true;
		dbs.with_capabilities(capabilities)
	} else {
		should_return = false;
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

	let mut dbs = dbs.with_query_timeout(Some(timeout_duration));

	let res = run_test_with_dbs(id, &runner, &mut dbs).await;

	if should_return {
		runner.dbs_sender.send(dbs).await.unwrap();
	} else {
		dbs.shutdown().await?;
	}

	let res = res?;

	runner.result_sender.send((id, res)).await.unwrap();

	Ok(())
}

async fn run_test_with_dbs(
	id: TestId,
	runner: &TestRunner,
	dbs: &mut Datastore,
) -> Result<TestJobResult> {
	let mut session = Session::owner();

	let config = &runner.set[id].config;
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

	for import in runner.set[id].config.imports() {
		let Some(test) = runner.set.find_all(import) else {
			bail!("Could not find import import `{import}`");
		};

		let source = str::from_utf8(&runner.set[test].source)
			.with_context(|| format!("Import `{import}` was not valid utf8"))?;

		if let Err(e) = dbs.execute(source, &session, None).await {
			bail!("Failed to run import `{import}`: {e}");
		}
	}

	let source = &runner.set[id].source;
	let mut parser = syn::parser::Parser::new(source);
	let mut stack = reblessive::Stack::new();

	let query = match stack.enter(|stk| parser.parse_query(stk)).finish() {
		Ok(x) => x,
		Err(e) => return Ok(TestJobResult::ParserError(e.render_on_bytes(source))),
	};

	let mut process_future = pin!(dbs.process(query, &session, None));
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
		return Ok(TestJobResult::Timeout);
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
		Ok(x) => Ok(TestJobResult::Results(x)),
		Err(e) => Ok(TestJobResult::RunningError(e)),
	}
}
