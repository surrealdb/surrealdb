mod cmp;
mod report;

use core::str;
use std::{pin::pin, thread, time::Duration};

use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use clap::ArgMatches;
use report::TestReport;
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
use tracing::{debug, info, info_span, warn, Instrument};

use crate::{
	cli::FailureMode,
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

pub async fn run(matches: &ArgMatches) -> Result<()> {
	let path: &String = matches.get_one("path").unwrap();
	let testset = TestSet::collect_directory(Utf8Path::new(&path)).await?;

	let subset = if let Some(x) = matches.get_one::<String>("filter") {
		testset.filter(x)
	} else {
		testset
	};

	let num_jobs = matches
		.get_one::<u32>("jobs")
		.copied()
		.unwrap_or_else(|| thread::available_parallelism().map(|x| x.get() as u32).unwrap_or(8));

	let failure_mode = matches.get_one::<FailureMode>("failure").unwrap();

	let mut schedular = Schedular::new(num_jobs);
	let (mut coordinator, runner) = TestRunner::new(num_jobs as usize, subset);

	info!("Running with {num_jobs} jobs");

	runner
		.fill_datastores(num_jobs as usize)
		.instrument(info_span!("initialize_reused_stores"))
		.await?;

	info!("Found {} tests", runner.set.len());

	let mut reports = Vec::new();

	let mut handle_pending_results = |id, result| {
		let report = TestReport::new(id, &runner.set, result);

		report.short_display(&runner.set);

		reports.push(report)
	};

	// spawn all tests.
	for (id, test) in runner.set.iter_ids() {
		let config = test.config.clone();

		if !config.should_run() {
			continue;
		}

		info!("running test '{}'", test.path);

		let db = if config.can_use_reusable_ds() {
			Some(coordinator.reusable_dbs.recv().await.unwrap())
		} else {
			None
		};

		let runner_clone = runner.clone();
		let name_clone = test.path.clone();
		let future = async move {
			let future = run_test(id, runner_clone, db)
				.instrument(info_span!("test_run", test_name = name_clone.as_str()));

			if let Err(e) = future.await {
				warn!("{:?}", e.context(format!("Failed to run test '{name_clone}'")))
			}
		};

		if config.should_run_sequentially() {
			schedular.spawn_sequential(future).await;
		} else {
			schedular.spawn(future).await;
		}

		while let Ok((id, result)) = coordinator.results.try_recv() {
			handle_pending_results(id, result);
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
				handle_pending_results(id, result);
			}
		}
	}

	// All tasks are finished. Process all final results.
	while let Ok((id, result)) = coordinator.results.try_recv() {
		handle_pending_results(id, result);
	}

	info!("All tests finished.");

	// all results handled, shutdown the remaining datasores.
	coordinator.shutdown().await?;

	println!();

	for v in reports.iter().filter(|x| x.has_warning()) {
		v.display(&runner.set)
	}

	println!();

	for v in reports.iter().filter(|x| x.has_failed()) {
		v.display(&runner.set)
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

	/// Returns Targets::All if there is no value and deny is false,
	/// Returns Targets::None if there is no value and deny is true ensuring the default behaviour
	/// is to allow everything.
	///
	/// If there is a value it will return Targets::All on the value true, Targets::None on the
	/// value false, and otherwise the returns the specified values.
	fn extract_targets<T>(v: &Option<BoolOr<Vec<SchemaTarget<T>>>>, deny: bool) -> Targets<T>
	where
		T: std::cmp::Eq + std::hash::Hash + Clone,
	{
		v.as_ref()
			.map(|x| match x {
				BoolOr::Bool(true) => Targets::All,
				BoolOr::Bool(false) => Targets::None,
				BoolOr::Value(x) => Targets::Some(x.iter().map(|x| x.0.clone()).collect()),
			})
			.unwrap_or(if deny {
				Targets::None
			} else {
				Targets::All
			})
	}

	let config = &runner.set[id].config;
	let capabilities = config
		.env
		.as_ref()
		.and_then(|x| x.capabilities.as_ref())
		.map(|x| {
			let schema_cap = match x {
				BoolOr::Bool(true) => return Capabilities::all(),
				BoolOr::Bool(false) => return Capabilities::none(),
				BoolOr::Value(x) => x,
			};

			Capabilities::all()
				.with_scripting(schema_cap.scripting.unwrap_or(true))
				.with_guest_access(schema_cap.quest_access.unwrap_or(true))
				.with_live_query_notifications(schema_cap.live_query_notifications.unwrap_or(true))
				.with_functions(extract_targets(&schema_cap.allow_functions, false))
				.without_functions(extract_targets(&schema_cap.deny_functions, true))
				.with_network_targets(extract_targets(&schema_cap.allow_net, false))
				.without_network_targets(extract_targets(&schema_cap.deny_net, true))
				.with_rpc_methods(extract_targets(&schema_cap.allow_rpc, false))
				.without_rpc_methods(extract_targets(&schema_cap.deny_rpc, true))
				.with_http_routes(extract_targets(&schema_cap.allow_http, false))
				.without_http_routes(extract_targets(&schema_cap.deny_http, true))
		})
		.unwrap_or_else(Capabilities::all);

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
		debug!("running import `{import}`");
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
			// still need to finish the future cause it might panic otherwise.
			let _ = process_future.as_mut().await;
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
