mod binaries;
mod process;
mod protocol;

use std::{
	collections::HashMap,
	net::{Ipv4Addr, SocketAddr},
	path::Path,
	sync::Arc,
	thread,
};

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use process::SurrealProcess;
use protocol::{ProxyObject, ProxyValue};
use semver::Version;
use surrealdb_core::kvs::{Datastore, LockType, TransactionType};
use tokio::task::JoinSet;

use crate::{
	cli::{ColorMode, DsVersion, ResultsMode, UpgradeBackend},
	format::Progress,
	temp_dir::TempDir,
	tests::{
		TestSet,
		report::{TestGrade, TestReport, TestTaskResult},
		set::TestId,
	},
};

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TaskId(usize);
impl TaskId {
	pub fn from_usize(v: usize) -> TaskId {
		TaskId(v)
	}
}

pub struct Task {
	id: TaskId,
	from: DsVersion,
	to: DsVersion,
	test: TestId,
	path: String,
	port: u16,
}

impl Task {
	pub fn name(&self, set: &TestSet) -> String {
		format!("{} {} => {}", &set[self.test].path, self.from, self.to)
	}
}

pub struct Config {
	test_path: String,
	jobs: u32,
	download_permission: bool,
	backend: UpgradeBackend,
	keep_files: bool,
}

impl Config {
	pub fn from_matches(matches: &ArgMatches) -> Self {
		Config {
			backend: *matches.get_one::<UpgradeBackend>("backend").unwrap(),
			test_path: matches.get_one::<String>("path").unwrap().clone(),
			download_permission: matches.get_one("allow-download").copied().unwrap_or(false),
			jobs: matches.get_one::<u32>("jobs").copied().unwrap_or_else(|| {
				thread::available_parallelism().map(|x| x.get() as u32).unwrap_or(1)
			}),
			keep_files: matches.get_one::<bool>("keep-files").copied().unwrap_or(false),
		}
	}
}

/// Function which generates the actual test tasks.
pub fn generate_tasks(
	from_versions: &[DsVersion],
	to_versions: &[DsVersion],
	actual_version: &HashMap<DsVersion, Version>,
	subset: &TestSet,
	temp_dir: &TempDir,
) -> Vec<Task> {
	let mut tasks = Vec::new();
	for from in from_versions.iter() {
		for to in to_versions.iter() {
			let from_v = actual_version.get(from).unwrap();
			let to_v = actual_version.get(to).unwrap();

			if from_v >= to_v {
				continue;
			}

			'include_test: for (t, case) in subset.iter_ids() {
				// if the test contains an error don't run it.
				if case.contains_error {
					continue;
				}

				if !case.config.should_run() {
					continue;
				}

				// Ensure that the test can run on the upgrading version.
				if let Some(ver_req) = case.config.test.as_ref().map(|x| &x.version) {
					if !ver_req.matches(to_v) {
						continue 'include_test;
					}
				}

				// Ensure that the test can run on the importing version.
				if let Some(ver_req) = case.config.test.as_ref().map(|x| &x.importing_version) {
					if !ver_req.matches(from_v) {
						continue 'include_test;
					}
				}

				// Ensure that the imports can run on importing version.
				for import in case.imports.iter() {
					if let Some(ver_req) =
						subset[import.id].config.test.as_ref().map(|x| &x.version)
					{
						if !ver_req.matches(from_v) {
							continue 'include_test;
						}
					}
				}

				tasks.push(Task {
					id: TaskId::from_usize(tasks.len()),
					from: from.clone(),
					to: to.clone(),
					test: t,
					// Set later.
					port: 0,
					path: temp_dir
						.sub_dir_path()
						.to_str()
						.expect("Paths should be utf-8")
						.to_owned(),
				})
			}
		}
	}
	tasks
}

pub fn filter_tests(testset: TestSet, matches: &ArgMatches) -> TestSet {
	let subset =
		testset.filter_map(|_, test| test.config.test.as_ref().map(|x| x.upgrade).unwrap_or(false));

	let subset = if let Some(x) = matches.get_one::<String>("filter") {
		subset.filter_map(|name, _| name.contains(x))
	} else {
		subset
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

/// Main subcommand function, runs the actual subcommand.
pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let config = Config::from_matches(matches);
	let config = Arc::new(config);

	let (testset, load_errors) = TestSet::collect_directory(&config.test_path).await?;

	let results_mode = matches.get_one::<ResultsMode>("results").unwrap();

	let from_versions = matches.get_many::<DsVersion>("from").unwrap().cloned().collect::<Vec<_>>();
	let to_versions = matches.get_many::<DsVersion>("to").unwrap().cloned().collect::<Vec<_>>();
	let mut all_versions: Vec<_> =
		from_versions.iter().cloned().chain(to_versions.iter().cloned()).collect();
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
		#[cfg(any(feature = "backend-foundation-7_1", feature = "backend-foundation-7_1"))]
		UpgradeBackend::Foundation => {}
		#[cfg(not(any(feature = "backend-foundation-7_1", feature = "backend-foundation-7_1")))]
		UpgradeBackend::Foundation => bail!("FoundationDB backend features is not enabled"),
	}

	if UpgradeBackend::SurrealKv == config.backend {
		if let Some(DsVersion::Version(v)) =
			all_versions.iter().find(|x| **x < DsVersion::Version(Version::new(2, 0, 0)))
		{
			bail!(
				"Cannot run with backend surrealkv and version {v}, surrealkv was not yet available on this version"
			)
		}
	}

	let subset = filter_tests(testset, matches);

	let mut actual_version = HashMap::new();
	println!("Preparing used versions of surrealdb");
	for v in all_versions {
		let actual = binaries::actual_version(v.clone()).await?;
		binaries::prepare(v.clone(), config.download_permission).await?;
		actual_version.insert(v, actual);
	}

	let temp_dir = TempDir::new("surreal_upgrade_tests")
		.await
		.context("Failed to create temporary directory for datastore")?;

	let tasks = generate_tasks(&from_versions, &to_versions, &actual_version, &subset, &temp_dir);

	println!("Using directory '{}' as a store directory", temp_dir.path().display());
	println!("Running {} tasks for {} tests", tasks.len(), subset.len());

	let mut progress = Progress::from_stderr(tasks.len(), color);
	let mut task_iter = tasks.into_iter();

	let mut reports = Vec::new();

	let ds = Datastore::new("memory")
		.await
		.expect("failed to create datastore for running matching expressions");

	let txn = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	txn.ensure_ns_db("match", "match", false).await.unwrap();
	txn.commit().await.unwrap();

	// Port distribution variables.
	let mut start_port = 9000u16;
	let mut reuse_port = Vec::<u16>::new();

	// The join set to spawn futures into and await.
	let mut join_set = JoinSet::new();

	loop {
		while join_set.len() < config.jobs as usize {
			// Schedule new tasks.
			match task_iter.next() {
				Some(mut task) => {
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

					task.port = port;

					let id = task.id;
					let name = task.name(&subset);
					join_set.spawn(run_task(task, subset.clone(), config.clone()));
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

		let (task, result) = res.unwrap();
		reuse_port.push(task.port);

		let extra_name = format!("{} => {}", task.from, task.to);
		let report =
			TestReport::from_test_result(task.test, &subset, result, &ds, Some(extra_name)).await;
		let grade = report.grade();
		reports.push(report);
		progress.finish_item(task.id, grade).unwrap();
	}

	if config.keep_files {
		temp_dir.keep();
	} else if let Err(e) = temp_dir.cleanup().await {
		println!();
		println!("Failed to clean up temporary directory:{e}");
	}

	println!();

	for v in reports.iter() {
		v.display(&subset, color);
	}

	println!();
	// Report test case loading errors.
	for e in load_errors.iter() {
		e.display(color)
	}

	match *results_mode {
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

	if !load_errors.is_empty() {
		bail!("Could not load all tests")
	}

	if reports.iter().any(|x| x.grade() == TestGrade::Failed) {
		bail!("Not all tests were successfull")
	}

	Ok(())
}

fn is_port_available(port: u16) -> bool {
	std::net::TcpListener::bind(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), port)).is_ok()
}

async fn run_imports(
	task: &Task,
	set: &TestSet,
	process: &mut SurrealProcess,
	namespace: Option<&str>,
	database: Option<&str>,
) -> anyhow::Result<Option<TestTaskResult>> {
	let mut connection = process
		.open_connection()
		.await
		.context("Failed to open connection to the importing database")?;

	process
		.assert_running_while(async {
			let imports = &set[task.test].imports;

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
					.context("Failed to set namespace/database on importing database")?;
				if let Err(e) = resp.result {
					bail!("Failed to set namespace/database on importing database: {}", e.message)
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
				.context("Failed to authenticate on importing database")?;
			if let Err(e) = resp.result {
				bail!("Failed to authenticate on importing database: {}", e.message)
			}

			for import in imports {
				let Ok(source) = std::str::from_utf8(&set[import.id].source) else {
					return Ok(Some(TestTaskResult::Import(
						import.path.clone(),
						"Import file was not valid utf-8.".to_string(),
					)));
				};

				match connection.query(source).await {
					Ok(TestTaskResult::RunningError(e)) => {
						return Ok(Some(TestTaskResult::Import(
							import.path.clone().to_owned(),
							format!("Failed to run import: {e:?}"),
						)));
					}
					Err(e) => {
						return Ok(Some(TestTaskResult::Import(
							import.path.clone().to_owned(),
							format!("Failed to run import: {e:?}."),
						)));
					}
					_ => {}
				}
			}
			Ok(None)
		})
		.await?
}

async fn run_upgrade_test(
	task: &Task,
	set: &TestSet,
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
					.context("Failed to set namespace/database on upgrading database")?;
				if let Err(e) = resp.result {
					bail!("Failed to set namespace/database on upgrading database: {}", e.message)
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
				.context("Failed to authenticate on upgrading database")?;
			if let Err(e) = resp.result {
				bail!("Failed to authenticate on upgrading database: {}", e.message)
			}

			let source = &set[task.test].source;
			let source = std::str::from_utf8(source).context("Text source was not valid utf-8")?;
			connection.query(source).await
		})
		.await?
}

async fn run_task(task: Task, test_set: TestSet, config: Arc<Config>) -> (Task, TestTaskResult) {
	match run_task_inner(&task, test_set, config).await {
		Ok(x) => (task, x),
		Err(e) => (task, TestTaskResult::RunningError(e)),
	}
}

async fn run_task_inner(
	task: &Task,
	test_set: TestSet,
	config: Arc<Config>,
) -> Result<TestTaskResult> {
	let dir = &task.path;
	tokio::fs::create_dir(dir).await.context("Failed to create tempory directory for datastore")?;

	// no need to write the info if it is going to be deleted anyway.
	if config.keep_files {
		// write some info to the test directory usefull for later debugging.
		tokio::fs::write(Path::new(dir).join("test.info"), format!("{} => {}", task.from, task.to))
			.await?;
	}

	if test_set[task.test].config.env.as_ref().and_then(|x| x.capabilities.as_ref()).is_some() {
		bail!("Setting capabilities are not supported for upgrade tests")
	}

	let namespace =
		test_set[task.test].config.env.as_ref().map(|x| x.namespace()).unwrap_or(Some("test"));
	let database =
		test_set[task.test].config.env.as_ref().map(|x| x.database()).unwrap_or(Some("test"));

	if database.is_some() && namespace.is_none() {
		bail!("Cannot have a database set but not a namespace.")
	}

	// run imports
	let mut process = process::SurrealProcess::new(&config, &task.from, dir, task.port).await?;
	match run_imports(task, &test_set, &mut process, namespace, database).await {
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
		let process = process::SurrealProcess::new(&config, &task.to, dir, task.port).await?;
		match run_upgrade_test(task, &test_set, process, namespace, database).await {
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
