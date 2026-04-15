mod binaries;
mod process;
mod protocol;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use process::SurrealProcess;
use protocol::{ProxyObject, ProxyValue};
use semver::Version;
use surrealdb_core::kvs::Datastore;
use tokio::task::JoinSet;

use crate::cli::{ColorMode, DsVersion, ResultsMode, UpgradeBackend};
use crate::format::Progress;
use crate::tests::report::{TestGrade, TestReport, TestTaskResult};
use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::schema::{BoolOr, ENV_DEFAULT_DATABASE, ENV_DEFAULT_NAMESPACE};
use crate::tests::{CaseSet, RunSetBuilder, TestRun};
use crate::util::TempDir;

pub struct UpgradeTestConfig {
	from: DsVersion,
	to: DsVersion,
	port: u16,
	path: String,
}

impl RunConfig for UpgradeTestConfig {
	fn name(&self, case: &CaseImports) -> String {
		format!("{} {} => {}", case.test.origin.path, self.from, self.to)
	}
}

pub struct Config {
	jobs: u32,
	download_permission: bool,
	backend: UpgradeBackend,
	keep_files: bool,
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
		}
	}
}

/// Main subcommand function, runs the actual subcommand.
pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let config = Config::from_matches(matches);
	let config = Arc::new(config);
	let mut load_errors = Vec::new();

	let path: &String = matches.get_one("path").unwrap();
	let results_mode = matches.get_one::<ResultsMode>("results").unwrap();
	let filter = matches.get_one::<String>("filter").cloned().unwrap_or_default();
	let no_wip = matches.get_flag("no-wip");
	let no_results = matches.get_flag("no-results");

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
	for v in all_versions {
		let actual = binaries::actual_version(v.clone()).await?;
		binaries::prepare(v.clone(), config.download_permission).await?;
		actual_version.insert(v, actual);
	}

	let temp_dir = TempDir::new("surreal_upgrade_tests")
		.await
		.context("Failed to create temporary directory for datastore")?;

	let case_set = CaseSet::load_surrealql_files(path.as_str(), &mut load_errors).await?;

	let run_set = RunSetBuilder::new(&case_set, &mut load_errors)
		.with_filter(|x| x.test.config.parsed.test.run)
		.with_filter(|x| x.test.config.parsed.test.upgrade)
		.with_filter(|x| x.test.origin.path.contains(&filter))
		.with_filter(|x| no_wip || !x.test.config.parsed.test.wip)
		.with_filter(|x| no_results || x.test.config.parsed.test.results.is_some())
		.with_expander(|x| {
			let mut res = Vec::new();

			for from in from_versions.iter() {
				for to in to_versions.iter() {
					let from_v = actual_version.get(from).unwrap();
					let to_v = actual_version.get(to).unwrap();

					if let Some(ver_req) = x.test.config.parsed.test.version.as_ref()
						&& !ver_req.matches(to_v)
					{
						continue;
					}

					if let Some(ver_req) = x.test.config.parsed.test.importing_version.as_ref()
						&& !ver_req.matches(from_v)
					{
						continue;
					}

					if x.imports.iter().any(|imp| {
						imp.config
							.parsed
							.test
							.version
							.as_ref()
							.map(|x| !x.matches(from_v))
							.unwrap_or(false)
					}) {
						continue;
					}

					res.push(UpgradeTestConfig {
						from: from.clone(),
						to: to.clone(),
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
			res
		})
		.build();

	println!("Using directory '{}' as a store directory", temp_dir.path().display());
	println!("Running {} test runs for {} test cases", run_set.len(), case_set.len());

	let mut progress = Progress::from_stderr(run_set.len(), color);

	let mut reports = Vec::new();

	let ds = Datastore::new("memory")
		.await
		.expect("failed to create datastore for running matching expressions");

	let mut session = surrealdb_core::dbs::Session::default();
	ds.process_use(None, &mut session, Some("match".to_string()), Some("match".to_string()))
		.await
		.unwrap();

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

					run.config.port = port;

					let id = run.id;
					let name = run.name();
					join_set.spawn(run_task(run, config.clone()));
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
		reuse_port.push(run.config.port);

		let id = run.id;
		let report = TestReport::from_test_result(run, result, &ds).await;
		let grade = report.grade();
		reports.push(report);
		progress.finish_item(id, grade).unwrap();
	}

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

	if reports.iter().any(|x| x.grade() == TestGrade::Failed) {
		bail!("Not all tests were successfull")
	}

	Ok(())
}

fn is_port_available(port: u16) -> bool {
	std::net::TcpListener::bind(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), port)).is_ok()
}

async fn run_imports(
	run: &TestRun<UpgradeTestConfig>,
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
			let imports = &run.case.imports;

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
					bail!("Failed to set namespace/database on importing database: {}", e.message())
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
				bail!("Failed to authenticate on importing database: {}", e.message())
			}

			for import in imports {
				match connection.query(&import.source).await {
					Ok(TestTaskResult::RunningError(e)) => {
						return Ok(Some(TestTaskResult::Import(
							import.origin.path.clone(),
							format!("Failed to run import: {e:?}"),
						)));
					}
					Err(e) => {
						return Ok(Some(TestTaskResult::Import(
							import.origin.path.clone(),
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
					bail!("Failed to set namespace/database on upgrading database: {}", e.message())
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
				bail!("Failed to authenticate on upgrading database: {}", e.message())
			}

			let source = &run.case.test.source;
			connection.query(source).await
		})
		.await?
}

async fn run_task(
	run: TestRun<UpgradeTestConfig>,
	config: Arc<Config>,
) -> (TestRun<UpgradeTestConfig>, TestTaskResult) {
	match run_task_inner(&run, config).await {
		Ok(x) => (run, x),
		Err(e) => (run, TestTaskResult::RunningError(e)),
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

	let BoolOr::Bool(false) = run.case.test.config.parsed.env.capabilities else {
		bail!("Setting capabilities are not supported for upgrade tests")
	};

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

	// run imports
	let mut process =
		process::SurrealProcess::new(&config, &run.config.from, dir, run.config.port).await?;
	match run_imports(run, &mut process, namespace, database).await {
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
			process::SurrealProcess::new(&config, &run.config.to, dir, run.config.port).await?;
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
