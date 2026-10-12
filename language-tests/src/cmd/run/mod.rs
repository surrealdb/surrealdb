use std::fmt::Write;
use std::io::IsTerminal;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::ArgMatches;
use executor::{EmbeddedExecutor, ExecOutcome, Executor};
use futures::FutureExt as _;
use provisioner::Provisioner;
use semver::Version;
use surrealdb_core::dbs::Session;
use surrealdb_core::env::VERSION;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::Value as SurValue;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::cli::{Backend, ColorMode, ResultsMode};
use crate::cmd::run::provisioner::CanReuse;
use crate::format::{IndentFormatter, Progress, ansi};
use crate::runner::Schedular;
use crate::tests::report::{TestGrade, TestReport, TestTaskResult};
use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::schema::ENV_DEFAULT_TIMEOUT;
use crate::tests::{CaseSet, RunSetBuilder, TestRun};

mod executor;
mod provisioner;

#[derive(Debug)]
pub struct TestRunConfig {
	pub backend: Backend,
}

impl RunConfig for TestRunConfig {
	fn name(&self, case: &CaseImports) -> String {
		format!("{} on {}", case.test.origin.path, self.backend)
	}
}

pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let mut load_errors = Vec::new();

	let path: &String = matches.get_one("path").unwrap();
	let set = CaseSet::load_surrealql_files(path, &mut load_errors).await?;

	let backend = *matches.get_one::<Backend>("backend").unwrap();

	// A `--endpoint` selects the remote (SDK) execution path; without it the
	// runner uses an embedded datastore (the default).
	let endpoint = matches.get_one::<String>("endpoint").cloned();
	#[cfg(not(feature = "remote"))]
	if endpoint.is_some() {
		bail!("--endpoint requires building language-tests with `--features remote`");
	}
	let remote = endpoint.is_some();

	// Datastore configuration keys for every datastore this run opens. Only the
	// embedded path opens one: a remote run's datastores belong to the server and
	// are configured where it was started, so a key given here could not reach
	// them. Accepting it silently would leave the run on the server's settings
	// while reporting otherwise — the very thing the embedded path verifies
	// against the datastore rather than assuming.
	let config: Vec<(String, String)> = matches
		.get_many::<(String, String)>("config")
		.map(|pairs| pairs.cloned().collect())
		.unwrap_or_default();
	if remote && !config.is_empty() {
		bail!(
			"--config is not supported with --endpoint: it configures the datastores this process opens, and a remote run's belong to the server. Set these where the server is started."
		);
	}

	// The version gates below describe the datastore that runs the tests. That is
	// this build only when the datastore is embedded; a remote endpoint may be
	// any version, so ask it rather than assume. Gating on the wrong version
	// schedules tests the server cannot support and drops ones it can.
	#[cfg(feature = "remote")]
	let core_version = match endpoint.as_deref() {
		Some(endpoint) => {
			let version = crate::cmd::remote::endpoint_version(endpoint).await?;
			println!(" Endpoint reports SurrealDB {version}; version gates follow it");
			version
		}
		None => Version::parse(VERSION).unwrap(),
	};
	#[cfg(not(feature = "remote"))]
	let core_version = Version::parse(VERSION).unwrap();

	// Check if the backend is supported by the enabled features.
	//
	// Only the embedded path opens a store in this process, so only it needs the
	// feature compiled in. A remote run still needs `--backend` — it selects which
	// tests the *server's* storage engine can answer, and a test declaring
	// `[env] backend` is filtered on it — but the harness never opens that engine
	// itself, and the `remote` build enables none of them.
	if !remote {
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
	}

	let set_builder = RunSetBuilder::new(&set, &mut load_errors)
		// Only run test for which run is enabled.
		.with_filter(|x| x.test.config.parsed.test.run)
		// A `[test.chain]` test has no meaning here. Its fixture is created by
		// the chain runner's `setup` on whichever version first supports the
		// feature, and its body asserts against `$hop`, which only a walk binds
		// — run standalone it would query a table nothing created.
		.with_filter(|x| x.test.config.parsed.test.chain.is_none())
		// Only run test for this backend.
		.with_filter(|x| {
			let config_backend = &x.test.config.parsed.env.backend;
			config_backend.is_empty() || config_backend.contains(&backend)
		})
		// A versioned (MVCC time-travel) test can only run where the provisioner
		// can open the store with versioning; elsewhere the engine rejects the
		// statement rather than answering it.
		//
		// A remote run keeps them regardless: the server's storage is not this
		// runner's to choose, and `remote_unsupported` reports every one of them
		// with a reason. Dropping them here instead would take them out of both
		// the scheduled and the skipped counts, so the run would look smaller
		// rather than partly unsupported.
		.with_filter(move |x| {
			!x.test.config.parsed.env.versioned || backend.supports_versioning() || remote
		})
		// One run per test.
		.with_expander(|_| {
			vec![TestRunConfig {
				backend,
			}]
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

		// A feature this build cannot create is not testable against it, and the
		// test's fixture would fail to parse or apply. Matched the same way the
		// upgrade expander matches it: strict semver rejects every prerelease,
		// and the workspace version is one, so a strict check here would drop
		// every gated test from this runner.
		if let Some(x) = &x.test.config.parsed.test.introduced
			&& !crate::version::version_matches(x, &core_version)
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

	// Expectations are the corpus's statement of what SurrealDB must answer, so
	// they are only ever written from a run of this checkout. A remote endpoint is
	// an arbitrary server — a different version, or a different implementation
	// entirely — and letting it fill an empty block would pin whatever it happens
	// to return as the canonical answer, silently and across the whole corpus.
	if remote && !matches!(failure_mode, ResultsMode::Default) {
		bail!(
			"--results is not supported with --endpoint: expectations must be generated from an embedded run of this checkout, not from whatever the remote server answers"
		);
	}

	// Every scheduled run must come back with a report. Checked in `finish`,
	// because a run that goes missing is invisible otherwise: the summary counts
	// what it was handed, so a dropped report reads as a smaller suite rather
	// than as a failure.
	let scheduled = runs.len();

	// `Some` when a remote run could not put the endpoint back the way it found
	// it, which fails the run in its own right below.
	let (reports, endpoint_left_dirty) = match endpoint {
		#[cfg(feature = "remote")]
		Some(endpoint) => {
			let user = matches
				.get_one::<String>("username")
				.cloned()
				.unwrap_or_else(|| "root".to_string());
			let pass = matches
				.get_one::<String>("password")
				.cloned()
				.unwrap_or_else(|| "root".to_string());
			run_remote_jobs(runs, num_jobs, endpoint, user, pass, color).await?
		}
		#[cfg(not(feature = "remote"))]
		Some(_) => unreachable!("--endpoint without the remote feature is rejected above"),
		// An embedded run opens its own datastores and leaves nothing to clean up.
		None => (run_embedded_jobs(runs, num_jobs, backend, config, color).await?, None::<String>),
	};

	let graded = finish(reports, scheduled, &load_errors, &set, path, color, failure_mode).await;

	// Residue on the endpoint fails the run even when every test passed: the
	// suite's own isolation rests on each run starting from a clean root, so a
	// green run that left state behind is the one outcome that would let the
	// next one grade against a polluted baseline. A failing `graded` is reported
	// in preference — the sweep printed its own account above either way.
	match endpoint_left_dirty {
		Some(reason) => graded.and_then(|()| Err(anyhow::anyhow!(reason))),
		None => graded,
	}
}

/// Schedules every run against an embedded datastore pool, collecting the reports.
async fn run_embedded_jobs(
	runs: Vec<TestRun<TestRunConfig>>,
	num_jobs: u32,
	backend: Backend,
	config: Vec<(String, String)>,
	color: ColorMode,
) -> Result<Vec<TestReport>> {
	println!(" Running with {num_jobs} jobs");
	let mut schedular = Schedular::new(num_jobs);

	// all reports are collected into the channel before processing.
	// So unbounded is required.
	let (report_send, mut report_recv) = mpsc::unbounded_channel();

	let mut provisioner = Provisioner::new(num_jobs as usize, backend, config).await?;

	println!("Found {} tests ", runs.len());

	let mut reports = Vec::new();
	let mut progress = Progress::from_stderr(runs.len(), color);

	// spawn all tests -- one task per test.
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

	Ok(reports)
}

/// Displays the collected reports, prints the summary, optionally updates test
/// configs with acquired results, and fails the run if any test failed.
async fn finish(
	reports: Vec<TestReport>,
	scheduled: usize,
	load_errors: &[crate::tests::TestLoadError],
	set: &CaseSet,
	path: &str,
	color: ColorMode,
	failure_mode: &ResultsMode,
) -> Result<()> {
	// A run that produced no report ran but was never graded, so nothing below
	// can see it. Fail rather than summarise a suite that quietly shrank.
	if reports.len() != scheduled {
		bail!(
			"{} of {scheduled} scheduled runs produced no report; the run is incomplete and its results cannot be trusted",
			scheduled - reports.len()
		);
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
	// passed/failed/warned are per-run counts (one report per test),
	// while skipped is a per-test count (tests excluded before the run is built).
	let passed = reports.iter().filter(|r| r.grade() == TestGrade::Success).count();
	let failed = reports.iter().filter(|r| r.grade() == TestGrade::Failed).count();
	let warned = reports.iter().filter(|r| r.grade() == TestGrade::Warning).count();
	let skipped = reports.iter().filter(|r| r.grade() == TestGrade::Skipped).count();
	if use_color {
		print!(ansi!(green, " {} runs passed", reset_format), passed);
		if failed > 0 {
			print!(ansi!(", ", red, "{} failed", reset_format), failed);
		}
		if warned > 0 {
			print!(ansi!(", ", yellow, "{} warnings", reset_format), warned);
		}
		if skipped > 0 {
			print!(", {skipped} skipped");
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
		if skipped > 0 {
			print!(", {skipped} skipped");
		}
		println!();
	}

	// Break down skips by reason, so it's clear what coverage was dropped.
	if skipped > 0 {
		let mut by_reason: std::collections::BTreeMap<&str, usize> =
			std::collections::BTreeMap::new();
		for r in reports.iter() {
			if let Some(reason) = r.skip_reason() {
				*by_reason.entry(reason).or_default() += 1;
			}
		}
		for (reason, count) in by_reason {
			println!("   - {count} skipped: {reason}");
		}
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
				let mut executor = EmbeddedExecutor::new(Arc::clone(ds));
				let fut = run_test(&run, &mut executor);
				let res = AssertUnwindSafe(fut).catch_unwind().await;

				match res {
					Ok(Ok(x)) => (
						CanReuse::Reusable,
						Ok(TestReport::from_test_result(run, x, grade_ds).await),
					),
					// An error from the executor is this test's outcome — report it as
					// one. Returning `Err` here sends no report at all, and the run
					// finishes a test short without noticing.
					Ok(Err(e)) => (
						CanReuse::Reusable,
						Ok(TestReport::from_test_result(
							run,
							TestTaskResult::RunningError(e),
							grade_ds,
						)
						.await),
					),
					Err(e) => (CanReuse::Reset, Ok(TestReport::from_panic(run, e))),
				}
			})
			.await;

		let res = match res {
			Ok(Ok(x)) => x,
			Ok(Err(e)) | Err(e) => {
				// The test never produced a report. `finish` refuses a run whose
				// report count is short of what it scheduled, so this ends the run
				// rather than quietly shrinking it.
				eprintln!("Could not run a test, so it has no result: {e:#}");
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

/// Whether a test's source defines, removes or inspects the root user `user`.
///
/// Matched on whitespace-normalised text rather than on a parse: this decides
/// whether to refuse a run up front, so over-matching costs a rejected
/// `--username` rather than a wrong result. Every form that names a root user
/// puts a keyword before `USER`, so the leading space is always present.
#[cfg(feature = "remote")]
fn touches_root_user(source: &str, user: &str) -> bool {
	let normalised = source.to_lowercase().split_whitespace().collect::<Vec<_>>().join(" ");
	let user = user.to_lowercase();

	[format!(" user {user} on root"), format!(" user `{user}` on root")]
		.iter()
		.any(|needle| normalised.contains(needle.as_str()))
}

/// Whether a test's source changes an endpoint-wide system setting.
///
/// `ALTER SYSTEM` is the only statement that reaches one. Such a test must run
/// alone, because the setting it changes applies to every request the server is
/// serving, not only to its own.
///
/// Matched on whitespace-normalised text for the same reason as
/// [`touches_root_user`]: this decides whether to refuse a run up front, and the
/// only cost of over-matching is a test asked to declare an exclusivity it did
/// not need. It cannot see a statement a function or an `execute` builds at run
/// time; the post-run snapshot still catches those, but only when the change
/// outlives the test.
#[cfg(feature = "remote")]
fn alters_system_config(source: &str) -> bool {
	source.to_lowercase().split_whitespace().collect::<Vec<_>>().join(" ").contains("alter system")
}

/// The namespace names a test's source removes outright.
///
/// `REMOVE NAMESPACE` is the one statement that destroys a namespace the
/// endpoint already held, and it is invisible to every check made after the
/// fact: [`RootSnapshot`](executor::RootSnapshot) compares name sets, so a test
/// that removes a name and defines it again leaves the set unchanged and the
/// databases under it gone. A test naming the namespace in its body rather than
/// its config is not covered by the configured-namespace check either — a test
/// declaring `namespace = false` has no configured name at all, yet is exclusive
/// precisely because it works at root.
///
/// The statement's target may be a parameter, which is why this reports what it
/// could not work out as well as what it could. `REMOVE NAMESPACE $ns` names a
/// namespace only once the server has evaluated the parameter, by which point
/// the data is already gone.
///
/// Matched on whitespace-normalised text for the same reason as
/// [`alters_system_config`], and ASCII-lowercased rather than lowercased so the
/// match offsets stay valid in the original-case text: the keywords are ASCII
/// but a namespace name need not be, and it is the name that has to come back
/// with its case intact.
#[cfg(feature = "remote")]
#[derive(Default)]
struct NamespaceRemovals {
	/// Names the source removes: written literally, or bound to a literal by a
	/// `LET` the source also carries.
	names: std::collections::BTreeSet<String>,
	/// Parameters whose value this cannot work out, spelled as they are written.
	/// A run cannot tell whether one of these names a namespace the endpoint
	/// holds, so it refuses rather than find out by destroying it.
	unresolved: std::collections::BTreeSet<String>,
}

/// The literal `LET $x = "..."` bindings a source makes, by parameter name.
///
/// Only a directly-quoted literal is taken. Anything computed is left out, and
/// a `REMOVE` naming it is reported unresolved rather than guessed at.
#[cfg(feature = "remote")]
fn literal_let_bindings(
	normalised: &str,
	lower: &str,
) -> std::collections::HashMap<String, String> {
	const LET: &str = "let $";

	let mut out = std::collections::HashMap::new();
	for (idx, _) in lower.match_indices(LET) {
		let rest = &normalised[idx + LET.len()..];
		let name_end = rest.find([' ', '=']).unwrap_or(rest.len());
		let (name, after) = rest.split_at(name_end);
		let Some(after) = after.trim_start().strip_prefix('=') else {
			continue;
		};
		let after = after.trim_start();
		let Some(quote) = after.chars().next().filter(|c| *c == '"' || *c == '\'') else {
			continue;
		};
		let value = &after[quote.len_utf8()..];
		let Some(end) = value.find(quote) else {
			continue;
		};
		if !name.is_empty() {
			out.insert(name.to_owned(), value[..end].to_owned());
		}
	}
	out
}

#[cfg(feature = "remote")]
fn removed_namespaces(source: &str) -> NamespaceRemovals {
	const STMT: &str = "remove namespace ";
	const IF_EXISTS: &str = "if exists ";

	let normalised = source.split_whitespace().collect::<Vec<_>>().join(" ");
	// `to_ascii_lowercase` is length-preserving, so an index into this is an
	// index into `normalised`; `to_lowercase` is not, and would misalign them.
	let lower = normalised.to_ascii_lowercase();
	let bindings = literal_let_bindings(&normalised, &lower);

	let mut out = NamespaceRemovals::default();
	for (idx, _) in lower.match_indices(STMT) {
		let from = idx + STMT.len();
		let (rest, rest_lower) = (&normalised[from..], &lower[from..]);
		let rest = match rest_lower.starts_with(IF_EXISTS) {
			true => &rest[IF_EXISTS.len()..],
			false => rest,
		};
		// A backtick-quoted name runs to its closing backtick and may hold the
		// separators below; a bare one ends at the statement separator or the
		// next clause.
		let token = match rest.strip_prefix('`') {
			Some(quoted) => quoted.split('`').next().unwrap_or_default(),
			None => rest.split([';', ' ']).next().unwrap_or_default(),
		};
		if token.is_empty() {
			continue;
		}
		match token.strip_prefix('$') {
			// A parameter: usable only if the source binds it to a literal.
			Some(param) => match bindings.get(param) {
				Some(name) => out.names.insert(name.clone()),
				None => out.unresolved.insert(token.to_owned()),
			},
			None => out.names.insert(token.to_owned()),
		};
	}
	out
}

/// Schedules every run against a remote SurrealDB instance, collecting the reports.
#[cfg(feature = "remote")]
async fn run_remote_jobs(
	runs: Vec<TestRun<TestRunConfig>>,
	num_jobs: u32,
	endpoint: String,
	user: String,
	pass: String,
	color: ColorMode,
) -> Result<(Vec<TestReport>, Option<String>)> {
	// Tests run concurrently against the one endpoint, each in a namespace of its
	// own named from this prefix. The prefix identifies the run as well as
	// separating it: every namespace a run creates carries it, so a leak check can
	// tell them from a namespace a test created itself, and an interrupted run
	// leaves an obviously labelled residue rather than an anonymous `test`.
	//
	// Process id and start time do not make it unique on their own. Two harness
	// processes can hold the same pid at the same moment — each in its own
	// container, where the harness is pid 1 — and starting within the same second
	// they would agree on both halves. Their per-test namespaces and provisioned
	// root users would then collide, and either run's cleanup would remove state
	// the other was still using. A random component is what actually separates
	// them; the pid and timestamp stay because they make a residue left by an
	// interrupted run legible.
	let run_prefix = format!(
		"lt_{:x}_{:x}_{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_secs())
			.unwrap_or_default(),
		// A namespace name has to stay a bare identifier, so the hyphens go.
		surrealdb_types::Uuid::new_v4().to_string().replace('-', "")
	);

	// The pooled connections re-authenticate as `user` between tests, so a test
	// that removes or redefines that root user locks every later test out of the
	// endpoint. Which tests those are depends on `--username`, because the corpus
	// names root users freely. Refuse before running rather than collapse from the
	// middle of a run, where the cause reads as a suite-wide authentication
	// failure.
	let colliding: std::collections::BTreeSet<String> = runs
		.iter()
		.filter(|r| executor::remote_unsupported(&r.case).is_none())
		.filter(|r| {
			// Imports run as the test, so a root user one of them defines locks the
			// run out exactly as the test's own body would.
			std::iter::once(&r.case.test)
				.chain(r.case.imports.iter())
				.any(|case| touches_root_user(&case.source, &user))
		})
		.map(|r| r.case.test.origin.path.to_string())
		.collect();

	if !colliding.is_empty() {
		bail!(
			"--username `{user}` names a root user that {} test(s) define or remove, which would \
			 lock the run out of {endpoint}: {}. Choose a --username the corpus does not use, or \
			 mark those tests `[env] remote-skip`.",
			colliding.len(),
			colliding.iter().take(10).cloned().collect::<Vec<_>>().join(", ")
		);
	}

	// A system setting is endpoint-wide, so a test that changes one while others
	// are running imposes it on them — a `QUERY_TIMEOUT` set for two statements
	// is a hard timeout every concurrent request inherits, failing neighbours for
	// a reason their own source does not explain. The post-run snapshot cannot
	// catch this: a test that restores the setting leaves nothing to find, and by
	// then the damage has landed elsewhere. Refuse up front instead.
	let undeclared: std::collections::BTreeSet<String> = runs
		.iter()
		.filter(|r| executor::remote_unsupported(&r.case).is_none())
		.filter(|r| !r.case.test.config.parsed.env.needs_exclusive_endpoint())
		.filter(|r| {
			// A test whose whole expectation is a parse error runs no statement, so
			// the `ALTER SYSTEM` its source carries never reaches the server.
			!matches!(
				r.case.test.config.parsed.test.results,
				Some(crate::tests::schema::TestDetailsResults::ParserError(_))
			)
		})
		.filter(|r| {
			std::iter::once(&r.case.test)
				.chain(r.case.imports.iter())
				.any(|case| alters_system_config(&case.source))
		})
		.map(|r| r.case.test.origin.path.to_string())
		.collect();

	if !undeclared.is_empty() {
		bail!(
			"{} test(s) change an endpoint-wide system setting but are not marked `[env] \
			 remote-exclusive`, so they would impose it on every test running beside them: {}. \
			 Add `remote-exclusive = true` to each.",
			undeclared.len(),
			undeclared.iter().take(10).cloned().collect::<Vec<_>>().join(", ")
		);
	}

	// A test that reaches outside its namespace can't be isolated by one, so it
	// takes the endpoint to itself. Counted up front to report the split.
	let exclusive =
		runs.iter().filter(|r| r.case.test.config.parsed.env.needs_exclusive_endpoint()).count();

	println!(" Running against {endpoint} with {num_jobs} jobs");
	let endpoint_label = endpoint.clone();
	let mut schedular = Schedular::new(num_jobs);
	let (report_send, mut report_recv) = mpsc::unbounded_channel();
	let mut provisioner =
		provisioner::RemoteProvisioner::new(num_jobs as usize, endpoint, user, pass).await?;
	let baseline = provisioner.root_baseline();

	// A namespace the endpoint already holds is destroyed, with every database
	// under it, by either of two things a run does, and neither can be undone
	// once done:
	//
	//   - An exclusive test keeps its configured namespace name — the corpus default is `test`, so
	//     most of them claim that one — and `prepare` drops that name before snapshotting root,
	//     because a namespace left behind by an earlier test would otherwise read as pre-existing
	//     and never be cleaned.
	//   - A test whose body removes a namespace by name. That one is invisible afterwards as well
	//     as irreversible: the snapshots compare name sets, so a test that removes a name and
	//     defines it again leaves the set unchanged while the databases under it are gone.
	//
	// The second reaches names the first cannot see. A test declaring
	// `namespace = false` is exclusive precisely because it works at root, and so
	// has no configured name to compare at all.
	//
	// A removal whose target is a parameter names nothing until the server has
	// evaluated it, too late to object. A parameter the source binds to a literal
	// is resolved and checked like any other name; one that is not is `undecidable`
	// below, and a run carrying one is refused outright rather than allowed to
	// find out by destroying something.
	use std::collections::BTreeSet;
	let mut occupied: std::collections::BTreeMap<String, BTreeSet<String>> =
		std::collections::BTreeMap::new();
	let mut undecidable: std::collections::BTreeMap<String, BTreeSet<String>> =
		std::collections::BTreeMap::new();
	for run in runs.iter().filter(|r| executor::remote_unsupported(&r.case).is_none()) {
		let path = run.case.test.origin.path.to_string();
		let env = &run.case.test.config.parsed.env;

		let configured = env
			.needs_exclusive_endpoint()
			.then(|| env.namespace())
			.flatten()
			.filter(|ns| baseline.has_namespace(ns))
			.map(str::to_owned);

		// Imports run as the test, so a namespace one of them removes is removed
		// just as the test's own body would remove it.
		let removals = std::iter::once(&run.case.test)
			.chain(run.case.imports.iter())
			.map(|case| removed_namespaces(&case.source))
			.fold(NamespaceRemovals::default(), |mut acc, r| {
				acc.names.extend(r.names);
				acc.unresolved.extend(r.unresolved);
				acc
			});

		let removed = removals.names.into_iter().filter(|ns| baseline.has_namespace(ns));
		for ns in configured.into_iter().chain(removed) {
			occupied.entry(ns).or_default().insert(path.clone());
		}
		for param in removals.unresolved {
			undecidable.entry(param).or_insert_with(BTreeSet::new).insert(path.clone());
		}
	}

	if !occupied.is_empty() {
		let detail = occupied
			.iter()
			.map(|(ns, tests)| {
				format!(
					"`{ns}` (claimed by {} test(s): {})",
					tests.len(),
					tests.iter().take(5).cloned().collect::<Vec<_>>().join(", ")
				)
			})
			.collect::<Vec<_>>()
			.join("; ");
		bail!(
			"{endpoint_label} already holds {} namespace(s) that a test claims or removes, which \
			 the run would drop whole and could not restore: {detail}. Point the run at an \
			 instance that does not hold them, or remove them yourself first.",
			occupied.len()
		);
	}

	if !undecidable.is_empty() {
		let detail = undecidable
			.iter()
			.map(|(param, tests)| {
				format!(
					"`{param}` (in {} test(s): {})",
					tests.len(),
					tests.iter().take(5).cloned().collect::<Vec<_>>().join(", ")
				)
			})
			.collect::<Vec<_>>()
			.join("; ");
		bail!(
			"{} namespace removal(s) name a parameter this cannot resolve, so whether they would \
			 destroy a namespace {endpoint_label} holds is unknowable until the server has already \
			 done it: {detail}. Bind the parameter to a literal in the test, or mark the test \
			 `[env] remote-skip`.",
			undecidable.len()
		);
	}

	println!("Found {} tests ", runs.len());
	println!(
		" {} run concurrently in `{run_prefix}_*` namespaces, {exclusive} exclusive",
		runs.len() - exclusive
	);

	let mut reports = Vec::new();
	let mut progress = Progress::from_stderr(runs.len(), color);

	for run in runs {
		progress.start_item(run.id, &run.name()).unwrap();
		schedule_run_remote(
			run,
			&mut schedular,
			&mut provisioner,
			&run_prefix,
			&baseline,
			num_jobs,
			report_send.clone(),
		)
		.await;

		while let Ok(report) = report_recv.try_recv() {
			let grade = report.grade();
			progress.finish_item(report.id, grade).unwrap();
			reports.push(report);
		}
	}

	std::mem::drop(report_send);
	while let Some(report) = report_recv.recv().await {
		let grade = report.grade();
		progress.finish_item(report.id, grade).unwrap();
		reports.push(report);
	}

	schedular.join_all().await;
	println!();

	// Every job is finished, so root is meant to look exactly as the run found
	// it; anything still there was left by a path that never reached `cleanup` —
	// a panicking test, or a connection that died mid-test. Nothing else reports
	// those: the leak check excuses this run's own prefix so that a live test's
	// namespace and user are not read as another test's leak, and an exclusive
	// test's root state is checked only by its own `cleanup`.
	let endpoint_left_dirty = match provisioner.sweep_to_baseline().await {
		Ok(residue) if residue.is_empty() => None,
		Ok(residue) => {
			let mut parts = Vec::new();
			if !residue.remaining.is_empty() {
				let mut part = format!("still present: {}", residue.remaining.join(", "));
				// Worth spelling out, because it is the one piece of residue that is
				// a hazard rather than a cost: the harness provisions root users with
				// a fixed password. `RootSnapshot::added_since` is what spells them.
				if residue.remaining.iter().any(|item| item.contains("USER ")) {
					part.push_str(
						" (a root user left this way holds the harness's fixed password)",
					);
				}
				parts.push(part);
			}
			// Destruction is the graver half and cannot be undone, so it is named
			// plainly: the endpoint held this before the run and does not now.
			if !residue.destroyed.is_empty() {
				parts.push(format!(
					"removed by this run and not restorable: {}",
					residue.destroyed.join(", ")
				));
			}
			let reason =
				format!("{endpoint_label} was not left as this run found it; {}", parts.join("; "));
			println!(" {reason}");
			Some(reason)
		}
		Err(e) => {
			let reason = format!(
				"could not establish whether {endpoint_label} was left as this run found it: {e:#}"
			);
			println!(" {reason}");
			Some(reason)
		}
	};

	if let Err(e) = provisioner.shutdown().await {
		println!("Shutdown error: {e:?}");
	}

	Ok((reports, endpoint_left_dirty))
}

#[cfg(feature = "remote")]
pub async fn schedule_run_remote(
	run: TestRun<TestRunConfig>,
	schedular: &mut Schedular,
	provisioner: &mut provisioner::RemoteProvisioner,
	run_prefix: &str,
	baseline: &Arc<executor::RootSnapshot>,
	jobs: u32,
	report_sender: UnboundedSender<TestReport>,
) {
	// Skip tests the remote executor can't faithfully reproduce, without taking
	// a connection from the pool.
	if let Some(reason) = executor::remote_unsupported(&run.case) {
		report_sender
			.send(TestReport::skipped(run, reason.reason()))
			.expect("Channel closed too early");
		return;
	}

	// A test that writes outside its own namespace, or asserts on the name of the
	// one it is in, needs the endpoint to itself; `spawn_sequential` below holds
	// every job slot for the duration so nothing else is running. Everything else
	// gets a namespace named from this run's prefix and shares the endpoint.
	let exclusive = run.case.test.config.parsed.env.needs_exclusive_endpoint();
	let isolation = if exclusive {
		executor::Isolation::Exclusive {
			run_prefix: run_prefix.to_owned(),
		}
	} else {
		executor::Isolation::Concurrent {
			ns: format!("{run_prefix}_{}", run.id.index()),
			run_prefix: run_prefix.to_owned(),
			baseline: baseline.clone(),
			jobs,
		}
	};
	let permit = provisioner.obtain().await;

	let future = async move {
		let res = permit
			.with(async |conn, grade_ds| {
				let mut executor = executor::RemoteExecutor::new(conn, isolation);
				let fut = run_test(&run, &mut executor);
				let res = AssertUnwindSafe(fut).catch_unwind().await;

				match res {
					Ok(Ok(x)) => (
						CanReuse::Reusable,
						Ok(TestReport::from_test_result(run, x, grade_ds).await),
					),
					// An error from the executor is this test's outcome — report it as
					// one. Returning `Err` here sends no report at all, and the run
					// finishes a test short without noticing.
					Ok(Err(e)) => (
						CanReuse::Reusable,
						Ok(TestReport::from_test_result(
							run,
							TestTaskResult::RunningError(e),
							grade_ds,
						)
						.await),
					),
					Err(e) => {
						// A panic unwinds past `run_test`'s own cleanup. The embedded
						// path is put right by `CanReuse::Reset` rebuilding the
						// datastore, but the endpoint is shared and keeps whatever the
						// test left, and the pooled connection is returned as it is. Undo
						// it here, or an exclusive test's root residue is read as
						// pre-existing by the next test's snapshot and never removed.
						if let Err(e) = executor.cleanup().await {
							eprintln!("Failed to clean up after a panicking test: {e:#}");
						}
						(CanReuse::Reset, Ok(TestReport::from_panic(run, e)))
					}
				}
			})
			.await;

		let res = match res {
			Ok(Ok(x)) => x,
			Ok(Err(e)) | Err(e) => {
				// The test never produced a report. `finish` refuses a run whose
				// report count is short of what it scheduled, so this ends the run
				// rather than quietly shrinking it.
				eprintln!("Could not run a test, so it has no result: {e:#}");
				return;
			}
		};

		report_sender.send(res).expect("Channel closed too early");
	};

	if exclusive {
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
	//
	// `!fl` is the store's feature-level record, which every new store writes when it is
	// created. It sits beside the version keys, outside the `/` root, so unlike the others its
	// prefix has no leading `/`.
	//
	// `/!nr` are node release records, written beside the node's heartbeat and so infrastructure
	// like `/!nd`. `/!mg` are ledger entries: a store's first stored write, or the raise of its
	// feature level, records the record-format entry, so the entry persists in any store that
	// has stored a record in the stored-value grammar.
	const ALLOWED_KEY_PREFIXES: &[&[u8]] =
		&[b"/!ni", b"/!nh", b"/!nd", b"/!nr", b"/!mg", b"/!ic", b"/!tl", b"!fl"];

	let txn = dbs.transaction(surrealdb_kvs::TransactionType::Read).await?;
	// The point of this check is to find keys the declared keyspace does *not*
	// describe, which is the one scan no key bound can express.
	let range = surrealdb_kvs::key::RawRange::every_key();
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

async fn run_test<E: Executor>(
	run: &TestRun<TestRunConfig>,
	executor: &mut E,
) -> Result<TestTaskResult> {
	let config = &run.case.test.config.parsed;

	// Establish the namespace/database/auth context for this test. If setup fails
	// (e.g. a remote endpoint rejecting a DEFINE), still run cleanup so nothing is
	// left behind on a shared endpoint, and report it as a running error rather
	// than crashing the task. Embedded `prepare` is infallible, so this is a no-op
	// there.
	if let Err(e) = executor.prepare(config).await {
		let _ = executor.cleanup().await;
		return Ok(TestTaskResult::RunningError(e));
	}

	// Run the test body, capturing any early-exit result. The cleanup below must
	// run on *every* exit path — otherwise seed data from an importing test that
	// fails before the query runs (e.g. a rejection test that also declares
	// `[env] imports`, or a failed signin) is left behind on the reused base
	// datastore and breaks every later test that imports the same schema.
	let outcome = run_test_body(run, executor).await;

	// Always clean up, then surface a body error (if any) ahead of a cleanup error.
	let cleanup = executor.cleanup().await;
	let outcome = outcome?;
	cleanup?;

	let (did_timeout, result) = match outcome {
		BodyOutcome::Early(result) => {
			// A body that stopped early still ran its imports, and an import creates
			// root-level state as readily as the query can. Hold it to the same
			// isolation contract as a body that ran: a leak outranks the early
			// result, as it does below, because it is what the other tests sharing
			// the endpoint saw.
			let leaked = executor.check_root_leak().await?;
			if !leaked.is_empty() {
				return Ok(TestTaskResult::LeakedRootState(leaked));
			}
			return Ok(result);
		}
		BodyOutcome::Executed {
			did_timeout,
			result,
		} => (did_timeout, result),
	};

	// If the test was not a clean test it should ensure that the datastore is reset for the next
	// test. A remote executor has no raw KV access and returns `None`, which skips the check.
	if !config.env.clean
		&& let Some(keys) = executor.check_retained_keys().await?
		&& !keys.is_empty()
	{
		return Ok(TestTaskResult::BadCleanup(keys));
	}

	// A test sharing an endpoint with others is isolated by its namespace alone,
	// so anything it created outside one outlived the cleanup above and was
	// visible to whatever ran alongside. Unlike the check above this is not
	// waived by `clean`: that key says root-level residue is expected, which is
	// exactly what makes a test unable to share an endpoint.
	let leaked = executor.check_root_leak().await?;
	if !leaked.is_empty() {
		return Ok(TestTaskResult::LeakedRootState(leaked));
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
async fn run_test_body<E: Executor>(
	run: &TestRun<TestRunConfig>,
	executor: &mut E,
) -> Result<BodyOutcome> {
	let config = &run.case.test.config.parsed;

	if let Some(x) = executor.run_imports(&run.case.imports).await? {
		return Ok(BodyOutcome::Early(TestTaskResult::Import(x.path, x.message)));
	}

	let timeout_duration =
		config.env.timeout.map(|x| x.0).into_value(ENV_DEFAULT_TIMEOUT).unwrap_or(Duration::MAX);

	if let Some(signup_vars) = config.env.signup.as_ref()
		&& let Err(e) = executor.signup(signup_vars.0.clone()).await
	{
		return Ok(BodyOutcome::Early(TestTaskResult::SignupError(e)));
	}

	if let Some(signin_vars) = config.env.signin.as_ref()
		&& let Err(e) = executor.signin(signin_vars.0.clone()).await
	{
		return Ok(BodyOutcome::Early(TestTaskResult::SigninError(e)));
	}

	match executor.execute(&run.case.test, timeout_duration).await? {
		ExecOutcome::ParserError(e) => Ok(BodyOutcome::Early(TestTaskResult::ParserError(e))),
		ExecOutcome::Executed {
			did_timeout,
			result,
		} => Ok(BodyOutcome::Executed {
			did_timeout,
			result,
		}),
	}
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
	// One pass deletes a bounded number of keys, so draining a prefix larger than
	// that budget takes several. Repeat until a pass reads no queue entry at all,
	// which is the only state that leaves nothing behind for the retained-key
	// check — `/!rc` is not one of the prefixes it tolerates.
	//
	// Bounded rather than unconditional: an entry the engine cannot act on stays
	// queued by design, so keying the loop on the queue being read would hang the
	// harness on it. Falling out leaves the residue for the retained-key check to
	// report, which names the test and the keys instead of stalling the run.
	const MAX_RECLAIM_PASSES: usize = 64;
	for _ in 0..MAX_RECLAIM_PASSES {
		let (batches, _) = Datastore::reclaim_tombstones(
			Arc::clone(dbs),
			std::time::Duration::from_secs(1),
			std::time::Duration::ZERO,
			tokio_util::sync::CancellationToken::new(),
		)
		.await
		.context("failed to drain tombstone reclaim queue")?;
		if batches == 0 {
			break;
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::SystemTime;

	use surrealdb_core::kvs::Datastore;
	use surrealdb_rpc::capabilities::{Capabilities, Targets};

	use super::*;
	use crate::tests::case::{CaseId, Dialect, Origin, TestCase};
	use crate::tests::run::{CaseImports, TestRunId};

	#[cfg(feature = "remote")]
	#[test]
	fn a_root_user_statement_is_matched_whatever_its_spelling() {
		for source in [
			"REMOVE USER foo ON ROOT;",
			"DEFINE USER foo ON ROOT PASSWORD 'x' ROLES OWNER;",
			"define user `foo` on root password 'x';",
			"DEFINE   USER\n\tfoo\n\tON ROOT;",
			"INFO FOR USER foo ON ROOT;",
		] {
			assert!(touches_root_user(source, "foo"), "should have matched: {source}");
		}
	}

	#[cfg(feature = "remote")]
	#[test]
	fn a_system_setting_change_is_matched_whatever_its_spelling() {
		for source in [
			"ALTER SYSTEM QUERY_TIMEOUT 2s;",
			"ALTER SYSTEM DROP QUERY_TIMEOUT;",
			"alter system compact;",
			"ALTER\n\tSYSTEM\tQUERY_TIMEOUT $t;",
			"LET $t = 2s;\nALTER SYSTEM QUERY_TIMEOUT $t;",
		] {
			assert!(alters_system_config(source), "should have matched: {source}");
		}
	}

	#[cfg(feature = "remote")]
	#[test]
	fn a_removed_namespace_is_named_whatever_the_spelling() {
		let names = |s| removed_namespaces(s).names.into_iter().collect::<Vec<_>>();

		assert_eq!(names("REMOVE NAMESPACE blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("remove namespace blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("REMOVE NAMESPACE IF EXISTS blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("REMOVE\n\tNAMESPACE\tblk_ns ;"), vec!["blk_ns"]);
		assert_eq!(names("REMOVE NAMESPACE `blk ns`;"), vec!["blk ns"]);
		// The statement as the corpus writes it, inside a block.
		assert_eq!(
			names("{\n  REMOVE NAMESPACE blk_ns;\n  DEFINE NAMESPACE blk_ns;\n};"),
			vec!["blk_ns"]
		);
		// Several, de-duplicated, and the name's own case is kept: namespace names
		// are case-sensitive, so lowercasing one would compare against the wrong
		// entry in the baseline.
		assert_eq!(
			names("REMOVE NAMESPACE One; REMOVE NAMESPACE two; REMOVE NAMESPACE One;"),
			vec!["One", "two"]
		);

		// Nothing to take: a database, or a namespace definition.
		assert!(removed_namespaces("REMOVE DATABASE blk_db;").names.is_empty());
		assert!(removed_namespaces("DEFINE NAMESPACE blk_ns;").names.is_empty());
		assert!(removed_namespaces("SELECT * FROM t;").names.is_empty());
	}

	#[cfg(feature = "remote")]
	#[test]
	fn a_removal_naming_a_parameter_is_resolved_or_refused() {
		// The corpus case: the name is bound to a literal two statements earlier,
		// so it is as knowable as if it had been written in place.
		let r = removed_namespaces(
			"LET $namespace = \"other\";\nDEFINE NAMESPACE $namespace;\nREMOVE NAMESPACE \
			 $namespace;",
		);
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["other"]);
		assert!(r.unresolved.is_empty());

		// Single quotes, and a binding made after the removal is still the same
		// binding as far as the server is concerned.
		let r = removed_namespaces("REMOVE NAMESPACE $ns; LET $ns = 'late';");
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["late"]);

		// Computed, so there is nothing to resolve and the run must refuse.
		let r = removed_namespaces("LET $ns = string::concat('a', 'b'); REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// Never bound at all.
		let r = removed_namespaces("REMOVE NAMESPACE $whatever;");
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$whatever"]);

		// A literal target is unaffected by any of this.
		let r = removed_namespaces("LET $ns = 'a'; REMOVE NAMESPACE plain;");
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["plain"]);
		assert!(r.unresolved.is_empty());
	}

	#[cfg(feature = "remote")]
	#[test]
	fn a_namespace_scoped_statement_is_not_a_system_setting() {
		for source in [
			// Database-scoped config, which lives inside the test's own namespace.
			"DEFINE CONFIG GRAPHQL AUTO;",
			"REMOVE CONFIG GRAPHQL;",
			// Other ALTER targets are scoped to the database.
			"ALTER SEQUENCE sq TIMEOUT 2s;",
			"ALTER TABLE person SCHEMAFULL;",
			// A record named after the statement, not the statement.
			"CREATE alter:system SET x = 1;",
		] {
			assert!(!alters_system_config(source), "should not have matched: {source}");
		}
	}

	#[cfg(feature = "remote")]
	#[test]
	fn a_statement_naming_another_user_or_level_is_left_alone() {
		for source in [
			// A different user whose name merely starts with ours.
			"REMOVE USER foobar ON ROOT;",
			// Ours, but not at root level.
			"DEFINE USER foo ON NAMESPACE PASSWORD 'x';",
			"DEFINE USER foo ON DATABASE PASSWORD 'x';",
			// A record named after the user, not a user statement.
			"CREATE user:foo SET root = true;",
		] {
			assert!(!touches_root_user(source, "foo"), "should not have matched: {source}");
		}
	}

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
				chain_setup: Vec::new(),
				chain_mutate: Vec::new(),
				repairs: Vec::new(),
			}),
			config: TestRunConfig {
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
		let mut exec_a = EmbeddedExecutor::new(Arc::clone(&dbs));
		let result_a = run_test(&run_a, &mut exec_a).await.unwrap();
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
		let mut exec_b = EmbeddedExecutor::new(Arc::clone(&dbs));
		let result_b = run_test(&run_b, &mut exec_b).await.unwrap();
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
