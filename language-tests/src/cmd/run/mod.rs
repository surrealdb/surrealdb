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
use surrealdb_rpc::capabilities::NewPlannerStrategy;
use surrealdb_types::Value as SurValue;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::cli::{Backend, ColorMode, ResultsMode};
use crate::cmd::run::provisioner::CanReuse;
use crate::format::{IndentFormatter, Progress, ansi};
use crate::runner::Schedular;
use crate::tests::report::{TestGrade, TestReport, TestTaskResult};
use crate::tests::run::{CaseImports, RunConfig};
use crate::tests::schema::{ENV_DEFAULT_TIMEOUT, NewPlannerStrategyConfig};
use crate::tests::{CaseSet, RunSetBuilder, TestRun};

mod executor;
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

	// A `--endpoint` selects the remote (SDK) execution path; without it the
	// runner uses an embedded datastore (the default).
	let endpoint = matches.get_one::<String>("endpoint").cloned();
	#[cfg(not(feature = "remote"))]
	if endpoint.is_some() {
		bail!("--endpoint requires building language-tests with `--features remote`");
	}
	let remote = endpoint.is_some();

	// The one planner strategy a remote run has, and `None` for an embedded one,
	// which selects a strategy per session and so runs the whole axis.
	//
	// The strategy is the server's startup configuration (its own
	// `--planner-strategy`), not something a session can choose, so the runner is
	// told which one the endpoint holds rather than selecting it. Clap bounds this
	// to a run that has an `--endpoint`.
	let remote_strategy: Option<NewPlannerStrategyConfig> = remote.then(|| {
		(*matches.get_one::<NewPlannerStrategy>("planner-strategy").unwrap()).into()
	});

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
		// An embedded run owns the strategy, so it runs the test once per strategy
		// the test declares. A remote one does not: the strategy is the server's
		// startup configuration and cannot be selected per statement over the SDK,
		// so the axis collapses to the single strategy `--planner-strategy` names.
		// A test that does not declare that strategy is reported by
		// `remote_unsupported` rather than graded under one it did not ask for.
		.with_expander(move |x| match remote_strategy {
			Some(planner_config) => vec![TestRunConfig {
				planner_config,
				backend,
			}],
			None => x
				.test
				.config
				.parsed
				.env
				.planner_strategy
				.iter()
				.map(|x| TestRunConfig {
					planner_config: *x,
					backend,
				})
				.collect(),
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
			run_remote_jobs(runs, num_jobs, endpoint, user, pass, remote_strategy, color).await?
		}
		#[cfg(not(feature = "remote"))]
		Some(_) => unreachable!("--endpoint without the remote feature is rejected above"),
		// An embedded run opens its own datastores and leaves nothing to clean up.
		None => (run_embedded_jobs(runs, num_jobs, backend, color).await?, None::<String>),
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
	color: ColorMode,
) -> Result<Vec<TestReport>> {
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
	// passed/failed/warned are per-run counts (one report per test-strategy pair),
	// while skipped is a per-test count (tests excluded before strategy expansion).
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
				let mut executor = EmbeddedExecutor::new(Arc::clone(ds), run.config.planner_config);
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

/// Whether a test's source defines, alters, removes or inspects the root user
/// `user`.
///
/// Matched on comment-stripped, whitespace-normalised text rather than on a
/// parse: this decides whether to refuse a run up front, so over-matching costs
/// a rejected `--username` while under-matching costs the run its login and
/// every test after it.
///
/// A match is anchored on the statement keyword, because ` USER ` on its own is
/// an ordinary field name. Within such a statement the optional clauses the
/// parser accepts between `USER` and the name — `IF EXISTS` on a removal,
/// `IF NOT EXISTS` or `OVERWRITE` on a definition — are eaten first; requiring
/// the name immediately after `USER` misses `REMOVE USER IF EXISTS root ON
/// ROOT`, which deletes the account, and `DEFINE USER OVERWRITE root ...`,
/// which changes its password.
///
/// A name this cannot read as a literal cannot be ruled out. A delimited one
/// counts outright, because it may decode to anything; any other expression
/// counts whenever the statement's scope is `ON ROOT`.
#[cfg(feature = "remote")]
fn touches_root_user(source: &str, user: &str) -> bool {
	const USER: &str = " user ";
	const CLAUSES: [&str; 3] = ["if not exists ", "if exists ", "overwrite "];
	const KEYWORDS: [&str; 4] = ["define", "alter", "remove", "info for"];

	let source = strip_comments(source);
	let normalised = source.split_whitespace().collect::<Vec<_>>().join(" ");
	let lower = normalised.to_ascii_lowercase();
	let wanted = user.to_lowercase();

	for (idx, _) in lower.match_indices(USER) {
		// `user` is a keyword only after the statement that introduces one.
		// Any character that cannot be part of an identifier ends the keyword,
		// `;` included: a statement need not be separated from the one before it
		// by whitespace.
		let head = &lower[..idx];
		let introduced = KEYWORDS.iter().any(|kw| {
			head.strip_suffix(kw).is_some_and(|before| {
				!before.chars().next_back().is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
			})
		});
		if !introduced {
			continue;
		}

		let mut at = idx + USER.len();
		for clause in CLAUSES {
			if lower[at..].starts_with(clause) {
				at += clause.len();
			}
		}
		let rest = &normalised[at..];
		// The name, and whatever follows it. A name that cannot be read here —
		// an expression, or a literal whose escapes the server decodes — leaves
		// only the statement's scope to go on.
		let (name, tail) = match delimited_name(rest) {
			Delimited::Name(name, tail) => (name, tail),
			Delimited::Unreadable => return true,
			Delimited::None => {
				let end = rest.find(' ').unwrap_or(rest.len());
				let (name, tail) = rest.split_at(end);
				if !is_bare_ident(name) {
					let statement = rest.split(';').next().unwrap_or(rest).to_ascii_lowercase();
					if statement.contains("on root") {
						return true;
					}
					continue;
				}
				(name, tail)
			}
		};
		if name.to_lowercase() != wanted {
			continue;
		}
		if tail.trim_start().to_ascii_lowercase().starts_with("on root") {
			return true;
		}
	}
	false
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
	strip_comments(source)
		.to_lowercase()
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ")
		.contains("alter system")
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
	/// Targets whose value this cannot work out, spelled as they are written —
	/// an unbound or reassigned parameter, or any computed expression. A run
	/// cannot tell whether one of these names a namespace the endpoint holds, so
	/// it refuses rather than find out by destroying it.
	unresolved: std::collections::BTreeSet<String>,
}

/// Every `LET $x = ...` a source makes, in source order, as
/// `(offset, name, value)`.
///
/// `value` is `None` for an assignment whose right-hand side is not a plain
/// quoted literal — a call, a concatenation, a literal carrying an escape the
/// server would decode. Such an assignment is recorded rather than dropped,
/// because a binding that cannot be read is still a binding: leaving it out
/// would let the literal assigned before it stand in for the value the
/// statement actually reads.
///
/// The offset is kept because a binding is not a property of the whole source:
/// the same parameter may be assigned more than once, and only the assignments
/// ahead of a given statement can be the one it reads.
///
/// `masked_lower` is the lowercased text with string contents blanked, and is
/// what the `LET` keyword is located in; values are read from `normalised` at
/// the same offsets. A `LET` written inside a string literal is not a binding,
/// and counting one lets a removal resolve to a value the server never sees.
#[cfg(feature = "remote")]
fn let_bindings(normalised: &str, masked_lower: &str) -> Vec<(usize, String, Option<String>)> {
	const LET: &str = "let $";

	let mut out = Vec::new();
	for (idx, _) in masked_lower.match_indices(LET) {
		let rest = &normalised[idx + LET.len()..];
		let name_end = rest.find([' ', '=']).unwrap_or(rest.len());
		let (name, after) = rest.split_at(name_end);
		if name.is_empty() {
			continue;
		}
		let value = after
			.trim_start()
			.strip_prefix('=')
			.map(str::trim_start)
			.and_then(|after| match delimited_name(after) {
				// A quoted literal stands for itself, but only when it is the
				// whole value: `'a' + 'b'` opens with one and is not one. A
				// backtick or angle-bracket name is an idiom here, not a string.
				Delimited::Name(value, tail) if after.starts_with(['"', '\'']) => {
					let tail = tail.trim_start();
					(tail.is_empty() || tail.starts_with(';')).then(|| value.to_owned())
				}
				_ => None,
			});
		out.push((idx, name.to_owned(), value));
	}
	// `match_indices` already walks left to right; sorted explicitly so callers
	// may rely on the order rather than on that detail.
	out.sort_by_key(|(idx, _, _)| *idx);
	out
}

/// The literal a `REMOVE` at `at` would read for `$param`, or `None` when a
/// text scan cannot say.
///
/// Only the bindings ahead of the statement are candidates, and only when there
/// is exactly one. Two is a reassignment — a branch, a loop, or a plain
/// overwrite — and which one is live at the statement is a question about
/// execution that reading the source cannot answer. `None` makes the run refuse
/// rather than act on a guess, which is the safe direction: the alternative is
/// destroying a namespace nobody named.
#[cfg(feature = "remote")]
fn binding_in_scope<'a>(
	bindings: &'a [(usize, String, Option<String>)],
	param: &str,
	at: usize,
) -> Option<&'a str> {
	let mut seen = bindings
		.iter()
		.filter(|(idx, name, _)| *idx < at && name == param)
		.map(|(_, _, value)| value.as_deref());
	match (seen.next(), seen.next()) {
		(Some(value), None) => value,
		_ => None,
	}
}

/// `normalised` with the contents of every quoted literal blanked out, byte
/// for byte, so that an index into one is an index into the other.
///
/// A keyword inside a string is text, not a statement, so a scan that resolves
/// a name must not read one. Only the scans whose mistake is *fail-open* use
/// this: a phantom `LET` supplies a value a removal then resolves to, which
/// destroys a namespace the source never names. The scans whose mistake is a
/// refusal — a removal, a root user, a system setting — read unmasked text on
/// purpose, so that a statement handed to `execute()` as a string is still
/// seen.
#[cfg(feature = "remote")]
fn mask_string_contents(normalised: &str) -> String {
	const FILLER: u8 = 0x01;

	let bytes = normalised.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		let q = bytes[i];
		if !matches!(q, b'"' | b'\'' | b'`') {
			out.push(q);
			i += 1;
			continue;
		}
		// The delimiters are kept so the span still reads as a literal; only
		// what is between them is blanked.
		out.push(q);
		i += 1;
		while i < bytes.len() {
			match bytes[i] {
				b'\\' if q != b'`' => {
					out.push(FILLER);
					out.push(FILLER);
					i += 2;
				}
				b if b == q => {
					out.push(q);
					i += 1;
					break;
				}
				_ => {
					out.push(FILLER);
					i += 1;
				}
			}
		}
	}
	// A `\\` straddling the end of the text advances two bytes past one.
	out.truncate(bytes.len());
	String::from_utf8(out).expect("the filler and the delimiters are ASCII")
}

/// The length in bytes of the line terminator at `bytes[i]`, or zero.
///
/// A single-line comment ends at any of these, so a scan that stops only at
/// CR or LF swallows the rest of the source — including the target of the very
/// statement it is reading. The set is the lexer's: CR, LF, and the three
/// Unicode terminators NEL, LS and PS.
#[cfg(feature = "remote")]
fn line_terminator_len(bytes: &[u8], i: usize) -> usize {
	match bytes[i] {
		b'\r' | b'\n' => 1,
		// U+0085 NEL.
		0xC2 if bytes.get(i + 1) == Some(&0x85) => 2,
		// U+2028 LS and U+2029 PS.
		0xE2 if matches!(bytes.get(i + 1), Some(&0x80))
			&& matches!(bytes.get(i + 2), Some(&0xA8) | Some(&0xA9)) =>
		{
			3
		}
		_ => 0,
	}
}

/// Replace every comment with a single space, leaving string and backtick-quoted
/// literals untouched.
///
/// A comment is a token separator to the lexer, not whitespace around one, so
/// `REMOVE/* note */NAMESPACE customer` is a removal and `REMOVE NAMESPACE --
/// note` continues on the next line. Scanning the raw text sees neither, and a
/// removal it does not see is one the pre-flight cannot refuse.
///
/// A space rather than nothing, so the comment still separates the tokens it
/// sat between. The forms are the lexer's: `--`, `//` and `#` to end of line,
/// and `/* */`. A test's frontmatter is one of these too, which is why prose in
/// a `reason` cannot be read as a statement.
#[cfg(feature = "remote")]
fn strip_comments(source: &str) -> String {
	let bytes = source.as_bytes();
	let mut out = String::with_capacity(source.len());
	let mut i = 0;
	while i < bytes.len() {
		match bytes[i] {
			// A quoted literal is copied whole: its contents are data, and a `--`
			// inside one starts no comment.
			q @ (b'"' | b'\'' | b'`') => {
				let start = i;
				i += 1;
				while i < bytes.len() {
					match bytes[i] {
						b'\\' if q != b'`' => i += 2,
						b if b == q => {
							i += 1;
							break;
						}
						_ => i += 1,
					}
				}
				out.push_str(&source[start..i.min(bytes.len())]);
			}
			b'/' if bytes.get(i + 1) == Some(&b'*') => {
				i += 2;
				while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
					i += 1;
				}
				i = (i + 2).min(bytes.len());
				out.push(' ');
			}
			b'#' => {
				while i < bytes.len() && line_terminator_len(bytes, i) == 0 {
					i += 1;
				}
				out.push(' ');
			}
			b'-' if bytes.get(i + 1) == Some(&b'-') => {
				while i < bytes.len() && line_terminator_len(bytes, i) == 0 {
					i += 1;
				}
				out.push(' ');
			}
			b'/' if bytes.get(i + 1) == Some(&b'/') => {
				while i < bytes.len() && line_terminator_len(bytes, i) == 0 {
					i += 1;
				}
				out.push(' ');
			}
			_ => {
				// Step by whole characters so a multi-byte one is not split.
				let ch = source[i..].chars().next().unwrap_or('\0');
				out.push(ch);
				i += ch.len_utf8();
			}
		}
	}
	out
}

/// What a removal's target reads as to a scan of the source.
///
/// The parser takes the target as a full expression and the server coerces
/// whatever it evaluates to into a string, so `REMOVE NAMESPACE
/// type::string('customer')` is as valid as a bare name and destroys the same
/// namespace. That space cannot be enumerated by reading text, so anything not
/// provably a literal is [`Target::Computed`] and refuses the run. Failing
/// closed is the whole point: the cost of refusing is a message, and the cost
/// of guessing wrong is a namespace nobody can restore.
#[cfg(feature = "remote")]
enum Target {
	/// A bare identifier, a backtick-quoted name, or a whole string literal.
	Name(String),
	/// `$param`, resolvable against the bindings ahead of the statement.
	Param(String),
	/// Anything else: a call, a concatenation, an arithmetic expression.
	Computed(String),
	/// No target at all — a truncated statement.
	Empty,
}

/// Whether `s` is a bare SurrealQL identifier, which needs no quoting and so
/// stands for itself.
#[cfg(feature = "remote")]
fn is_bare_ident(s: &str) -> bool {
	let mut chars = s.chars();
	chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
		&& chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A name's spelling when the text opens with one of the delimiters the lexer
/// accepts around an identifier or a string.
#[cfg(feature = "remote")]
enum Delimited<'a> {
	/// The text does not open with a delimiter.
	None,
	/// A delimited name that stands for itself, and the text following it.
	Name(&'a str, &'a str),
	/// Delimited, but not readable as written: the delimiter is never closed,
	/// or the name carries an escape sequence. The server decodes escapes, so
	/// the source text is not the name — `'ro\u{6f}t'` is `root`.
	Unreadable,
}

/// Read a delimited name, if the text opens with one.
///
/// The lexer takes `` ` ``, `"`, `'` and `⟨`…`⟩` around a name. Decoding what
/// is inside belongs to the lexer, so anything carrying a backslash is
/// reported unreadable rather than guessed at: a caller must then fall back on
/// whatever it does for a name it cannot resolve.
#[cfg(feature = "remote")]
fn delimited_name(s: &str) -> Delimited<'_> {
	let (open, close) = match s.chars().next() {
		Some(c @ ('`' | '"' | '\'')) => (c, c),
		Some(c @ '⟨') => (c, '⟩'),
		_ => return Delimited::None,
	};
	let body = &s[open.len_utf8()..];
	let Some(end) = body.find(close) else {
		return Delimited::Unreadable;
	};
	let (name, tail) = (&body[..end], &body[end + close.len_utf8()..]);
	// A backslash both changes what the name decodes to and can escape the
	// delimiter the scan just stopped at, so the name may not even end here.
	if name.contains('\\') {
		return Delimited::Unreadable;
	}
	Delimited::Name(name, tail)
}

/// Classify the text following `REMOVE NAMESPACE` and its clauses.
#[cfg(feature = "remote")]
fn removal_target(rest: &str) -> Target {
	// A delimited name runs to its closing delimiter and may hold the
	// separators below. What follows has to end the statement: otherwise the
	// name is merely where a longer expression starts.
	match delimited_name(rest) {
		Delimited::Name(name, tail) => {
			let tail = tail.trim_start();
			return match tail.is_empty() || tail.starts_with(';') {
				true => Target::Name(name.to_owned()),
				false => {
					Target::Computed(rest.split(';').next().unwrap_or(rest).trim().to_owned())
				}
			};
		}
		Delimited::Unreadable => {
			return Target::Computed(rest.split(';').next().unwrap_or(rest).trim().to_owned());
		}
		Delimited::None => {}
	}

	let seg = rest.split(';').next().unwrap_or_default().trim();
	if seg.is_empty() {
		return Target::Empty;
	}
	if let Some(param) = seg.strip_prefix('$')
		&& is_bare_ident(param)
	{
		return Target::Param(param.to_owned());
	}
	if is_bare_ident(seg) {
		return Target::Name(seg.to_owned());
	}
	Target::Computed(seg.to_owned())
}

#[cfg(feature = "remote")]
fn removed_namespaces(source: &str) -> NamespaceRemovals {
	// Every spelling the parser accepts, in the order it reads them:
	// `REMOVE (NAMESPACE|NS) [AND EXPUNGE] [IF EXISTS] <name>`. `NS` lexes to the
	// same keyword, and the two optional clauses are fixed in this order. A
	// spelling missed here is not a lesser check but no check at all — the
	// namespace is destroyed and the end-of-run sweep can only report the loss.
	const STMTS: [&str; 2] = ["remove namespace ", "remove ns "];
	const CLAUSES: [&str; 2] = ["and expunge ", "if exists "];

	let source = strip_comments(source);
	let normalised = source.split_whitespace().collect::<Vec<_>>().join(" ");
	// `to_ascii_lowercase` is length-preserving, so an index into this is an
	// index into `normalised`; `to_lowercase` is not, and would misalign them.
	let lower = normalised.to_ascii_lowercase();
	let masked = mask_string_contents(&lower);
	let bindings = let_bindings(&normalised, &masked);
	// A `/` may open a regex, whose body is data the lexer reads as one token
	// and which may hold anything at all — a `LET` among it. Whether a given
	// `/` opens one, or divides, is a question about the parse, so while one is
	// present no parameter is resolvable. Refusing here can only turn a
	// resolution into a refusal, never the reverse: masking the span instead
	// could swallow a second assignment and leave a single binding standing for
	// a value the statement never reads.
	let regex_possible = masked.contains('/');

	let mut out = NamespaceRemovals::default();
	let mut starts: Vec<usize> = STMTS
		.iter()
		.flat_map(|stmt| lower.match_indices(stmt).map(|(idx, _)| idx + stmt.len()))
		.collect();
	starts.sort_unstable();
	for mut at in starts {
		for clause in CLAUSES {
			if lower[at..].starts_with(clause) {
				at += clause.len();
			}
		}
		match removal_target(&normalised[at..]) {
			Target::Name(name) => out.names.insert(name),
			// Usable only where the source binds it to one literal ahead of this
			// statement.
			Target::Param(param) => {
				match (!regex_possible).then(|| binding_in_scope(&bindings, &param, at)).flatten()
				{
					Some(name) => out.names.insert(name.to_owned()),
					None => out.unresolved.insert(format!("${param}")),
				}
			}
			Target::Computed(text) => out.unresolved.insert(text),
			Target::Empty => continue,
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
	strategy: Option<NewPlannerStrategyConfig>,
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
		.filter(|r| executor::remote_unsupported(&r.case, strategy).is_none())
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
		.filter(|r| executor::remote_unsupported(&r.case, strategy).is_none())
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
	let ctx = RemoteRunContext {
		run_prefix: run_prefix.clone(),
		baseline: baseline.clone(),
		jobs: num_jobs,
		strategy,
	};

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
	for run in runs.iter().filter(|r| executor::remote_unsupported(&r.case, strategy).is_none()) {
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
			undecidable.entry(param).or_default().insert(path.clone());
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
		schedule_run_remote(run, &mut schedular, &mut provisioner, &ctx, report_send.clone())
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

/// What every test of one remote run shares: how its namespaces are named, the
/// root state the endpoint started with, how many may run at once, and the
/// planner strategy the endpoint holds.
#[cfg(feature = "remote")]
pub struct RemoteRunContext {
	run_prefix: String,
	baseline: Arc<executor::RootSnapshot>,
	jobs: u32,
	strategy: Option<NewPlannerStrategyConfig>,
}

#[cfg(feature = "remote")]
pub async fn schedule_run_remote(
	run: TestRun<TestRunConfig>,
	schedular: &mut Schedular,
	provisioner: &mut provisioner::RemoteProvisioner,
	ctx: &RemoteRunContext,
	report_sender: UnboundedSender<TestReport>,
) {
	let RemoteRunContext {
		run_prefix,
		baseline,
		jobs,
		strategy,
	} = ctx;
	// Skip tests the remote executor can't faithfully reproduce, without taking
	// a connection from the pool.
	if let Some(reason) = executor::remote_unsupported(&run.case, *strategy) {
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
			jobs: *jobs,
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
	const ALLOWED_KEY_PREFIXES: &[&[u8]] = &[b"/!ni", b"/!nh", b"/!nd", b"/!ic", b"/!tl"];

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
	use crate::tests::schema::NewPlannerStrategyConfig;

	#[cfg(feature = "remote")]
	#[test]
	fn a_root_user_statement_is_matched_whatever_its_spelling() {
		for source in [
			"REMOVE USER foo ON ROOT;",
			"DEFINE USER foo ON ROOT PASSWORD 'x' ROLES OWNER;",
			"define user `foo` on root password 'x';",
			"DEFINE   USER\n\tfoo\n\tON ROOT;",
			"INFO FOR USER foo ON ROOT;",
			// The optional clauses the parser accepts between `USER` and the
			// name. Missing these costs the run its login: the first deletes the
			// account every reconnect uses, the second changes its password.
			"REMOVE USER IF EXISTS foo ON ROOT;",
			"DEFINE USER OVERWRITE foo ON ROOT PASSWORD 'x' ROLES OWNER;",
			"DEFINE USER IF NOT EXISTS foo ON ROOT PASSWORD 'x';",
			"remove user if exists `foo` on root;",
			// A comment separates tokens, so the statement can carry no
			// whitespace of its own.
			"REMOVE/*x*/USER/*y*/IF EXISTS/*z*/foo ON ROOT;",
			"REMOVE USER -- note\nfoo ON ROOT;",
			// A name this cannot read as a literal cannot be ruled out, and
			// over-matching only costs a rejected `--username`.
			"REMOVE USER type::string('foo') ON ROOT;",
			// A statement need not be separated from the one before it by
			// whitespace: `;` ends the preceding keyword.
			"DEFINE NAMESPACE zzz;REMOVE USER IF EXISTS foo ON ROOT;",
			// The server decodes escapes, so the source text is not the name.
			r"DEFINE USER OVERWRITE 'fo\u{6f}' ON ROOT PASSWORD 'x';",
			r"REMOVE USER `fo\u{6f}` ON ROOT;",
			// The lexer takes angle brackets around an identifier too.
			"DEFINE USER OVERWRITE ⟨foo⟩ ON ROOT PASSWORD 'x';",
			// `ALTER` reaches an existing account's password without naming a
			// definition, and leaves its name in place for the root snapshot to
			// find unchanged.
			"ALTER USER foo ON ROOT PASSWORD 'x';",
			"ALTER USER IF EXISTS foo ON ROOT PASSWORD 'x';",
			// A comment ends at any of the lexer's five line terminators.
			"REMOVE USER -- note\u{2028}foo ON ROOT;",
			"REMOVE USER // note\u{2029}foo ON ROOT;",
			"REMOVE USER # note\u{0085}foo ON ROOT;",
		] {
			assert!(touches_root_user(source, "foo"), "should have matched: {source}");
		}

		// A different user, and a non-root scope, are not this run's login.
		for source in [
			"REMOVE USER IF EXISTS other ON ROOT;",
			"DEFINE USER OVERWRITE foo ON NAMESPACE PASSWORD 'x';",
			"DEFINE USER foo ON DATABASE PASSWORD 'x';",
		] {
			assert!(!touches_root_user(source, "foo"), "should not have matched: {source}");
		}

		// Granting an access token for a user neither removes the account nor
		// changes its password, and `FOR` there is not `INFO FOR`.
		assert!(!touches_root_user("ACCESS a ON ROOT GRANT FOR USER foo;", "foo"));

		// A name that merely starts with the login is a different account.
		assert!(!touches_root_user("REMOVE USER footer ON ROOT;", "foo"));

		// `user` is an ordinary field name, and the corpus is full of it. Only
		// the statement that introduces an account makes it a keyword.
		for source in [
			"CREATE engagement:1 SET user = 'foo', weight = 2;",
			"SELECT user FROM engagement GROUP BY user;",
			"CREATE t SET user = $root;",
		] {
			assert!(!touches_root_user(source, "foo"), "should not have matched: {source}");
		}
	}

	/// A comment separates tokens for the lexer, so a system change can be
	/// spelled without whitespace and must still be seen.
	#[cfg(feature = "remote")]
	#[test]
	fn a_commented_system_setting_change_is_still_matched() {
		assert!(alters_system_config("ALTER/* now */SYSTEM QUERY_TIMEOUT 2s;"));
		assert!(alters_system_config("ALTER SYSTEM -- note\nCOMPACT;"));
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

		// `NS` lexes to the same keyword as `NAMESPACE`, so it destroys a namespace
		// exactly as readily and has to be seen here.
		assert_eq!(names("REMOVE NS blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("remove ns `blk ns`;"), vec!["blk ns"]);
		assert_eq!(names("REMOVE NS IF EXISTS blk_ns;"), vec!["blk_ns"]);

		// `AND EXPUNGE` precedes `IF EXISTS` in the grammar. Read as the name, it
		// would record `AND` and let the real target through unseen.
		assert_eq!(names("REMOVE NAMESPACE AND EXPUNGE blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("REMOVE NS AND EXPUNGE blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("REMOVE NAMESPACE AND EXPUNGE IF EXISTS blk_ns;"), vec!["blk_ns"]);
		assert_eq!(names("remove ns and expunge if exists `blk ns`;"), vec!["blk ns"]);

		// A parameter is still resolved through the longer spellings.
		assert_eq!(names("LET $n = \"blk_ns\"; REMOVE NS AND EXPUNGE $n;"), vec!["blk_ns"]);
		assert_eq!(
			removed_namespaces("REMOVE NS AND EXPUNGE $n;").unresolved.into_iter().collect::<Vec<_>>(),
			vec!["$n"]
		);

		// The target is a full expression the server coerces to a string, so a
		// call, a concatenation or a quoted name all name a namespace. Only what
		// is provably a literal resolves; the rest refuses the run, because the
		// alternative is waving through a removal of a namespace the check never
		// saw. Each of these destroys its namespace on a real server.
		let computed = |src: &str| {
			let r = removed_namespaces(src);
			assert!(r.names.is_empty(), "{src} should resolve to no literal name");
			r.unresolved.into_iter().collect::<Vec<_>>()
		};
		assert_eq!(
			computed("REMOVE NAMESPACE type::string('customer');"),
			vec!["type::string('customer')"]
		);
		assert_eq!(
			computed("REMOVE NS string::concat('conc', 'atty');"),
			vec!["string::concat('conc', 'atty')"]
		);
		assert_eq!(computed("REMOVE NAMESPACE $a + $b;"), vec!["$a + $b"]);
		assert_eq!(computed("REMOVE NAMESPACE `a` + `b`;"), vec!["`a` + `b`"]);
		assert_eq!(computed("REMOVE NAMESPACE \"a\" + \"b\";"), vec!["\"a\" + \"b\""]);
		// A name carrying its own quote is not unescaped here; refusing is safe.
        assert_eq!(computed("REMOVE NAMESPACE 'it''s';"), vec!["'it''s'"]);

		// A whole string literal is a literal name, and resolves.
		assert_eq!(names("REMOVE NAMESPACE \"quoted\";"), vec!["quoted"]);
		assert_eq!(names("REMOVE NS 'quoted';"), vec!["quoted"]);

		// A comment separates tokens rather than padding them, so a removal can be
		// spelled with no whitespace at all. Each of these removes its namespace
		// on a real server.
		assert_eq!(names("REMOVE/* safety */NAMESPACE customer;"), vec!["customer"]);
		assert_eq!(names("REMOVE NAMESPACE -- note\ndashy;"), vec!["dashy"]);
		assert_eq!(names("REMOVE NAMESPACE # note\nhashy;"), vec!["hashy"]);
		assert_eq!(names("REMOVE NAMESPACE // note\nslashy;"), vec!["slashy"]);
		assert_eq!(names("REMOVE/*a*/NS/*b*/AND/*c*/EXPUNGE/*d*/blk_ns;"), vec!["blk_ns"]);

		// A comment marker inside a literal is data, not a comment: truncating
		// there would read a different name, or none.
		assert_eq!(names("REMOVE NAMESPACE `a -- b`;"), vec!["a -- b"]);
		assert_eq!(names("REMOVE NAMESPACE \"a # b\";"), vec!["a # b"]);

		// The frontmatter every test carries is a comment too, so prose in a
		// `reason` is not read as a statement.
		assert!(
			removed_namespaces("/**\n[test]\nreason = \"REMOVE NAMESPACE prose\"\n*/\nSELECT 1;")
				.names
				.is_empty()
		);

		// Nothing to take: a database, or a namespace definition.
		assert!(removed_namespaces("REMOVE DATABASE blk_db;").names.is_empty());
		assert!(removed_namespaces("REMOVE DATABASE AND EXPUNGE blk_db;").names.is_empty());
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

		// Single quotes, read from the binding ahead of the removal.
		let r = removed_namespaces("LET $ns = 'early'; REMOVE NAMESPACE $ns;");
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["early"]);

		// A binding made only *after* the removal is not the one the statement
		// reads — at that point the parameter is unset and the statement errors,
		// so taking the later literal would name a namespace nothing removes.
		let r = removed_namespaces("REMOVE NAMESPACE $ns; LET $ns = 'late';");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// Reassigned: the removal reads the binding ahead of it, not the last one
		// in the file. Taking the last would preflight this as removing `scratch`
		// and wave the run through against an endpoint holding `customer`, which
		// it would then destroy.
		let r = removed_namespaces(
			"LET $ns = 'customer'; REMOVE NAMESPACE $ns; LET $ns = 'scratch';",
		);
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["customer"]);

		// Two bindings ahead of it — a branch, a loop, or a plain overwrite. Which
		// is live is a question about execution, so the run refuses rather than
		// picks one.
		let r = removed_namespaces(
			"LET $ns = 'a'; LET $ns = 'b'; REMOVE NAMESPACE $ns;",
		);
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// Distinct parameters do not interfere.
		let r = removed_namespaces(
			"LET $a = 'one'; LET $b = 'two'; REMOVE NAMESPACE $a; REMOVE NS $b;",
		);
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["one", "two"]);

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

		// The server decodes escapes, so the source text is not the name: this
		// removal destroys `customer`, which an endpoint may well hold. Neither
		// the target nor a binding carrying one may be read as written.
		let r = removed_namespaces(r"REMOVE NAMESPACE 'cust\u{6f}mer';");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.len(), 1);

		let r = removed_namespaces(r"LET $ns = 'cust\u{6f}mer'; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// A reassignment this cannot read is still a reassignment: the earlier
		// literal is not what the removal reads, and resolving to it destroys a
		// namespace the source never names.
		let r = removed_namespaces(
			"LET $ns = 'scratch'; LET $ns = type::string('customer'); REMOVE NAMESPACE $ns;",
		);
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		let r = removed_namespaces(r"LET $ns = 'scratch'; LET $ns = 'cust\u{6f}mer'; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// A value that merely opens with a literal is not one.
		let r = removed_namespaces("LET $ns = 'cust' + 'omer'; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// A `LET` written inside a string literal is text, not a binding.
		// Counting one resolves the removal to a value the server never reads.
		let r = removed_namespaces("RETURN \"let $ns = 'scratch';\"; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// A real binding beside a phantom one still resolves.
		let r = removed_namespaces(
			"RETURN \"let $ns = 'scratch';\"; LET $ns = 'real'; REMOVE NAMESPACE $ns;",
		);
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["real"]);

		// `DEFINE PARAM` binds a parameter a removal reads, and this scan does
		// not follow it; with no `LET` in scope the removal is unresolved, which
		// refuses the run.
		let r = removed_namespaces("DEFINE PARAM $ns VALUE 'customer'; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// A single-line comment ends at any of the lexer's five terminators, so
		// what follows one of the three Unicode ones is the statement's target,
		// not more comment.
		for (sep, ns) in [("\u{2028}", "ls"), ("\u{2029}", "ps"), ("\u{0085}", "nel")] {
			for comment in ["--", "//", "#"] {
				let source = format!("REMOVE NAMESPACE {comment} note{sep}{ns};");
				let r = removed_namespaces(&source);
				assert_eq!(
					r.names.into_iter().collect::<Vec<_>>(),
					vec![ns.to_owned()],
					"missed the target of: {source:?}"
				);
			}
		}

		// A regex body is one token to the lexer and may hold anything, a `LET`
		// among it. Whether a `/` opens one or divides is a question about the
		// parse, so no parameter resolves while one is present.
		let r = removed_namespaces("RETURN /let $ns = 'scratch';/; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// Division reads the same way and is refused on the same grounds: the
		// alternative, masking the span, could swallow the second assignment
		// and leave the first standing for a value the removal never reads.
		let r = removed_namespaces("LET $ns = 'a'; RETURN 6 / 2; LET $ns = 'b'; REMOVE NAMESPACE $ns;");
		assert!(r.names.is_empty());
		assert_eq!(r.unresolved.into_iter().collect::<Vec<_>>(), vec!["$ns"]);

		// A `/` inside a comment or a string is not one in the source: the
		// comment is gone before this runs and the string is masked.
		let r = removed_namespaces("LET $ns = 'real'; -- see a/b\nRETURN 'x/y'; REMOVE NAMESPACE $ns;");
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["real"]);

		// Angle brackets delimit an identifier for the lexer as backticks do.
		let r = removed_namespaces("REMOVE NAMESPACE \u{27e8}customer\u{27e9};");
		assert_eq!(r.names.into_iter().collect::<Vec<_>>(), vec!["customer"]);
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
		let mut exec_a = EmbeddedExecutor::new(Arc::clone(&dbs), NewPlannerStrategyConfig::ComputeOnly);
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
		let mut exec_b = EmbeddedExecutor::new(Arc::clone(&dbs), NewPlannerStrategyConfig::ComputeOnly);
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
