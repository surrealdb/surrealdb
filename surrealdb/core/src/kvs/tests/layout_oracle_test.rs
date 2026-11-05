//! The layout oracle: schema-change scenarios run over stores that began each way a 4.0 store
//! begins, with the stored rows read raw and compared with what the engine answers.
//!
//! A scenario is a SurrealQL script under `layout_oracle/`, run one unit at a time: a statement, a
//! `BEGIN` … `COMMIT` block, or everything from a `LET` to the next marker. A marker is a line of
//! its own:
//!
//! - `-- @upgrade`: a store that began at the Legacy feature level is raised to the first stored
//!   release here. The other stores ignore it. A scenario without one is upgraded before its first
//!   unit that names `MIGRATE`, or else before its first unit.
//! - `-- @drain`: the table migration job runs until nothing is queued.
//! - `-- @check`: the differential check (below) runs here.
//! - `-- @client`: every statement from here to the next marker runs in one transaction its caller
//!   owns, as an SDK's `begin` … `commit` does, and the caller commits it whatever the statements
//!   answered. A statement that fails before it writes leaves the others to commit; one that fails
//!   after it wrote refuses the commit, so the unit leaves nothing.
//! - `-- @end`: ends the statements a `@client` runs, and is nothing else.
//!
//! Each scenario runs in six modes: three origins — a store this build created, with its database
//! at compatibility 4 or altered to 3, and a store an earlier release wrote, held at the Legacy
//! feature level until `@upgrade`, whose database reads at level 3 — each run lazily (the job runs
//! only at `@drain`) and eagerly (the job also runs after every unit). The job may run at any
//! moment, so no answer may depend on when it ran; and an upgraded store answers, once upgraded, as
//! a store created at the same compatibility level does.
//!
//! After every unit, for every table but a lightweight relation:
//!
//! - every stored row is admitted against the table's ring and decodes, and a row with no
//!   generation sits under no known floor;
//! - a scan of the table, and a fetch of each of its records, answers exactly the stored rows
//!   brought through the steps they have not passed;
//! - every claim that holds for a field's current definition is true of every row it covers, read
//!   as a later step's `$value` sees it and judged against the test a verification pass applies
//!   (`verification_test`);
//! - once the job has run, no row is behind a step.
//!
//! At `@check`, after `@upgrade` and `@drain`, and at the end, every path the rows hold one level
//! deep, and every indexed column, is read through each access path — an index, a scan without
//! one, and a filter over the decoded rows — and the answers must agree. At the end the database is
//! exported and imported into a store this build creates, which must answer the same rows.
//!
//! A finding [`KNOWN`] names is a warning, and one it names that no longer occurs is a failure.
//!
//! The language tests of field migrations run as scenarios too. The scenarios are split into
//! slices, each a test of its own (`layout_oracle_00` …) so the runner runs them in parallel: a
//! scenario is in the slice its position in the list of every scenario names. A slice judges the
//! scenarios it holds alone, so each entry of [`KNOWN`] names exactly one scenario.
//!
//! `LAYOUT_ORACLE_FILTER` runs only the scenarios whose name contains it, `LAYOUT_ORACLE_CORPUS=0`
//! leaves the language tests out, `LAYOUT_ORACLE_REPORT` names a file each slice writes its
//! findings to, with its number appended, `LAYOUT_ORACLE_ANSWERS` a directory each run's answers
//! are written to, and `LAYOUT_ORACLE_DUMP` one each export is.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_types::{ToSql, Value as PublicValue, Variables};
use tokio_util::sync::CancellationToken;

use crate::catalog::TableType;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::corpus_slices::{assert_slices_cover, slice, sliced_tests};
use crate::dbs::Session;
use crate::exec::migrate::{Replay, into_error, migration_frame, verification_test};
use crate::expr::Part;
use crate::key::KVKeyDecode;
use crate::key::schema::{RecordKey, RecordPrefix, StorageVersionKey, VersionKey};
use crate::kvs::version::{MajorVersion, StorageVersion};
use crate::kvs::{Datastore, QueryRequest, Release};
use crate::val::{RecordId, Value, convert_value_to_public_value};

/// The scenarios this suite owns.
const SCENARIOS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/kvs/tests/layout_oracle");

/// The language tests of field migrations, run as scenarios.
const CORPUS: &str = concat!(
	env!("CARGO_MANIFEST_DIR"),
	"/../../language-tests/tests/language/statements/define/field/migrate"
);

/// The root the language tests' imports are named from.
const CORPUS_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../language-tests/tests");

/// How many of a table's records are fetched one by one.
const FETCHED: usize = 25;

/// How many paths of a table the differential check reads.
const PATHS: usize = 12;

/// How many values of a path the differential check filters on.
const VALUES: usize = 3;

/// Fragments of an error message that no schema change may cause.
const SUSPECT: &[&str] = &[
	"nternal",
	"unreachable",
	"panick",
	"poison",
	"corrupt",
	"decode",
	"not admitted",
	"outside the ring",
];

/// A finding of known behaviour, a defect outside field migrations or an open loss of precision in
/// them: one of `kind`, in a scenario whose name contains `scenario`, whose detail contains
/// `detail`.
struct Known {
	kind: &'static str,
	scenario: &'static str,
	detail: &'static str,
	/// The behaviour the finding is of.
	reason: &'static str,
}

const UNDECLARED_KEPT: &str = "a table made SCHEMAFULL at level 4 keeps the fields its rows hold \
                               that it does not declare, which its check for undeclared keys \
                               reports, an export carries and an import refuses";

const VIEW_EXPORT_ORDER: &str = "an export writes a view before a table its subquery reads, and an \
                                 import refuses the view";

const VIEW_IMPORT_WRITES: &str = "an `OPTION IMPORT` write skips the maintenance of the views over \
                                  its table, and an import builds each view from every source row";

const CLAIM_FORFEIT: &str = "a change to a field forfeits the claims of every field of its table \
                             and verifies only the changed field again, so a claim the job settled \
                             before the change is lost, and a cast that relies on it is accepted or \
                             refused by when the job ran";

/// The known findings ([`Known`]). Each must still occur in every run that includes its scenario,
/// so the list holds nothing that has been fixed.
const KNOWN: &[Known] = &[
	Known {
		kind: "import-failed",
		scenario: "lt/interaction_schema_mode",
		detail: "but no such field exists",
		reason: UNDECLARED_KEPT,
	},
	Known {
		kind: "import-failed",
		scenario: "lt/interaction_table_type_and_view",
		detail: "but no such field exists",
		reason: UNDECLARED_KEPT,
	},
	Known {
		kind: "import-failed",
		scenario: "lt/interaction_undeclared_keys",
		detail: "but no such field exists",
		reason: UNDECLARED_KEPT,
	},
	Known {
		kind: "import-failed",
		scenario: "vw_view_convert",
		detail: "but no such field exists",
		reason: UNDECLARED_KEPT,
	},
	Known {
		kind: "import-failed",
		scenario: "vw_multi_source_refs",
		detail: "does not exist",
		reason: VIEW_EXPORT_ORDER,
	},
	Known {
		kind: "round-trip-differs",
		scenario: "ie_view_index_import_writes",
		detail: "vw: ",
		reason: VIEW_IMPORT_WRITES,
	},
	Known {
		kind: "modes-differ",
		scenario: "ft_numeric_retypes",
		detail: "AS w_float",
		reason: CLAIM_FORFEIT,
	},
	Known {
		kind: "modes-differ",
		scenario: "ft_numeric_retypes",
		detail: "AS gl_wf",
		reason: CLAIM_FORFEIT,
	},
	Known {
		kind: "modes-differ",
		scenario: "ft_numeric_retypes",
		detail: "SELECT VALUE w FROM",
		reason: CLAIM_FORFEIT,
	},
	Known {
		kind: "contents-differ",
		scenario: "ft_numeric_retypes",
		detail: "gl\n",
		reason: CLAIM_FORFEIT,
	},
];

impl Known {
	fn matches(&self, scenario: &str, finding: &Finding) -> bool {
		scenario.contains(self.scenario)
			&& finding.kind == self.kind
			&& finding.detail.contains(self.detail)
	}
}

// ------------------------------------------------------------------------------------------------
// Scenarios
// ------------------------------------------------------------------------------------------------

/// One step of a scenario.
#[derive(Clone, Debug)]
enum Step {
	/// Statements run in one request.
	Unit(String),
	/// Statements run in one request, in a transaction the caller owns and commits.
	Client(String),
	/// Raise a Legacy store to the first stored release.
	Upgrade,
	/// Run the table migration job until nothing is queued.
	Drain,
	/// Run the differential check.
	Check,
}

#[derive(Clone, Debug)]
struct Scenario {
	name: String,
	steps: Vec<Step>,
}

/// The statements of `text`, split at each `;` outside brackets, strings and comments, with the
/// comments left out.
fn statements(text: &str) -> Vec<String> {
	let mut out = Vec::new();
	let mut current = String::new();
	let mut depth = 0i32;
	let mut chars = text.chars().peekable();
	let flush = |current: &mut String, out: &mut Vec<String>| {
		let trimmed = current.trim();
		if !trimmed.is_empty() {
			out.push(trimmed.to_owned());
		}
		current.clear();
	};
	while let Some(c) = chars.next() {
		match c {
			'\'' | '"' | '`' => {
				current.push(c);
				while let Some(d) = chars.next() {
					current.push(d);
					if d == '\\' {
						if let Some(e) = chars.next() {
							current.push(e);
						}
					} else if d == c {
						break;
					}
				}
			}
			'⟨' => {
				current.push(c);
				for d in chars.by_ref() {
					current.push(d);
					if d == '⟩' {
						break;
					}
				}
			}
			'-' if chars.peek() == Some(&'-') => {
				for d in chars.by_ref() {
					if d == '\n' {
						break;
					}
				}
				current.push('\n');
			}
			'/' if chars.peek() == Some(&'/') => {
				for d in chars.by_ref() {
					if d == '\n' {
						break;
					}
				}
				current.push('\n');
			}
			'/' if chars.peek() == Some(&'*') => {
				chars.next();
				let mut last = ' ';
				for d in chars.by_ref() {
					if last == '*' && d == '/' {
						break;
					}
					last = d;
				}
				current.push(' ');
			}
			'(' | '{' | '[' => {
				depth += 1;
				current.push(c);
			}
			')' | '}' | ']' => {
				depth -= 1;
				current.push(c);
			}
			';' if depth <= 0 => flush(&mut current, &mut out),
			_ => current.push(c),
		}
	}
	flush(&mut current, &mut out);
	out
}

/// The first keyword of a statement, upper-cased.
fn keyword(statement: &str) -> String {
	statement.split_whitespace().next().unwrap_or_default().to_ascii_uppercase()
}

/// The units of one segment's statements: a `BEGIN` … `COMMIT`/`CANCEL` block is one unit, and so
/// is everything from a `LET` to the end of the segment, which reads what the `LET` bound.
fn units(statements: Vec<String>) -> Vec<String> {
	let mut out = Vec::new();
	let mut block: Option<Vec<String>> = None;
	let mut iter = statements.into_iter();
	while let Some(statement) = iter.next() {
		let word = keyword(&statement);
		if let Some(open) = block.as_mut() {
			open.push(statement);
			if word == "COMMIT" || word == "CANCEL" {
				out.push(format!("{};", block.take().unwrap().join(";\n")));
			}
			continue;
		}
		match word.as_str() {
			"BEGIN" => block = Some(vec![statement]),
			"LET" => {
				let mut rest = vec![statement];
				rest.extend(iter.by_ref());
				out.push(format!("{};", rest.join(";\n")));
			}
			_ => out.push(format!("{statement};")),
		}
	}
	if let Some(open) = block {
		out.push(format!("{};", open.join(";\n")));
	}
	out
}

/// The scenario `name` holds `text`.
fn parse(name: &str, text: &str) -> Scenario {
	let mut steps = Vec::new();
	let mut segment = String::new();
	let mut upgraded = false;
	let mut client = false;
	let flush = |segment: &mut String, steps: &mut Vec<Step>, client: &mut bool| {
		let statements = statements(segment);
		if std::mem::take(client) {
			if !statements.is_empty() {
				steps.push(Step::Client(format!("{};", statements.join(";\n"))));
			}
		} else {
			steps.extend(units(statements).into_iter().map(Step::Unit));
		}
		segment.clear();
	};
	for line in text.lines() {
		let trimmed = line.trim();
		let marker = trimmed.strip_prefix("--").map(str::trim).and_then(|m| m.strip_prefix('@'));
		match marker.map(|m| m.split_whitespace().next().unwrap_or_default()) {
			Some("client") => {
				flush(&mut segment, &mut steps, &mut client);
				client = true;
			}
			Some("end") => flush(&mut segment, &mut steps, &mut client),
			Some(marker @ ("upgrade" | "drain" | "check")) => {
				flush(&mut segment, &mut steps, &mut client);
				steps.push(match marker {
					"upgrade" => {
						upgraded = true;
						Step::Upgrade
					}
					"drain" => Step::Drain,
					_ => Step::Check,
				});
			}
			_ => {
				segment.push_str(line);
				segment.push('\n');
			}
		}
	}
	flush(&mut segment, &mut steps, &mut client);
	if !upgraded {
		let at = steps
			.iter()
			.position(|step| matches!(step, Step::Unit(text) | Step::Client(text) if text.to_ascii_uppercase().contains("MIGRATE")))
			.unwrap_or(0);
		steps.insert(at, Step::Upgrade);
	}
	Scenario {
		name: name.to_owned(),
		steps,
	}
}

/// The text of a language test with the files its header imports before it, and without the
/// header.
fn with_imports(text: &str) -> String {
	let mut out = String::new();
	if let Some(header) = text.strip_prefix("/**").and_then(|rest| rest.split("*/").next()) {
		for line in header.lines() {
			let Some(list) = line.trim().strip_prefix("imports") else {
				continue;
			};
			for import in list.split('"').skip(1).step_by(2) {
				let path = Path::new(CORPUS_ROOT).join(import);
				if let Ok(imported) = std::fs::read_to_string(&path) {
					out.push_str(&imported);
					out.push('\n');
				}
			}
		}
	}
	out.push_str(text);
	out
}

/// Every scenario the environment's filter admits: this suite's own, then the language tests.
fn scenarios() -> Vec<Scenario> {
	let filter = std::env::var("LAYOUT_ORACLE_FILTER").unwrap_or_default();
	let corpus = std::env::var("LAYOUT_ORACLE_CORPUS").map_or(true, |v| v != "0");
	scenarios_matching(&filter, corpus)
}

/// Every scenario whose name contains `filter`: this suite's own, then the language tests when
/// `corpus` is set.
fn scenarios_matching(filter: &str, corpus: bool) -> Vec<Scenario> {
	let mut out = Vec::new();
	let mut read = |dir: &str, prefix: &str, imports: bool| {
		let Ok(entries) = std::fs::read_dir(dir) else {
			return;
		};
		let mut paths: Vec<PathBuf> = entries
			.filter_map(|entry| entry.ok().map(|entry| entry.path()))
			.filter(|path| path.extension().is_some_and(|ext| ext == "surql"))
			.collect();
		paths.sort();
		for path in paths {
			let stem = path.file_stem().unwrap().to_string_lossy();
			let name = format!("{prefix}{stem}");
			if !name.contains(filter) {
				continue;
			}
			let text = std::fs::read_to_string(&path).unwrap();
			let text = if imports {
				with_imports(&text)
			} else {
				text
			};
			out.push(parse(&name, &text));
		}
	};
	read(SCENARIOS, "", false);
	if corpus {
		read(CORPUS, "lt/", true);
	}
	out
}

// ------------------------------------------------------------------------------------------------
// Modes and stores
// ------------------------------------------------------------------------------------------------

/// How a scenario's store began.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Origin {
	/// Created by this build; its database at compatibility 4.
	Fresh4,
	/// Created by this build; its database altered to compatibility 3.
	Fresh3,
	/// Written by an earlier release and held at the Legacy feature level until `@upgrade`.
	Upgraded,
}

#[derive(Clone, Copy, Debug)]
struct Mode {
	origin: Origin,
	/// Whether the job also runs after every unit.
	eager: bool,
}

impl Mode {
	const ALL: [Mode; 6] = [
		Mode {
			origin: Origin::Fresh4,
			eager: false,
		},
		Mode {
			origin: Origin::Fresh4,
			eager: true,
		},
		Mode {
			origin: Origin::Fresh3,
			eager: false,
		},
		Mode {
			origin: Origin::Fresh3,
			eager: true,
		},
		Mode {
			origin: Origin::Upgraded,
			eager: false,
		},
		Mode {
			origin: Origin::Upgraded,
			eager: true,
		},
	];

	fn label(self) -> String {
		let origin = match self.origin {
			Origin::Fresh4 => "fresh4",
			Origin::Fresh3 => "fresh3",
			Origin::Upgraded => "upgraded",
		};
		if self.eager {
			format!("{origin}+eager")
		} else {
			origin.to_owned()
		}
	}
}

/// A database a scenario's statements run in: `test`/`test` until a `USE` names another.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Place {
	ns: String,
	db: String,
}

impl Place {
	fn test() -> Self {
		Self {
			ns: "test".to_owned(),
			db: "test".to_owned(),
		}
	}

	fn session(&self) -> Session {
		Session::owner().with_ns(&self.ns).with_db(&self.db)
	}

	/// Where the `USE` statement `text` leaves a session that was here.
	fn after_use(&self, text: &str) -> Self {
		let mut next = self.clone();
		let words: Vec<String> = text
			.trim_end_matches(';')
			.split_whitespace()
			.skip(1)
			.map(|word| word.trim_matches(|c| c == '`' || c == '⟨' || c == '⟩').to_owned())
			.collect();
		for pair in words.chunks(2) {
			if let [kind, name] = pair {
				match kind.to_ascii_uppercase().as_str() {
					"NS" | "NAMESPACE" => next.ns = name.clone(),
					"DB" | "DATABASE" => next.db = name.clone(),
					_ => {}
				}
			}
		}
		next
	}

	/// A prefix naming the place in a finding, empty for `test`/`test`.
	fn label(&self) -> String {
		if *self == Self::test() {
			String::new()
		} else {
			format!("{}/{}: ", self.ns, self.db)
		}
	}
}

fn session() -> Session {
	Place::test().session()
}

/// A store this build creates, its database at `level`.
async fn fresh(level: u8) -> Arc<Datastore> {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	if level == 3 {
		for response in
			ds.execute("ALTER DATABASE test COMPATIBILITY 3;", &session(), None).await.unwrap()
		{
			response.result.unwrap();
		}
	}
	ds
}

/// A store an earlier release wrote, opened by this build with the opt-in to upgrade it, and held
/// at the Legacy feature level.
async fn upgraded() -> Arc<Datastore> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.without_startup_transactions()
		.with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("experimental_upgrade_from_3x", "true"),
		)
		.with_legacy_feature_level()
		.build_with_path("memory")
		.await
		.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
	txn.set_key(&StorageVersionKey {}, &StorageVersion::parse("3.3.0").unwrap()).await.unwrap();
	txn.commit().await.unwrap();
	ds.check_version().await.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	assert!(ds.feature_level().await.unwrap().level.is_legacy(), "the store left the Legacy level");
	ds
}

async fn store(origin: Origin) -> Arc<Datastore> {
	match origin {
		Origin::Fresh4 => fresh(4).await,
		Origin::Fresh3 => fresh(3).await,
		Origin::Upgraded => upgraded().await,
	}
}

/// Run the table migration job until nothing is queued.
async fn drain(ds: &Arc<Datastore>) -> Result<(), String> {
	Datastore::table_migration(Arc::clone(ds), Duration::from_secs(2), CancellationToken::new())
		.await
		.map_err(|e| format!("{e:#}"))
}

/// Whether every transaction opened through `ds` so far has ended, waiting up to a second for
/// them. The table migration job's verification waits for exactly this before it judges a row.
async fn transactions_ended(ds: &Datastore) -> bool {
	let live = ds.commit_triggers().live_transactions();
	let deadline = web_time::Instant::now() + Duration::from_secs(1);
	let bucket = loop {
		if let Some(bucket) = live.advance() {
			break bucket;
		}
		if web_time::Instant::now() >= deadline {
			return false;
		}
		tokio::time::sleep(Duration::from_millis(5)).await;
	};
	while !live.drained(bucket) {
		if web_time::Instant::now() >= deadline {
			return false;
		}
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
	true
}

/// The answers of `sql`, one per statement, as text: a value's debug form, or `ERR` and the
/// message.
async fn answers(
	ds: &Datastore,
	place: &Place,
	sql: &str,
	vars: Option<Variables>,
) -> Vec<Result<PublicValue, String>> {
	match ds.execute(sql, &place.session(), vars).await {
		Ok(responses) => responses
			.into_iter()
			.map(|response| response.result.map_err(|e| e.to_string()))
			.collect(),
		Err(e) => vec![Err(e.to_string())],
	}
}

/// The answers of `sql` run in one transaction its caller owns, then the answer of the caller's
/// commit, which follows whatever the statements answered.
async fn client_answers(
	ds: &Datastore,
	place: &Place,
	sql: &str,
) -> Vec<Result<PublicValue, String>> {
	let txn = match ds.transaction(Write).await {
		Ok(txn) => Arc::new(txn),
		Err(e) => return vec![Err(e.to_string())],
	};
	let session = place.session();
	let request = QueryRequest::new(sql, &session).with_transaction(Arc::clone(&txn));
	let mut out: Vec<Result<PublicValue, String>> = match ds.run(request).await {
		Ok(responses) => responses
			.into_iter()
			.map(|response| response.result.map_err(|e| e.to_string()))
			.collect(),
		Err(e) => vec![Err(e.to_string())],
	};
	out.push(txn.commit().await.map(|()| PublicValue::None).map_err(|e| e.to_string()));
	out
}

fn render(answer: &Result<PublicValue, String>) -> String {
	match answer {
		Ok(value) => format!("{value:?}"),
		Err(message) => format!("ERR {message}"),
	}
}

/// `text` with what differs between two runs of the same statements masked: the clock, random
/// record ids and UUIDs.
fn masked(text: &str) -> String {
	static MASKS: std::sync::LazyLock<[(regex::Regex, &str); 3]> = std::sync::LazyLock::new(|| {
		[
			(regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z").unwrap(), "<time>"),
			(
				regex::Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
					.unwrap(),
				"<uuid>",
			),
			(regex::Regex::new(r#"String\("[0-9a-z]{20}"\)"#).unwrap(), "String(<rand>)"),
		]
	});
	let mut out = text.to_owned();
	for (pattern, mask) in MASKS.iter() {
		out = pattern.replace_all(&out, *mask).into_owned();
	}
	out
}

// ------------------------------------------------------------------------------------------------
// Findings
// ------------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Finding {
	/// What kind of property failed.
	kind: &'static str,
	/// Where in the scenario.
	at: String,
	detail: String,
	/// A warning is reported and fails nothing.
	warning: bool,
}

#[derive(Default)]
struct Findings(Vec<Finding>);

impl Findings {
	fn fail(&mut self, kind: &'static str, at: &str, detail: impl Into<String>) {
		self.0.push(Finding {
			kind,
			at: at.to_owned(),
			detail: detail.into(),
			warning: false,
		});
	}

	fn warn(&mut self, kind: &'static str, at: &str, detail: impl Into<String>) {
		self.0.push(Finding {
			kind,
			at: at.to_owned(),
			detail: detail.into(),
			warning: true,
		});
	}
}

/// `text` cut to `max` characters.
fn cut(text: &str, max: usize) -> String {
	if text.chars().count() <= max {
		text.to_owned()
	} else {
		format!("{}…", text.chars().take(max).collect::<String>())
	}
}

// ------------------------------------------------------------------------------------------------
// What is stored
// ------------------------------------------------------------------------------------------------

/// A path of names into a row.
type FieldPath = Vec<String>;

/// The SurrealQL spelling of `path`.
fn path_sql(path: &[String]) -> String {
	path.iter()
		.map(|name| {
			let plain = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
				&& name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
			if plain {
				name.clone()
			} else {
				format!("`{}`", name.replace('`', "\\`"))
			}
		})
		.collect::<Vec<_>>()
		.join(".")
}

/// The names of `parts`, or `None` when a part is not a name.
fn names(parts: &[Part]) -> Option<FieldPath> {
	parts
		.iter()
		.map(|part| {
			if let Part::Field(name) = part {
				Some(name.as_str().to_owned())
			} else {
				None
			}
		})
		.collect()
}

fn at_path<'a>(value: &'a PublicValue, path: &[String]) -> Option<&'a PublicValue> {
	let mut value = value;
	for name in path {
		if let PublicValue::Object(object) = value {
			value = object.get(name.as_str())?;
		} else {
			return None;
		}
	}
	Some(value)
}

fn remove_path(value: &mut PublicValue, path: &[String]) {
	let Some((last, parents)) = path.split_last() else {
		return;
	};
	let mut value = value;
	for name in parents {
		if let PublicValue::Object(object) = value {
			match object.get_mut(name.as_str()) {
				Some(child) => value = child,
				None => return,
			}
		} else {
			return;
		}
	}
	if let PublicValue::Object(object) = value {
		object.remove(last.as_str());
	}
}

/// A stored row as the oracle reads it.
struct Row {
	/// The record id, as SurrealQL.
	id: String,
	/// The row brought through the steps it has not passed.
	read: PublicValue,
}

/// A table as the oracle reads it.
struct TableState {
	/// The table's name, as SurrealQL.
	name: String,
	rows: Vec<Row>,
	/// The paths of computed fields and named record id components, which no row stores.
	computed: Vec<FieldPath>,
	/// The paths of indexed columns.
	indexed: Vec<FieldPath>,
	/// How many rows are behind a step.
	behind: usize,
}

/// Every database of the store.
async fn places(ds: &Datastore) -> Vec<Place> {
	let txn = ds.transaction(Read).await.unwrap();
	let mut out = Vec::new();
	for ns in txn.all_ns(None).await.unwrap().iter() {
		for db in txn.all_db(ns.namespace_id, None).await.unwrap().iter() {
			out.push(Place {
				ns: ns.name.to_string(),
				db: db.name.to_string(),
			});
		}
	}
	txn.cancel().await.unwrap();
	out.sort();
	out
}

/// Read every table of every database raw, judging each row's admission, decoding, replay and
/// claims.
async fn stored(
	ds: &Arc<Datastore>,
	at: &str,
	findings: &mut Findings,
) -> Vec<(Place, Vec<TableState>)> {
	let mut out = Vec::new();
	for place in places(ds).await {
		let at = format!("{at} {}", place.label());
		let tables = stored_in(ds, &place, at.trim_end(), findings).await;
		out.push((place, tables));
	}
	out
}

/// [`stored`], for the database `place`.
async fn stored_in(
	ds: &Arc<Datastore>,
	place: &Place,
	at: &str,
	findings: &mut Findings,
) -> Vec<TableState> {
	let txn = Arc::new(ds.transaction(Read).await.unwrap());
	let Some(db) = txn.get_db_by_name(&place.ns, &place.db, None).await.unwrap() else {
		txn.cancel().await.unwrap();
		return Vec::new();
	};
	let (ns, db) = (db.namespace_id, db.database_id);
	let exec = ds.privileged_database_context(&txn, &place.ns, &place.db).await.unwrap();
	let tables = txn.all_tb(ns, db, None).await.unwrap();
	let mut out = Vec::new();
	for tb in tables.iter() {
		if matches!(&tb.table_type, TableType::Relation(relation) if relation.lightweight) {
			continue;
		}
		let name = path_sql(&[tb.name.as_str().to_owned()]);
		let layouts = txn.table_layouts(ns, db, &tb.name, None).await.unwrap();
		let ring = layouts.ring();
		let replay = Replay::of(&layouts, ns, db);
		let fields = txn.all_tb_fields(ns, db, &tb.name, None).await.unwrap();
		let indexes = txn.all_tb_indexes(ns, db, &tb.name, None).await.unwrap();
		let conformance = txn.table_conformance(ns, db, &tb.name, None).await.unwrap();
		// The claims that hold for the current definitions, with the definition each holds for
		// and the test a value at the field passes when the claim is true.
		let held: Vec<_> = conformance
			.iter()
			.flat_map(|conformance| conformance.claims().iter())
			.filter_map(|claim| {
				let fd = fields.iter().find(|fd| fd.name.to_raw_string() == claim.field)?;
				claim.holds_for(&fd.to_stored()).then_some(fd)
			})
			.collect();
		let mut claims = Vec::with_capacity(held.len());
		for fd in held {
			if fd.field_kind.is_some() {
				let (test, _) = verification_test(&exec, &fields, fd, tb.schemafull)
					.await
					.map_err(into_error)
					.unwrap();
				claims.push((fd, test));
			}
		}
		let frame = migration_frame(&exec);
		let computed: Vec<FieldPath> = fields
			.iter()
			.filter(|fd| fd.computed.is_some() || fd.key_part.is_some())
			.filter_map(|fd| names(&fd.name.0))
			.collect();
		let range = RecordPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&tb.name),
		}
		.range()
		.unwrap();
		let raw = txn.scan_raw(range, u32::MAX, 0, None).await.unwrap();
		let mut rows = Vec::with_capacity(raw.len());
		let mut behind = 0;
		for (key, value) in raw {
			let decoded = match RecordKey::decode_key(&key) {
				Ok(decoded) => decoded,
				Err(e) => {
					findings.fail("undecodable-key", at, format!("{name}: {e:#}"));
					continue;
				}
			};
			let rid = RecordId {
				table: decoded.tb.into_owned(),
				key: decoded.id.into_owned(),
			};
			let id = convert_value_to_public_value(Value::RecordId(rid.clone()))
				.map(|id| id.to_sql())
				.unwrap_or_else(|_| rid.to_sql());
			let (record, stamp) = match txn
				.decode_stamped_record(ns, db, &tb.name, &value, rid.clone(), None)
				.await
			{
				Ok(decoded) => decoded,
				Err(e) => {
					findings.fail("undecodable-row", at, format!("{id}: {e:#}"));
					continue;
				}
			};
			match stamp {
				Some(generation) if !ring.contains(generation) => findings.fail(
					"stamp-outside-ring",
					at,
					format!("{id} is stamped {generation}, outside {ring:?}"),
				),
				None if ring.floor().is_some() => findings.fail(
					"unstamped-under-floor",
					at,
					format!("{id} has no generation, under the floor of {ring:?}"),
				),
				_ => {}
			}
			let mut data = record.data.clone();
			if let Some(replay) = replay.as_ref().filter(|replay| replay.behind(stamp)) {
				behind += 1;
				if let Err(e) = replay.apply(&exec, &mut data, stamp).await.map_err(into_error) {
					findings.fail("replay-failed", at, format!("{id} (stamp {stamp:?}): {e:#}"));
					continue;
				}
			}
			for (fd, test) in &claims {
				let field = fd.name.to_raw_string();
				let mut values = Vec::new();
				data.walk_ref(&fd.name.0, &mut |value: &Value| values.push(value.clone()));
				for value in values {
					if test.conforms(&frame, &value).await.map_err(into_error).unwrap() {
						continue;
					}
					let detail = format!(
						"{id} (stamp {stamp:?}) holds {} at `{field}`, claimed {}",
						cut(&value.to_sql(), 120),
						fd.field_kind.as_ref().map(|kind| kind.to_sql()).unwrap_or_default()
					);
					if matches!(value, Value::None) {
						findings.warn("claim-absent", at, detail);
					} else if stamp.is_none() {
						findings.fail("claim-revision-row", at, detail);
					} else {
						findings.fail("claim-false", at, detail);
					}
				}
			}
			match convert_value_to_public_value(data) {
				Ok(read) => rows.push(Row {
					id,
					read: stripped(read, &computed),
				}),
				Err(e) => findings.fail("unconvertible-row", at, format!("{id}: {e:#}")),
			}
		}
		let indexed = indexes
			.iter()
			.flat_map(|ix| ix.cols.iter())
			.filter_map(|col| names(&col.0))
			.filter(|path| !path.is_empty())
			.collect();
		out.push(TableState {
			name,
			rows,
			computed,
			indexed,
			behind,
		});
	}
	txn.cancel().await.unwrap();
	out
}

/// `value` without the paths no row stores.
fn stripped(mut value: PublicValue, computed: &[FieldPath]) -> PublicValue {
	for path in computed {
		remove_path(&mut value, path);
	}
	value
}

/// The id of a row the engine answered, as SurrealQL.
fn id_of(value: &PublicValue) -> String {
	if let PublicValue::Object(object) = value {
		object.get("id").map(|id| id.to_sql()).unwrap_or_else(|| "<no id>".to_owned())
	} else {
		format!("<not an object: {}>", cut(&value.to_sql(), 60))
	}
}

/// Judge what the engine answers for each table's rows against the oracle's reading of them.
async fn judge_reads(
	ds: &Datastore,
	place: &Place,
	tables: &[TableState],
	at: &str,
	findings: &mut Findings,
) {
	for table in tables {
		let oracle: BTreeMap<&str, &PublicValue> =
			table.rows.iter().map(|row| (row.id.as_str(), &row.read)).collect();
		let scan =
			answers(ds, place, &format!("SELECT * FROM {} ORDER BY id;", table.name), None).await;
		match scan.first() {
			Some(Ok(PublicValue::Array(rows))) => {
				let engine: BTreeMap<String, PublicValue> = rows
					.iter()
					.map(|row| (id_of(row), stripped(row.clone(), &table.computed)))
					.collect();
				for (id, read) in &oracle {
					match engine.get(*id) {
						None => findings.fail(
							"scan-missing-row",
							at,
							format!("{}: the scan answers no {id}", table.name),
						),
						Some(answered) if answered != *read => findings.fail(
							"scan-differs",
							at,
							format!(
								"{id}: the scan answers {}, the stored row reads {}",
								cut(&answered.to_sql(), 300),
								cut(&read.to_sql(), 300)
							),
						),
						Some(_) => {}
					}
				}
				for id in engine.keys() {
					if !oracle.contains_key(id.as_str()) {
						findings.fail(
							"scan-extra-row",
							at,
							format!("{}: the scan answers {id}, which no row stores", table.name),
						);
					}
				}
			}
			other => findings.fail(
				"scan-failed",
				at,
				format!("{}: {}", table.name, other.map(render).unwrap_or_default()),
			),
		}
		// A fetch of each record, by a literal id and through a link.
		let fetched: Vec<&Row> = table.rows.iter().take(FETCHED).collect();
		if fetched.is_empty() {
			continue;
		}
		let mut sql = String::new();
		for row in &fetched {
			let _ = writeln!(sql, "SELECT * FROM {};", row.id);
			let _ = writeln!(sql, "RETURN {}.*;", row.id);
		}
		let answered = answers(ds, place, &sql, None).await;
		for (i, row) in fetched.iter().enumerate() {
			let by_select = answered.get(2 * i).cloned();
			let by_link = answered.get(2 * i + 1).cloned();
			let by_select = match by_select {
				Some(Ok(PublicValue::Array(rows))) => rows.first().cloned().map(Ok),
				other => other,
			};
			for (how, answer) in [("SELECT FROM", by_select), ("a link", by_link)] {
				match answer {
					Some(Ok(value)) => {
						let value = stripped(value, &table.computed);
						if value != row.read {
							findings.fail(
								"fetch-differs",
								at,
								format!(
									"{} fetched by {how} reads {}, the stored row reads {}",
									row.id,
									cut(&value.to_sql(), 300),
									cut(&row.read.to_sql(), 300)
								),
							);
						}
					}
					other => findings.fail(
						"fetch-failed",
						at,
						format!(
							"{} fetched by {how}: {}",
							row.id,
							other.as_ref().map(render).unwrap_or_else(|| "no answer".to_owned())
						),
					),
				}
			}
		}
	}
}

/// The paths of `table` the differential check reads: indexed columns first, then the paths its
/// rows hold one level deep, without computed ones.
fn paths(table: &TableState) -> Vec<FieldPath> {
	let mut out: Vec<FieldPath> = Vec::new();
	let push = |path: FieldPath, out: &mut Vec<FieldPath>| {
		if !table.computed.contains(&path)
			&& !out.contains(&path)
			&& path.first().is_some_and(|p| p != "id")
		{
			out.push(path);
		}
	};
	for path in &table.indexed {
		push(path.clone(), &mut out);
	}
	for row in &table.rows {
		let PublicValue::Object(object) = &row.read else {
			continue;
		};
		for (name, value) in object.iter() {
			push(vec![name.clone()], &mut out);
			if let PublicValue::Object(inner) = value {
				for child in inner.keys() {
					push(vec![name.clone(), child.clone()], &mut out);
				}
			}
		}
	}
	out.truncate(PATHS);
	out
}

/// Whether every answer in `group` is the same, reporting the group when not.
fn agree(
	group: &[(&str, &Result<PublicValue, String>)],
	kind: &'static str,
	at: &str,
	findings: &mut Findings,
) {
	let Some((_, first)) = group.first() else {
		return;
	};
	if group.iter().all(|(_, answer)| answer == first) {
		return;
	}
	let detail = group
		.iter()
		.map(|(query, answer)| format!("\n    {query}\n      => {}", cut(&render(answer), 300)))
		.collect::<String>();
	findings.fail(kind, at, detail);
}

/// Read every path of every table through each access path, judging that the answers agree.
async fn judge_paths(
	ds: &Datastore,
	place: &Place,
	tables: &[TableState],
	at: &str,
	findings: &mut Findings,
) {
	for table in tables {
		let tb = &table.name;
		let count = format!("(SELECT count() FROM {tb} GROUP ALL)[0].count ?? 0;");
		let count_noindex =
			format!("(SELECT count() FROM {tb} WITH NOINDEX GROUP ALL)[0].count ?? 0;");
		let counted = answers(ds, place, &format!("{count}\n{count_noindex}"), None).await;
		let expected =
			Ok(PublicValue::Number(surrealdb_types::Number::Int(table.rows.len() as i64)));
		if let (Some(a), Some(b)) = (counted.first(), counted.get(1)) {
			agree(
				&[("stored rows", &expected), (&count, a), (&count_noindex, b)],
				"count-differs",
				at,
				findings,
			);
		}
		for path in paths(table) {
			let p = path_sql(&path);
			let queries = [
				format!("SELECT VALUE id FROM {tb} ORDER BY {p}, id;"),
				format!("SELECT VALUE id FROM {tb} WITH NOINDEX ORDER BY {p}, id;"),
				format!("SELECT VALUE {p} FROM {tb} ORDER BY id;"),
				format!("(SELECT * FROM {tb} ORDER BY id).map(|$r| $r.{p});"),
			];
			let answered = answers(ds, place, &queries.join("\n"), None).await;
			if answered.len() == queries.len() {
				agree(
					&[(&queries[0], &answered[0]), (&queries[1], &answered[1])],
					"order-differs",
					at,
					findings,
				);
				agree(
					&[(&queries[2], &answered[2]), (&queries[3], &answered[3])],
					"projection-differs",
					at,
					findings,
				);
			}
			let mut values: Vec<&PublicValue> = Vec::new();
			for row in &table.rows {
				if let Some(value) = at_path(&row.read, &path)
					&& !matches!(value, PublicValue::None)
					&& !values.contains(&value)
				{
					values.push(value);
				}
				if values.len() == VALUES {
					break;
				}
			}
			for value in values {
				let lit = value.to_sql();
				let queries = [
					format!("SELECT VALUE id FROM {tb} WHERE {p} = {lit} ORDER BY id;"),
					format!(
						"SELECT VALUE id FROM {tb} WITH NOINDEX WHERE {p} = {lit} ORDER BY id;"
					),
					format!("SELECT VALUE id FROM {tb} WHERE {p} = $v ORDER BY id;"),
					format!("SELECT VALUE id FROM {tb} WITH NOINDEX WHERE {p} = $v ORDER BY id;"),
					format!(
						"(SELECT * FROM {tb} ORDER BY id).filter(|$r| $r.{p} = {lit}).map(|$r| $r.id);"
					),
					format!("SELECT VALUE id FROM {tb} WHERE {p} > {lit} ORDER BY id;"),
					format!(
						"SELECT VALUE id FROM {tb} WITH NOINDEX WHERE {p} > {lit} ORDER BY id;"
					),
					format!(
						"(SELECT * FROM {tb} ORDER BY id).filter(|$r| $r.{p} > {lit}).map(|$r| $r.id);"
					),
					format!(
						"(SELECT count() FROM {tb} WHERE {p} = {lit} GROUP ALL)[0].count ?? 0;"
					),
					format!("((SELECT * FROM {tb}).filter(|$r| $r.{p} = {lit})).len();"),
				];
				let mut vars = Variables::new();
				vars.insert("v", value.clone());
				let answered = answers(ds, place, &queries.join("\n"), Some(vars)).await;
				if answered.len() != queries.len() {
					findings.fail(
						"differential-failed",
						at,
						format!(
							"{tb}.{p} = {lit}: {} answers to {} queries",
							answered.len(),
							queries.len()
						),
					);
					continue;
				}
				let pick = |i: usize| (queries[i].as_str(), &answered[i]);
				agree(
					&[pick(0), pick(1), pick(2), pick(3), pick(4)],
					"filter-differs",
					at,
					findings,
				);
				agree(&[pick(5), pick(6), pick(7)], "range-differs", at, findings);
				agree(&[pick(8), pick(9)], "filtered-count-differs", at, findings);
			}
		}
	}
}

/// What every table of every database answers to a scan, for comparing modes.
async fn contents(ds: &Datastore) -> BTreeMap<String, String> {
	let mut out = BTreeMap::new();
	for place in places(ds).await {
		out.extend(contents_in(ds, &place).await);
	}
	out
}

/// [`contents`], for the database `place`.
async fn contents_in(ds: &Datastore, place: &Place) -> BTreeMap<String, String> {
	let mut out = BTreeMap::new();
	let tables = answers(ds, place, "INFO FOR DB;", None).await;
	let Some(Ok(PublicValue::Object(info))) = tables.first() else {
		return out;
	};
	let Some(PublicValue::Object(tables)) = info.get("tables") else {
		return out;
	};
	for name in tables.keys() {
		let tb = path_sql(std::slice::from_ref(name));
		let scan = answers(ds, place, &format!("SELECT * FROM {tb} ORDER BY id;"), None).await;
		let rendered = match scan.first() {
			Some(Ok(PublicValue::Array(rows))) => {
				let mut rows: Vec<String> =
					rows.iter().map(|row| masked(&format!("{row:?}"))).collect();
				rows.sort();
				rows.join("\n")
			}
			other => other.map(|answer| masked(&render(answer))).unwrap_or_default(),
		};
		out.insert(format!("{}{name}", place.label()), rendered);
	}
	out
}

/// The database exported as text.
async fn export_text(ds: &Datastore, place: &Place) -> anyhow::Result<String> {
	let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
	let session = place.session();
	let task = ds.export(&session, tx).await?;
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await?;
	Ok(String::from_utf8(collector.await?)?)
}

/// The compatibility level of the database `place`.
async fn level_of(ds: &Datastore, place: &Place) -> u8 {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name(&place.ns, &place.db, None).await.unwrap();
	txn.cancel().await.unwrap();
	match db {
		Some(db) if db.compatibility.0 == crate::catalog::DatabaseCompatibility::LEVEL_3 => 3,
		_ => 4,
	}
}

/// Export every database and import each into a store this build creates, at the same level,
/// judging that the copy answers the same rows.
async fn judge_round_trip(ds: &Datastore, label: &str, at: &str, findings: &mut Findings) {
	let copy =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	for place in places(ds).await {
		let at = format!("{at} {}", place.label());
		let at = at.trim_end();
		let text = match export_text(ds, &place).await {
			Ok(text) => text,
			Err(e) => {
				findings.fail("export-failed", at, format!("{e:#}"));
				continue;
			}
		};
		if let Ok(dir) = std::env::var("LAYOUT_ORACLE_DUMP") {
			let name = format!("{dir}/{}.{}.{}.surql", label.replace('/', "_"), place.ns, place.db);
			let _ = std::fs::write(name, &text);
		}
		// An export redacts the keys of access methods and the hashes of users, which an import
		// refuses to restore: neither bears on the rows.
		let text: String = text
			.lines()
			.filter(|line| !line.starts_with("DEFINE ACCESS") && !line.starts_with("DEFINE USER"))
			.map(|line| format!("{line}\n"))
			.collect();
		let txn = copy.transaction(Write).await.unwrap();
		txn.ensure_ns_db(None, &place.ns, &place.db).await.unwrap();
		txn.commit().await.unwrap();
		let level = level_of(ds, &place).await;
		let alter = format!(
			"ALTER DATABASE {} COMPATIBILITY {level};",
			path_sql(std::slice::from_ref(&place.db))
		);
		for answer in answers(&copy, &place, &alter, None).await {
			answer.unwrap();
		}
		let failed = match copy.import(&text, &place.session()).await {
			Ok(responses) => responses
				.into_iter()
				.find_map(|response| response.result.err())
				.map(|e| e.to_string()),
			Err(e) => Some(format!("{e:#}")),
		};
		if let Some(error) = failed {
			// At level 3 a row may hold what its table no longer declares, or an id its `id` field
			// no longer admits, which a dump carries and an import refuses: a loss of the level.
			let level_3_loss = level == 3 && error.contains("but no such field exists");
			let id_loss = error.contains("for field `id`");
			if level_3_loss || id_loss {
				findings.warn("import-refused-known", at, error);
			} else {
				findings.fail("import-failed", at, error);
			}
			continue;
		}
		let (source, imported) = (contents_in(ds, &place).await, contents_in(&copy, &place).await);
		for (table, rows) in &source {
			match imported.get(table) {
				Some(copied) if copied == rows => {}
				copied => findings.fail(
					"round-trip-differs",
					at,
					format!(
						"{table}: the store answers {}, the imported copy {}",
						cut(rows, 400),
						cut(copied.map(String::as_str).unwrap_or("nothing"), 400)
					),
				),
			}
		}
	}
}

// ------------------------------------------------------------------------------------------------
// Running a scenario
// ------------------------------------------------------------------------------------------------

/// A unit's answers in one mode.
#[derive(Clone, Debug)]
struct Outcome {
	text: String,
	/// Whether the unit ran after `@upgrade`.
	upgraded: bool,
	answers: Vec<String>,
	/// The answers with the rows of each sorted, for statements whose order among equal sort keys
	/// is not fixed.
	unordered: Vec<String>,
}

/// What one mode of a scenario produced.
struct Run {
	mode: Mode,
	outcomes: Vec<Outcome>,
	findings: Vec<Finding>,
	contents: BTreeMap<String, String>,
}

/// The cheap checks, and with `full` the differential one, after the step `at`.
///
/// `drained` says the job has just run: no row may be left behind a step, unless a transaction
/// has been left open (`leaked`), which keeps the job from judging a verification and with it from
/// rewriting the table.
async fn check(
	ds: &Arc<Datastore>,
	at: &str,
	full: bool,
	drained: bool,
	leaked: bool,
	findings: &mut Findings,
) {
	for (place, tables) in stored(ds, at, findings).await {
		let at = format!("{at} {}", place.label());
		let at = at.trim_end();
		if drained {
			for table in &tables {
				if table.behind > 0 {
					let detail = format!(
						"{}: {} rows are behind a step after the job ran",
						table.name, table.behind
					);
					if leaked {
						findings.warn("left-behind", at, detail);
					} else {
						findings.fail("left-behind", at, detail);
					}
				}
			}
		}
		judge_reads(ds, &place, &tables, at, findings).await;
		if full {
			judge_paths(ds, &place, &tables, at, findings).await;
		}
	}
}

async fn run(scenario: Scenario, mode: Mode) -> Run {
	let ds = store(mode.origin).await;
	let mut findings = Findings::default();
	let mut outcomes = Vec::new();
	let mut upgraded = false;
	// A leaked transaction is reported once: every later wait would report it again.
	let mut leaked = false;
	let mut place = Place::test();
	for (index, step) in scenario.steps.iter().enumerate() {
		match step {
			Step::Unit(text) | Step::Client(text) => {
				let client = matches!(step, Step::Client(_));
				let at = format!(
					"#{index} {}{}`{}`",
					if client {
						"@client "
					} else {
						""
					},
					place.label(),
					cut(text, 80)
				);
				let answered = if client {
					client_answers(&ds, &place, text).await
				} else {
					answers(&ds, &place, text, None).await
				};
				// Each unit is a request of its own, so the database a `USE` names is carried to
				// the next one here.
				if !client && keyword(text) == "USE" {
					place = place.after_use(text);
				}
				for answer in &answered {
					if let Err(message) = answer
						&& SUSPECT.iter().any(|fragment| message.contains(fragment))
					{
						findings.fail("suspect-error", &at, message.clone());
					}
				}
				outcomes.push(Outcome {
					text: text.clone(),
					upgraded,
					answers: answered.iter().map(|answer| masked(&render(answer))).collect(),
					unordered: answered.iter().map(unordered).collect(),
				});
				if !leaked && !transactions_ended(&ds).await {
					leaked = true;
					findings.fail(
						"transaction-left-open",
						&at,
						"a transaction the unit opened is still open after it answered",
					);
				}
				let drained = if mode.eager {
					if let Err(e) = drain(&ds).await {
						findings.fail("drain-failed", &at, e);
					}
					true
				} else {
					false
				};
				check(&ds, &at, false, drained, leaked, &mut findings).await;
				if !leaked && !transactions_ended(&ds).await {
					leaked = true;
					findings.fail(
						"transaction-left-open",
						&at,
						"a transaction the oracle's own check or the job opened is still open",
					);
				}
			}
			Step::Upgrade => {
				upgraded = true;
				if mode.origin == Origin::Upgraded {
					if let Err(e) = ds.raise_feature_level(Release::FIRST_STORED).await {
						findings.fail("upgrade-failed", "@upgrade", format!("{e:#}"));
					}
					check(&ds, &format!("#{index} @upgrade"), true, false, leaked, &mut findings)
						.await;
				}
			}
			Step::Drain => {
				let at = format!("#{index} @drain");
				if let Err(e) = drain(&ds).await {
					findings.fail("drain-failed", &at, e);
				}
				check(&ds, &at, true, true, leaked, &mut findings).await;
			}
			Step::Check => {
				check(&ds, &format!("#{index} @check"), true, false, leaked, &mut findings).await
			}
		}
	}
	check(&ds, "end", true, mode.eager, leaked, &mut findings).await;
	judge_round_trip(&ds, &format!("{}.{}", scenario.name, mode.label()), "end", &mut findings)
		.await;
	let contents = contents(&ds).await;
	if let Ok(dir) = std::env::var("LAYOUT_ORACLE_ANSWERS") {
		let mut text = String::new();
		for outcome in &outcomes {
			let _ = writeln!(text, "{}", outcome.text.trim_end());
			for answer in &outcome.answers {
				let _ = writeln!(text, "  => {}", cut(answer, 600));
			}
		}
		let name =
			format!("{dir}/{}.{}.answers.txt", scenario.name.replace('/', "_"), mode.label());
		let _ = std::fs::write(name, text);
	}
	Run {
		mode,
		outcomes,
		findings: findings.0,
		contents,
	}
}

/// `answer` masked, with the rows of an array answer sorted.
fn unordered(answer: &Result<PublicValue, String>) -> String {
	match answer {
		Ok(PublicValue::Array(rows)) => {
			let mut rows: Vec<String> =
				rows.iter().map(|row| masked(&format!("{row:?}"))).collect();
			rows.sort();
			rows.join("\n")
		}
		other => masked(&render(other)),
	}
}

/// Refusals that depend on whether the table migration job has finished a table, which differ
/// between a lazy and an eager run by design.
const JOB_STATE: &[&str] = &[
	"until the rewrite job has finished",
	"has not finished rewriting",
	"rows are behind a migration step",
];

/// Whether a unit's answers describe how a read was carried out or the job's progress, which
/// differ between modes by design.
fn incidental(text: &str) -> bool {
	let upper = text.to_ascii_uppercase();
	upper.contains("EXPLAIN") || upper.contains("INFO FOR") || upper.contains("SHOW CHANGES")
}

/// Judge that `b` answered as `a` did: every unit, or only those after `@upgrade`, and the final
/// contents.
fn compare(a: &Run, b: &Run, upgraded_only: bool, findings: &mut Vec<Finding>) {
	let at = format!("{} vs {}", a.mode.label(), b.mode.label());
	for (x, y) in a.outcomes.iter().zip(&b.outcomes) {
		if (upgraded_only && !(x.upgraded && y.upgraded)) || incidental(&x.text) {
			continue;
		}
		if x.answers != y.answers {
			// One run refused what depends on the job's progress: the two states part here, and
			// nothing after is comparable.
			if x.answers
				.iter()
				.chain(&y.answers)
				.any(|answer| JOB_STATE.iter().any(|s| answer.contains(s)))
			{
				return;
			}
			if x.unordered == y.unordered {
				continue;
			}
			findings.push(Finding {
				kind: "modes-differ",
				at: at.clone(),
				detail: format!(
					"`{}`\n    {}: {}\n    {}: {}",
					cut(&x.text, 200),
					a.mode.label(),
					cut(&x.answers.join(" | "), 400),
					b.mode.label(),
					cut(&y.answers.join(" | "), 400)
				),
				warning: false,
			});
		}
	}
	for (table, rows) in &a.contents {
		let other = b.contents.get(table);
		if other != Some(rows) {
			findings.push(Finding {
				kind: "contents-differ",
				at: at.clone(),
				detail: format!(
					"{table}\n    {}: {}\n    {}: {}",
					a.mode.label(),
					cut(rows, 400),
					b.mode.label(),
					cut(other.map(String::as_str).unwrap_or("absent"), 400)
				),
				warning: false,
			});
		}
	}
}

/// The worker threads a slice runs its jobs on, which is also how many jobs it runs at once.
const WORKERS: usize = 4;

/// Runs the scenarios of slice `k` in every mode and requires every finding to be one [`KNOWN`]
/// names, and every one it names to occur.
fn layout_oracle(k: usize) {
	let admitted = scenarios();
	assert!(!admitted.is_empty(), "no scenario matches the filter");
	let scenarios = slice(admitted, k);
	if scenarios.is_empty() {
		return;
	}
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(WORKERS)
		.thread_stack_size(32 * 1024 * 1024)
		.enable_all()
		.build()
		.unwrap();
	runtime.block_on(async {
		let jobs: Vec<(Scenario, Mode)> = scenarios
			.iter()
			.flat_map(|scenario| Mode::ALL.iter().map(move |mode| (scenario.clone(), *mode)))
			.collect();
		let runs: Vec<(String, Mode, Result<Run, String>)> = futures::stream::iter(jobs)
			.map(|(scenario, mode)| async move {
				let name = scenario.name.clone();
				let outcome = tokio::spawn(run(scenario, mode)).await.map_err(|e| {
					if e.is_panic() {
						let panic = e.into_panic();
						let message = panic
							.downcast_ref::<String>()
							.cloned()
							.or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
							.unwrap_or_default();
						format!("panicked: {message}")
					} else {
						format!("{e}")
					}
				});
				(name, mode, outcome)
			})
			.buffer_unordered(WORKERS)
			.collect()
			.await;

		let mut report = String::new();
		let mut failures = 0usize;
		let mut warnings = 0usize;
		let mut occurred = vec![false; KNOWN.len()];
		for scenario in &scenarios {
			let mut findings: Vec<(String, Finding)> = Vec::new();
			let mut by_mode: Vec<&Run> = Vec::new();
			for (name, mode, outcome) in &runs {
				if *name != scenario.name {
					continue;
				}
				match outcome {
					Ok(run) => {
						findings.extend(run.findings.iter().map(|f| (mode.label(), f.clone())));
						by_mode.push(run);
					}
					Err(message) => findings.push((
						mode.label(),
						Finding {
							kind: "panic",
							at: "run".to_owned(),
							detail: message.clone(),
							warning: false,
						},
					)),
				}
			}
			let find = |label: &str| by_mode.iter().find(|run| run.mode.label() == label).copied();
			let mut compared = Vec::new();
			for (a, b, upgraded_only) in [
				("fresh4", "fresh4+eager", false),
				("fresh3", "fresh3+eager", false),
				("upgraded", "upgraded+eager", false),
				("fresh3", "upgraded", true),
			] {
				if let (Some(a), Some(b)) = (find(a), find(b)) {
					compare(a, b, upgraded_only, &mut compared);
				}
			}
			findings.extend(compared.into_iter().map(|f| ("modes".to_owned(), f)));
			for (_, finding) in &mut findings {
				if let Some(at) =
					KNOWN.iter().position(|known| known.matches(&scenario.name, finding))
				{
					occurred[at] = true;
					finding.warning = true;
					finding.detail = format!("{}\n    known: {}", finding.detail, KNOWN[at].reason);
				}
			}
			if findings.is_empty() {
				let _ = writeln!(report, "ok   {}", scenario.name);
				continue;
			}
			let failing = findings.iter().filter(|(_, f)| !f.warning).count();
			failures += failing;
			warnings += findings.len() - failing;
			let status = if failing > 0 {
				"FAIL"
			} else {
				"warn"
			};
			let _ = writeln!(report, "{status} {}", scenario.name);
			for (mode, finding) in &findings {
				let level = if finding.warning {
					"warning"
				} else {
					"failure"
				};
				let _ = writeln!(
					report,
					"  [{level}] {} [{mode}] at {}: {}",
					finding.kind, finding.at, finding.detail
				);
			}
		}
		for (known, occurred) in KNOWN.iter().zip(occurred) {
			if !occurred && scenarios.iter().any(|scenario| scenario.name.contains(known.scenario))
			{
				failures += 1;
				let _ = writeln!(
					report,
					"FAIL {}\n  [failure] known-finding-gone: no {} finding occurs: {}",
					known.scenario, known.kind, known.reason
				);
			}
		}
		let _ = writeln!(
			report,
			"\n{} scenarios, {failures} failures, {warnings} warnings",
			scenarios.len()
		);
		println!("{report}");
		if let Ok(path) = std::env::var("LAYOUT_ORACLE_REPORT") {
			std::fs::write(format!("{path}.{k:02}"), &report).unwrap();
		}
		assert_eq!(failures, 0, "the layout oracle found failures:\n{report}");
	});
}

sliced_tests! {
	layout_oracle, ORACLE_SLICES;
	layout_oracle_00 => 0,
	layout_oracle_01 => 1,
	layout_oracle_02 => 2,
	layout_oracle_03 => 3,
	layout_oracle_04 => 4,
	layout_oracle_05 => 5,
	layout_oracle_06 => 6,
	layout_oracle_07 => 7,
	layout_oracle_08 => 8,
	layout_oracle_09 => 9,
	layout_oracle_10 => 10,
	layout_oracle_11 => 11,
}

/// The slices of the scenarios hold each one once between them and none is empty, so the sliced
/// tests judge every scenario and each has work to do.
#[test]
fn slices_cover_the_scenarios() {
	let names: Vec<String> =
		scenarios_matching("", true).into_iter().map(|scenario| scenario.name).collect();
	assert!(names.iter().any(|name| name.starts_with("lt/")), "the language tests are listed");
	assert!(names.iter().any(|name| !name.starts_with("lt/")), "this suite's scenarios are listed");
	assert_slices_cover("scenarios", &names);
}

/// A slice reports a [`KNOWN`] finding gone when it holds a scenario the entry names and none of
/// its scenarios has the finding. An entry that named several scenarios could be reported gone by
/// a slice that does not hold the one that has the finding, so each entry names exactly one.
#[test]
fn each_known_finding_names_one_scenario() {
	let scenarios = scenarios_matching("", true);
	for known in KNOWN {
		let named: Vec<&str> = scenarios
			.iter()
			.filter(|scenario| scenario.name.contains(known.scenario))
			.map(|scenario| scenario.name.as_str())
			.collect();
		assert_eq!(named.len(), 1, "{} names {named:?}", known.scenario);
	}
}

#[test]
fn statements_split_outside_strings_brackets_and_comments() {
	let split = statements(
		"DEFINE TABLE t; -- a comment; with a semicolon\n\
		 CREATE t:1 SET a = 'x;y', b = { c: 1; }; /* block; */ RETURN [1, 2];",
	);
	assert_eq!(
		split,
		vec!["DEFINE TABLE t", "CREATE t:1 SET a = 'x;y', b = { c: 1; }", "RETURN [1, 2]"]
	);
	let grouped = units(statements(
		"BEGIN; CREATE t:1; COMMIT; CREATE t:2; LET $x = 1; RETURN $x; RETURN 2;",
	));
	assert_eq!(
		grouped,
		vec!["BEGIN;\nCREATE t:1;\nCOMMIT;", "CREATE t:2;", "LET $x = 1;\nRETURN $x;\nRETURN 2;"]
	);
}
