//! Seed, export, import, export again — and compare the two databases.
//!
//! An export earns its keep only if replaying it reconstructs the database it
//! came from. The failures that matter here are the quiet ones: a restore that
//! answers success on every statement while having dropped a back-reference,
//! rewound a sequence, or turned a view into a frozen table. Neither the export
//! text nor a record dump announces any of that, so [`RoundTrip`] compares four
//! independent surfaces and each test picks the ones its fixture can speak to:
//!
//! - **P1** — every record, as typed [`Value`]s ([`RoundTrip::assert_records_equal`]).
//! - **P2** — the catalog, through `INFO FOR DB` and `INFO FOR TABLE`
//!   ([`RoundTrip::assert_catalog_equal`]).
//! - **P3** — derived state, which appears in neither of the above and so is compared by running
//!   the same query against both sides ([`RoundTrip::assert_query_equal`]).
//! - **P5** — the export text itself, which must reach a fixed point
//!   ([`RoundTrip::assert_fixed_point`]).
//!
//! Two properties hold this together.
//!
//! Comparison is on typed values throughout, and stricter than `==`. JSON
//! collapses `3`, `3f` and `3dec` onto each other, rendered SurrealQL is what
//! the fixed-point check already compares, and [`Value`]'s own `PartialEq`
//! compares numbers across kinds by magnitude — so all three would pass a
//! restore that rewrote every number's kind. [`identical`] is what the
//! assertions run on instead.
//!
//! The import runs through [`Datastore::import_stream`], the path `POST /import`
//! and `surreal import` take, and **every** statement it reports must have
//! succeeded. That path sets `OPTION IMPORT`, which suppresses events, field
//! processing and table views — so an import driven by a plain `execute` would
//! be a different operation from the one under test, and would silently pass
//! cases the real restore drops.

#![allow(dead_code)]

mod catalog;
mod derived;
mod failure;
mod values;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use common::fmt::EscapeIdent;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_rpc::{QueryResult, QueryType};
use surrealdb_types::{Error as TypesError, ToSql, Value};

use crate::helpers::{new_ns_db, zero_volatile_system_block};

/// The namespace and database both sides of a round trip live in.
///
/// An export carries a `DEFINE DATABASE` line only when the database holds a
/// clause of its own, and that line names `$session.db` rather than a literal,
/// so a restore always lands in a namespace and database the operator created
/// first. The target is prepared the same way for that reason.
const NS: &str = "test";
const DB: &str = "test";

/// How much of a long export a failure message is allowed to quote.
const QUOTE_LIMIT: usize = 8192;

/// How many divergent records one failure message names before it summarises.
const REPORT_LIMIT: usize = 5;

/// A completed round trip, holding both datastores and both exports.
///
/// Built by [`RoundTrip::run`]. The source is the database the fixture seeded;
/// the target is a fresh database the source's export was replayed into. Both
/// stay alive so derived state can be queried on either side after the fact.
pub struct RoundTrip {
	source: Arc<Datastore>,
	target: Arc<Datastore>,
	session: Session,
	export_a: String,
	export_b: String,
}

impl RoundTrip {
	/// Seeds a fresh source datastore with `seed`, exports it, imports that
	/// export into a second fresh datastore, and re-exports the second.
	///
	/// Panics if any seed statement or any import statement returned an error.
	/// An import that rejects one statement among thousands still answers
	/// success overall and leaves nothing behind but that one result, so this
	/// assertion is the only thing standing between a lost statement and a
	/// green test.
	pub async fn run(seed: &str) -> Result<RoundTrip> {
		let (round_trip, results) = Self::run_allowing_import_errors(seed).await?;
		let errors = error_messages(&results);
		assert!(
			errors.is_empty(),
			"the import rejected {} of its statements. A restore reports these per statement \
			 while still answering success overall, so a rejected statement is data that never \
			 arrived:\n{}\n\n--- the export being replayed ---\n{}",
			errors.len(),
			errors.join("\n"),
			quote(&round_trip.export_a),
		);
		Ok(round_trip)
	}

	/// As [`RoundTrip::run`], but returns the import's per-statement results
	/// rather than asserting they all succeeded. For tests that pin a failing
	/// import.
	///
	/// The import path reports only the statements that failed, so a clean
	/// restore returns an empty vector. A failure that ends the whole stream
	/// rather than one statement — a body the parser rejects outright, say —
	/// is reported as a single error result, so a test pinning it reads the
	/// same shape as a per-statement rejection.
	///
	/// Still panics on a failing seed: a fixture that cannot be built says
	/// nothing about the restore.
	pub async fn run_allowing_import_errors(seed: &str) -> Result<(RoundTrip, Vec<QueryResult>)> {
		let session = Session::owner().with_ns(NS).with_db(DB);

		let source = fresh_datastore().await?;
		let results = source.execute(seed, &session, None).await?;
		let errors = error_messages(&results);
		assert!(
			errors.is_empty(),
			"the seed failed, so this fixture says nothing about the restore:\n{}\n\n--- seed ---\n{}",
			errors.join("\n"),
			quote(seed),
		);
		let export_a = export_text(&source, &session).await?;

		let target = fresh_datastore().await?;
		let body = {
			let sql = export_a.clone();
			futures::stream::once(async move { Ok(Bytes::from(sql)) })
		};
		let results = match target.import_stream(&session, body).await {
			Ok(results) => results,
			Err(e) => vec![QueryResult {
				time: Duration::ZERO,
				result: Err(TypesError::internal(e.to_string())),
				query_type: QueryType::Other,
			}],
		};
		let export_b = export_text(&target, &session).await?;

		Ok((
			RoundTrip {
				source,
				target,
				session,
				export_a,
				export_b,
			},
			results,
		))
	}

	/// P1: every record in the target equals the source's, compared as typed
	/// [`Value`]s under [`identical`] — never as JSON and never as rendered
	/// text.
	///
	/// Tables come from the catalog rather than from the caller, so a table
	/// present on one side and not the other is itself a failure.
	pub async fn assert_records_equal(&self) -> Result<()> {
		let source_tables = table_names(&self.source, &self.session).await?;
		let target_tables = table_names(&self.target, &self.session).await?;
		assert_tables_match(&source_tables, &target_tables);

		for table in &source_tables {
			// A fixed order on both sides, so a record is compared against its
			// own counterpart rather than against whatever the scan returned
			// first.
			let sql = format!("SELECT * FROM {} ORDER BY id", EscapeIdent(table).to_sql());
			let source = single_value(&self.source, &self.session, &sql).await?;
			let target = single_value(&self.target, &self.session, &sql).await?;
			assert_records_match(table, &source, &target);
		}
		Ok(())
	}

	/// P2: `INFO FOR DB` and every `INFO FOR TABLE` match between the two.
	pub async fn assert_catalog_equal(&self) -> Result<()> {
		self.assert_query_equal("INFO FOR DB").await?;

		let source_tables = table_names(&self.source, &self.session).await?;
		let target_tables = table_names(&self.target, &self.session).await?;
		assert_tables_match(&source_tables, &target_tables);
		for table in &source_tables {
			let sql = format!("INFO FOR TABLE {}", EscapeIdent(table).to_sql());
			self.assert_query_equal(&sql).await?;
		}
		Ok(())
	}

	/// P5: the two exports are byte-identical.
	///
	/// A restore whose own export differs from the one it was built from has
	/// changed something the export can see, and every later export of that
	/// database inherits the change.
	#[track_caller]
	pub fn assert_fixed_point(&self) {
		if self.export_a == self.export_b {
			return;
		}
		panic!(
			"re-exporting the restored database did not reproduce the export it was built \
			 from:\n{}\n\n--- first export ---\n{}\n\n--- second export ---\n{}",
			describe_text_difference(&self.export_a, &self.export_b),
			quote(&self.export_a),
			quote(&self.export_b),
		);
	}

	/// P1 + P2 + P5.
	pub async fn assert_all(&self) -> Result<()> {
		self.assert_records_equal().await?;
		self.assert_catalog_equal().await?;
		self.assert_fixed_point();
		Ok(())
	}

	/// P3: runs `sql` against both datastores and asserts the values match.
	///
	/// This is how derived state — references, sequence positions, index-backed
	/// reads — is compared, since none of it appears in the records or in the
	/// catalog. The query has to be its own oracle: give it a deterministic
	/// order, and read no clock.
	pub async fn assert_query_equal(&self, sql: &str) -> Result<()> {
		let mut source = self.source_query(sql).await?;
		let mut target = self.target_query(sql).await?;
		// Host metrics belong to the machine, not to the restore. Zeroing them
		// leaves the shape of the block in the comparison.
		source.iter_mut().for_each(zero_volatile_system_block);
		target.iter_mut().for_each(zero_volatile_system_block);

		assert_eq!(
			source.len(),
			target.len(),
			"`{sql}` answered {} statement(s) against the source and {} against the target",
			source.len(),
			target.len(),
		);
		for (i, (source, target)) in source.iter().zip(target.iter()).enumerate() {
			if identical(source, target) {
				continue;
			}
			panic!(
				"`{sql}` diverged at statement {}:\n{}",
				i + 1,
				describe_value_difference(source, target),
			);
		}
		Ok(())
	}

	/// Runs `sql` against the seeded database, one value per statement.
	pub async fn source_query(&self, sql: &str) -> Result<Vec<Value>> {
		query(&self.source, &self.session, sql).await.context("against the source")
	}

	/// Runs `sql` against the restored database, one value per statement.
	pub async fn target_query(&self, sql: &str) -> Result<Vec<Value>> {
		query(&self.target, &self.session, sql).await.context("against the target")
	}

	/// The export taken from the seeded database — the text that was replayed.
	pub fn export_a(&self) -> &str {
		&self.export_a
	}

	/// The export taken from the restored database.
	pub fn export_b(&self) -> &str {
		&self.export_b
	}
}

/// Builds a memory datastore holding an empty `test`/`test` database.
async fn fresh_datastore() -> Result<Arc<Datastore>> {
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await?;
	new_ns_db(&ds, NS, DB).await?;
	Ok(ds)
}

/// Exports the whole database as text.
async fn export_text(ds: &Datastore, ses: &Session) -> Result<String> {
	let (tx, rx) = surrealdb_core::channel::bounded::<Vec<u8>>(16);
	let task = ds.export(ses, tx).await?;
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

/// Runs `sql`, returning one value per statement and failing on the first
/// statement that errored.
async fn query(ds: &Datastore, ses: &Session, sql: &str) -> Result<Vec<Value>> {
	let results = ds.execute(sql, ses, None).await?;
	let mut values = Vec::with_capacity(results.len());
	for (i, result) in results.into_iter().enumerate() {
		values.push(result.result.with_context(|| format!("statement {} of `{sql}`", i + 1))?);
	}
	Ok(values)
}

/// Runs a single-statement `sql`, returning its value.
async fn single_value(ds: &Datastore, ses: &Session, sql: &str) -> Result<Value> {
	let mut values = query(ds, ses, sql).await?;
	if values.len() != 1 {
		bail!("`{sql}` must answer with exactly one statement, got {}", values.len());
	}
	Ok(values.remove(0))
}

/// The table names the catalog holds, in a deterministic order.
async fn table_names(ds: &Datastore, ses: &Session) -> Result<Vec<String>> {
	let info = single_value(ds, ses, "INFO FOR DB").await?;
	let Value::Object(info) = &info else {
		bail!("`INFO FOR DB` must answer with an object, got {info:?}");
	};
	let Some(Value::Object(tables)) = info.get("tables") else {
		bail!("`INFO FOR DB` must carry a `tables` block, got {info:?}");
	};
	Ok(tables.keys().cloned().collect())
}

/// The error message of every result that failed.
fn error_messages(results: &[QueryResult]) -> Vec<String> {
	results.iter().filter_map(|r| r.result.as_ref().err()).map(|e| format!("  - {e}")).collect()
}

/// Quotes text into a failure message, trimming a long export to its head.
fn quote(text: &str) -> String {
	if text.len() <= QUOTE_LIMIT {
		return text.to_owned();
	}
	// Trim on a char boundary: an export carries arbitrary user text.
	let mut end = QUOTE_LIMIT;
	while !text.is_char_boundary(end) {
		end -= 1;
	}
	format!("{}\n… {} further bytes elided", &text[..end], text.len() - end)
}

/// Fails unless both catalogs hold the same tables, naming the ones that differ.
#[track_caller]
fn assert_tables_match(source: &[String], target: &[String]) {
	if source == target {
		return;
	}
	let source_set: BTreeSet<&String> = source.iter().collect();
	let target_set: BTreeSet<&String> = target.iter().collect();
	let only_source: Vec<&&String> = source_set.difference(&target_set).collect();
	let only_target: Vec<&&String> = target_set.difference(&source_set).collect();
	panic!(
		"the two catalogs hold different tables — only in the source: {only_source:?}; \
		 only in the target: {only_target:?}"
	);
}

/// Fails unless a table holds the same records on both sides, naming every
/// record and field that differs.
#[track_caller]
fn assert_records_match(table: &str, source: &Value, target: &Value) {
	let source = expect_rows(table, "source", source);
	let target = expect_rows(table, "target", target);

	let source_by_id: BTreeMap<String, &Value> =
		source.iter().enumerate().map(|(i, r)| (record_label(i, r), r)).collect();
	let target_by_id: BTreeMap<String, &Value> =
		target.iter().enumerate().map(|(i, r)| (record_label(i, r), r)).collect();

	assert_eq!(
		source_by_id.len(),
		source.len(),
		"two records in `{table}` on the source render the same id, so they cannot be paired"
	);
	assert_eq!(
		target_by_id.len(),
		target.len(),
		"two records in `{table}` on the target render the same id, so they cannot be paired"
	);

	let source_ids: BTreeSet<&String> = source_by_id.keys().collect();
	let target_ids: BTreeSet<&String> = target_by_id.keys().collect();
	let only_source: Vec<&&String> = source_ids.difference(&target_ids).collect();
	let only_target: Vec<&&String> = target_ids.difference(&source_ids).collect();
	assert!(
		only_source.is_empty() && only_target.is_empty(),
		"table `{table}` holds different records: {} only in the source {only_source:?}; \
		 {} only in the target {only_target:?}",
		only_source.len(),
		only_target.len(),
	);

	let mut divergent = Vec::new();
	for (id, source) in &source_by_id {
		let target = target_by_id[id];
		if identical(source, target) {
			continue;
		}
		divergent.push(format!(
			"  {table}:{id}\n{}",
			indent(&describe_value_difference(source, target))
		));
	}
	if divergent.is_empty() {
		return;
	}
	let total = divergent.len();
	divergent.truncate(REPORT_LIMIT);
	let elided = total - divergent.len();
	let tail = if elided == 0 {
		String::new()
	} else {
		format!("\n  … and {elided} further record(s)")
	};
	panic!(
		"table `{table}` restored {total} record(s) differently:\n{}{tail}",
		divergent.join("\n")
	);
}

/// The rows a `SELECT * FROM <table>` answered with.
#[track_caller]
fn expect_rows<'a>(table: &str, side: &str, value: &'a Value) -> &'a [Value] {
	match value {
		Value::Array(rows) => rows,
		other => panic!("selecting from `{table}` on the {side} answered with {other:?}, not rows"),
	}
}

/// How a record is addressed in a failure message: its own id where it has one,
/// its position in the ordered scan otherwise.
fn record_label(index: usize, record: &Value) -> String {
	match record {
		Value::Object(fields) => match fields.get("id") {
			Some(id) => id.to_sql(),
			None => format!("#{index} (no id)"),
		},
		_ => format!("#{index}"),
	}
}

/// Whether two values are identical in both magnitude and representation.
///
/// [`Value`]'s own `PartialEq` compares numbers across kinds by magnitude: `3`,
/// `3f` and `3dec` all count as equal to each other, and `0f` counts as equal to
/// `-0f`. An export renders each of those differently and the parser reads each
/// one back as its own kind, so a restore that rewrote every number's kind would
/// pass an ordinary comparison with nothing to show for it. Requiring the derived
/// `Debug` forms to match as well pins the representation — which number kind, a
/// decimal's scale, the sign on a zero — alongside the magnitude, for every value
/// nested anywhere inside the one being compared.
fn identical(a: &Value, b: &Value) -> bool {
	a == b && format!("{a:?}") == format!("{b:?}")
}

/// Describes every leaf at which two values differ, addressed by its path from
/// the value's root.
fn describe_value_difference(source: &Value, target: &Value) -> String {
	let mut lines = Vec::new();
	collect_differences("", source, target, &mut lines);
	if lines.is_empty() {
		// Unreachable for values that compare unequal, but a comparison that
		// disagrees with itself must say so rather than report nothing.
		lines.push(format!(
			"  the two values compare unequal but no leaf differs: source = {source:?}, target = {target:?}"
		));
	}
	lines.join("\n")
}

fn collect_differences(path: &str, source: &Value, target: &Value, out: &mut Vec<String>) {
	if identical(source, target) {
		return;
	}
	match (source, target) {
		(Value::Object(source), Value::Object(target)) => {
			let keys: BTreeSet<&String> = source.keys().chain(target.keys()).collect();
			for key in keys {
				let child = if path.is_empty() {
					key.clone()
				} else {
					format!("{path}.{key}")
				};
				match (source.get(key), target.get(key)) {
					(Some(source), Some(target)) => {
						collect_differences(&child, source, target, out)
					}
					(Some(source), None) => out.push(format!(
						"  {child}: present in the source ({}), absent from the target",
						source.to_sql()
					)),
					(None, Some(target)) => out.push(format!(
						"  {child}: absent from the source, present in the target ({})",
						target.to_sql()
					)),
					(None, None) => {}
				}
			}
		}
		(Value::Array(source), Value::Array(target)) if source.len() == target.len() => {
			for (i, (source, target)) in source.iter().zip(target.iter()).enumerate() {
				collect_differences(&format!("{path}[{i}]"), source, target, out);
			}
		}
		_ => out.push(describe_leaf(path, source, target)),
	}
}

/// One line naming a leaf that differs.
///
/// Values are quoted as SurrealQL because that is what a reader recognises. Two
/// values can render alike and still differ — `3` and `3f` do — so a rendering
/// that fails to separate them is escalated to the typed form, which is what the
/// comparison itself ran on.
fn describe_leaf(path: &str, source: &Value, target: &Value) -> String {
	let path = if path.is_empty() {
		"<value>"
	} else {
		path
	};
	let (rendered_source, rendered_target) = (source.to_sql(), target.to_sql());
	if rendered_source == rendered_target {
		return format!(
			"  {path}: both render as `{rendered_source}` but hold different types — \
			 source = {source:?}, target = {target:?}"
		);
	}
	format!("  {path}: source = {rendered_source}, target = {rendered_target}")
}

/// Names the first line at which two exports diverge.
fn describe_text_difference(a: &str, b: &str) -> String {
	let (a_lines, b_lines): (Vec<&str>, Vec<&str>) = (a.lines().collect(), b.lines().collect());
	for (i, (a, b)) in a_lines.iter().zip(b_lines.iter()).enumerate() {
		if a != b {
			return format!(
				"  first divergence at line {}:\n    first  export: {a}\n    second export: {b}",
				i + 1
			);
		}
	}
	match a_lines.len().cmp(&b_lines.len()) {
		std::cmp::Ordering::Less => format!(
			"  the second export carries {} extra line(s), the first of which is: {}",
			b_lines.len() - a_lines.len(),
			b_lines[a_lines.len()]
		),
		std::cmp::Ordering::Greater => format!(
			"  the first export carries {} extra line(s), the first of which is: {}",
			a_lines.len() - b_lines.len(),
			a_lines[b_lines.len()]
		),
		// Equal line counts with no differing line: the two differ only in
		// trailing bytes that `lines` drops.
		std::cmp::Ordering::Equal => format!(
			"  the two exports differ only in line endings or in trailing bytes \
			 ({} bytes against {})",
			a.len(),
			b.len()
		),
	}
}

/// Indents an already-formatted block by one level, for nesting inside a list.
fn indent(block: &str) -> String {
	block.lines().map(|l| format!("  {l}")).collect::<Vec<_>>().join("\n")
}

/// The harness end to end on a fixture small enough to reason about: a schema,
/// a couple of records, and a pair of numbers that `Value`'s own `PartialEq`
/// treats as equal while the suite's comparison does not.
#[tokio::test]
async fn round_trip_harness_restores_a_trivial_fixture() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMAFULL;
		 DEFINE FIELD name ON person TYPE string;
		 DEFINE FIELD score ON person TYPE number;
		 CREATE person:alice SET name = 'Alice', score = 3;
		 CREATE person:bob SET name = 'Bob', score = 3f;",
	)
	.await?;

	round_trip.assert_all().await?;
	round_trip.assert_query_equal("SELECT VALUE score FROM person ORDER BY id").await?;

	// The fixture has to have reached the export, or every assertion above is
	// comparing two empty databases.
	assert!(
		round_trip.export_a().contains("DEFINE TABLE person"),
		"the export must carry the fixture's schema:\n{}",
		round_trip.export_a()
	);
	let seeded = round_trip.source_query("SELECT * FROM person").await?;
	let Value::Array(seeded) = &seeded[0] else {
		bail!("selecting from `person` must answer with rows, got {:?}", seeded[0]);
	};
	assert_eq!(seeded.len(), 2, "the fixture must have seeded two records");
	Ok(())
}
