//! Phase 4 of the plan: the failures a restore reports — imports that reject a
//! statement, pinned so a silent success cannot replace a loud one.
//!
//! An import is not transactional. Each statement commits on its own, execution
//! carries on past one that fails, and the path reports only the statements that
//! did not apply — so a restore that dropped a statement and a restore that
//! dropped nothing differ by one entry in a vector nobody is obliged to read.
//! Two contracts follow from that, and neither is stated anywhere else:
//!
//! - an export that cannot be fully replayed reports every statement it dropped, rather than
//!   answering with an empty success;
//! - what the failing statement leaves behind is the statements before it, applied — the database
//!   an operator has to reason about mid-migration.
//!
//! The rest of the file is the ordering the replay depends on. A dump is a flat
//! statement sequence, but the database it describes is not flat: a view is
//! recomputed from tables that have to exist first, a view over a view from a
//! view that has to exist first, and an edge names endpoint records that arrive
//! later in the same dump. Each of those is a case where the export succeeds, the
//! import answers success, and the restored database is still wrong unless the
//! order is right — so each is pinned from the import side, on both sides of the
//! name order the exporter would otherwise follow.

use anyhow::{Result, bail};
use surrealdb_expr::val::MAX_VALUE_DEPTH;
use surrealdb_rpc::QueryResult;
use surrealdb_types::{ToSql, Value};

use super::RoundTrip;

/// The message of every result that failed, in the order the import reported
/// them.
fn failures(results: &[QueryResult]) -> Vec<String> {
	results.iter().filter_map(|r| r.result.as_ref().err()).map(|e| e.to_string()).collect()
}

/// Fails unless `sql` answers with one value on each side, and returns the pair.
async fn both(rt: &RoundTrip, sql: &str) -> Result<(Value, Value)> {
	let (mut source, mut target) = (rt.source_query(sql).await?, rt.target_query(sql).await?);
	if source.len() != 1 || target.len() != 1 {
		bail!("`{sql}` must answer with exactly one statement on each side");
	}
	Ok((source.remove(0), target.remove(0)))
}

/// The table names a database holds, rendered as a sorted SurrealQL array.
const TABLE_NAMES: &str = "RETURN array::sort(object::keys((INFO FOR DB).tables))";

/// The tables an export carries records for, in the order their data sections
/// appear.
///
/// A table's `INSERT` statements are emitted under a header naming it, and a
/// table the export writes no records for gets no header — so this reports what
/// the dump actually carries rather than what the catalog says the database
/// holds.
fn data_sections(export: &str) -> Vec<&str> {
	export.lines().filter_map(|l| l.strip_prefix("-- TABLE DATA: ")).collect()
}

/// A database holding a definition cycle: two views that select from each other.
///
/// The catalog stores such a pair, and no order of a single-pass replay can
/// satisfy it — whichever view is defined first names one that does not exist
/// yet. It is the one fixture that makes a real export unreplayable without
/// hand-writing the dump, which is what makes it the fixture for the two
/// contracts above. The stored tables carry the rest of the database, so what
/// survives the failure is visible rather than inferred.
///
/// The cycle is formed while `base` is empty and the records live on a table no
/// view reads, because a view cascade over a cycle with rows in it does not
/// terminate.
const CYCLE: &str = "DEFINE TABLE base SCHEMALESS;
	 DEFINE TABLE v_a AS SELECT count() AS c FROM base GROUP ALL;
	 DEFINE TABLE v_b AS SELECT count() AS c FROM v_a GROUP ALL;
	 DEFINE TABLE OVERWRITE v_a AS SELECT count() AS c FROM v_b GROUP ALL;
	 DEFINE TABLE other SCHEMALESS;
	 CREATE other:1 SET n = 1;
	 CREATE other:2 SET n = 2;";

/// An export that cannot be fully replayed reports every statement it dropped.
///
/// The import path suppresses the results of statements that succeeded, so a
/// clean restore and a restore that lost half the database both answer with a
/// vector — one empty, one not — and nothing else distinguishes them. A caller
/// that reads the vector's length, or that treats `Ok` as the verdict, sees
/// success either way. What has to hold is that the dropped statements are in
/// there and say which definition failed.
#[tokio::test]
async fn an_unreplayable_export_reports_every_statement_it_dropped() -> Result<()> {
	let (round_trip, results) = RoundTrip::run_allowing_import_errors(CYCLE).await?;

	let failures = failures(&results);
	assert_eq!(
		failures.len(),
		results.len(),
		"the import reported {} result(s) of which only {} failed, so it reported a statement \
		 that succeeded:\n{results:?}",
		results.len(),
		failures.len(),
	);
	// One member of the cycle is emitted before the other, and only that one
	// cannot replay: the dump defines each member as a plain table ahead of its
	// own definition, so by the time the second member's definition runs, the
	// name it reads is there. The report is the definition that was dropped and
	// the reason, which is the source it could not find.
	assert_eq!(
		failures.len(),
		1,
		"only the member emitted before its source may be reported — more means the dump \
		 stopped defining the names ahead of the definitions that read them:\n{failures:#?}"
	);
	assert!(
		failures[0].contains("v_b"),
		"the reported failure must name the source the dropped definition could not \
		 find:\n{failures:#?}"
	);

	// And the export really was one an import cannot replay, rather than a
	// fixture that failed to build the cycle.
	let (source, _) = both(&round_trip, TABLE_NAMES).await?;
	assert_eq!(
		source.to_sql(),
		"['base', 'other', 'v_a', 'v_b']",
		"the seeded database must hold both views for the restore to drop"
	);
	Ok(())
}

/// A statement an import drops leaves the statements before it applied, and the
/// database says so.
///
/// This is the state an operator is handed when a restore reports a failure:
/// not the database the dump describes, and not an empty one either. Nothing
/// rolls back and execution does not stop at the first failure, so what is left
/// is every statement but the failing ones. An export of that database is
/// therefore a smaller dump than the one it was restored from, which is the
/// other half of what has to be pinned: the loss is not confined to the one
/// restore that reported it.
#[tokio::test]
async fn a_dropped_statement_leaves_the_rest_of_the_dump_applied() -> Result<()> {
	let (round_trip, results) = RoundTrip::run_allowing_import_errors(CYCLE).await?;
	assert!(!failures(&results).is_empty(), "the fixture must produce a failing import");

	// The stored tables are replayed before the views, so both definitions and
	// every record reached the restore.
	let (source, target) = both(&round_trip, "SELECT * FROM other ORDER BY id").await?;
	assert_eq!(
		target.to_sql(),
		source.to_sql(),
		"the records of a table the failure did not touch must still be restored in full"
	);
	assert_eq!(
		target.to_sql(),
		"[{ id: other:1, n: 1 }, { id: other:2, n: 2 }]",
		"and they must be the records the fixture seeded"
	);

	// Every table is there, including both members of the cycle: the dropped
	// statement cost a definition, not a name.
	let (source, target) = both(&round_trip, TABLE_NAMES).await?;
	assert_eq!(
		target.to_sql(),
		source.to_sql(),
		"a dropped view definition must not take its table with it"
	);
	assert_eq!(
		target.to_sql(),
		"['base', 'other', 'v_a', 'v_b']",
		"and the fixture's four tables are what should be there"
	);

	// What it did cost is that one definition. The member whose definition was
	// dropped is a plain table on the restored side, and its partner — replayed
	// after that table existed — is a view of it.
	let (_, target) = both(&round_trip, "RETURN (INFO FOR DB).tables.v_a").await?;
	assert!(
		!target.to_sql().contains("AS SELECT"),
		"the view whose definition was dropped must be left as the plain table that stood in \
		 for it: {}",
		target.to_sql()
	);
	let (_, target) = both(&round_trip, "RETURN (INFO FOR DB).tables.v_b").await?;
	assert!(
		target.to_sql().contains("AS SELECT"),
		"its partner's definition had a table to read and must have replayed: {}",
		target.to_sql()
	);

	// So a re-export of the restored database still carries every table's data,
	// and states the loss: the cycle is gone, and with it the view `v_a` was.
	for table in data_sections(round_trip.export_a()) {
		assert!(
			data_sections(round_trip.export_b()).contains(&table) || table == "v_b",
			"the restored database dropped the data section for `{table}`, which the dump it \
			 was restored from carried"
		);
	}
	assert!(
		round_trip.export_a().contains("DEFINE TABLE v_a TYPE ANY SCHEMALESS AS SELECT"),
		"the export being replayed must define the views:\n{}",
		round_trip.export_a()
	);
	assert!(
		!round_trip.export_b().contains("DEFINE TABLE v_a TYPE ANY SCHEMALESS AS SELECT"),
		"the restored database must not define a view whose restore failed:\n{}",
		round_trip.export_b()
	);
	Ok(())
}

/// The seed that grows a record to the deepest value the storage layer accepts.
///
/// Each statement is shallow, so the depth is reached by repeated writes and
/// never passes through the query parser on the way in — which is precisely what
/// makes it a restore problem: the dump is the first time that value has to be
/// read as text.
fn deepest_storable_record() -> String {
	let mut seed = String::from("CREATE deep:1 SET data = 'leaf' RETURN NONE;\n");
	// The record object is the first level and the leaf the last, so the
	// wrappers between them carry the rest.
	for _ in 0..MAX_VALUE_DEPTH - 2 {
		seed.push_str("UPDATE deep:1 SET data = { l: data } RETURN NONE;\n");
	}
	seed
}

/// A record at the deepest depth the storage layer accepts survives a restore.
///
/// The writer and the import parser bound the same thing from opposite sides,
/// and a record between the two bounds is one the database holds, exports, and
/// cannot read back. Nothing else in the suite reaches that band: every other
/// fixture is nested a handful of levels deep, and a value this deep cannot be
/// written as a literal at all.
///
/// The records are compared through the two exports rather than through
/// [`RoundTrip::assert_records_equal`]: a statement result nests a record one
/// level inside the rows it answers with, one past what the public-value
/// conversion accepts, so no `SELECT` can carry this record.
#[tokio::test]
async fn the_deepest_storable_record_survives_a_restore() -> Result<()> {
	let round_trip = RoundTrip::run(&deepest_storable_record()).await?;

	round_trip.assert_catalog_equal().await?;
	round_trip.assert_fixed_point();

	// The fixture has to have reached the depth it claims, or the export above
	// is an ordinary shallow record and the assertion says nothing.
	assert_eq!(
		round_trip.export_a().matches("{ l: ").count(),
		MAX_VALUE_DEPTH - 2,
		"the export must carry the record at full depth:\n{}",
		round_trip.export_a()
	);
	let (source, target) = both(&round_trip, "SELECT VALUE id FROM deep").await?;
	assert_eq!(target.to_sql(), source.to_sql(), "the deep record must be restored");
	assert_eq!(target.to_sql(), "[deep:1]", "and it must be the record the fixture seeded");
	Ok(())
}

/// The depth ceiling is the writer's, and it is refused where it is crossed.
///
/// A value one level past the limit is rejected at the write that would create
/// it, on a restored database exactly as on the database it was restored from —
/// so no export can carry a record too deep to be read back, and the band where
/// a record could be stored and not restored is closed rather than moved. The
/// two databases refusing identically is also the only available evidence that
/// the restored record sits at the same depth as the original: no query can
/// return a record this deep to compare.
#[tokio::test]
async fn a_value_past_the_depth_ceiling_is_refused_on_both_sides() -> Result<()> {
	let round_trip = RoundTrip::run(&deepest_storable_record()).await?;

	const ONE_MORE: &str = "UPDATE deep:1 SET data = { l: data } RETURN NONE";
	for (side, result) in [
		("source", round_trip.source_query(ONE_MORE).await),
		("target", round_trip.target_query(ONE_MORE).await),
	] {
		let Err(error) = result else {
			panic!("the {side} accepted a value past the depth ceiling");
		};
		let error = format!("{error:#}");
		assert!(
			error.contains("nested too deeply") && error.contains(&MAX_VALUE_DEPTH.to_string()),
			"the {side} must refuse a value past the ceiling by naming the ceiling, got: {error}"
		);
	}

	// The refusal is the whole of what happened: the record is untouched and
	// still takes writes on both sides.
	for (side, result) in [
		("source", round_trip.source_query("UPDATE deep:1 SET tag = 'kept' RETURN NONE").await),
		("target", round_trip.target_query("UPDATE deep:1 SET tag = 'kept' RETURN NONE").await),
	] {
		result
			.map_err(|e| e.context(format!("a shallow write to the {side} after the refusal")))?;
	}
	Ok(())
}

/// A view restores whichever side of its source table it sorts on.
///
/// A dump is replayed in order and a view's `DEFINE TABLE` recomputes it from
/// tables that must already exist, so a view whose name sorts before the table
/// it selects from is the case an export ordered by name gets wrong. The whole
/// restore still completes when it does: the view is one statement of many, and
/// what is lost is a definition rather than a record. The fixture carries both
/// orders so a restore that satisfies one by accident cannot satisfy both.
#[tokio::test]
async fn a_view_restores_on_either_side_of_its_source_table() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 CREATE person:1 SET age = 17;
		 CREATE person:2 SET age = 17;
		 DEFINE TABLE aa_view AS SELECT age, count() AS c FROM person GROUP BY age;
		 DEFINE TABLE zz_view AS SELECT age, count() AS c FROM person GROUP BY age;",
	)
	.await?;

	round_trip.assert_all().await?;
	// Both views are populated, so the comparison above ran on rows rather than
	// on two empty tables.
	let (_, target) = both(&round_trip, "SELECT VALUE c FROM aa_view").await?;
	assert_eq!(target.to_sql(), "[2]", "the restored view must hold the count of its source");

	// A view's rows are carried like any other table's, ahead of the definition
	// that recomputes them. `RoundTrip::run` has already asserted that every
	// statement of the replay applied, which is what says those rows did not
	// collide with the ones the definition wrote over them.
	assert_eq!(
		data_sections(round_trip.export_a()),
		["person", "aa_view", "zz_view"],
		"the export must carry records for the views as well as the stored table:\n{}",
		round_trip.export_a()
	);
	Ok(())
}

/// A restored view is still derived from its source, not a snapshot of it.
///
/// A view that failed to restore leaves a plain table holding the rows the dump
/// put there, which answers every read exactly as the view did — until the
/// source changes, at which point the table keeps answering with the old rows
/// and nothing reports it. Comparing the two databases cannot see this: a
/// snapshot matches the view it was taken from. Only a write after the restore
/// separates them.
#[tokio::test]
async fn a_restored_view_still_tracks_its_source() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 CREATE person:1 SET age = 17;
		 CREATE person:2 SET age = 17;
		 DEFINE TABLE aa_view AS SELECT age, count() AS c FROM person GROUP BY age;
		 DEFINE TABLE zz_view AS SELECT age, count() AS c FROM person GROUP BY age;",
	)
	.await?;
	round_trip.assert_all().await?;

	round_trip.target_query("CREATE person:3 SET age = 17 RETURN NONE").await?;
	for view in ["aa_view", "zz_view"] {
		let sql = format!("SELECT VALUE c FROM {view}");
		let values = round_trip.target_query(&sql).await?;
		assert_eq!(
			values[0].to_sql(),
			"[3]",
			"`{view}` did not follow a write to the table it selects from, so the restore left a \
			 table holding the rows the source had rather than a view"
		);
	}
	Ok(())
}

/// A view over a view restores after the view it reads.
///
/// Ordering the views after the stored tables is not enough on its own: a view
/// selecting from another view constrains the order among the views themselves,
/// and the constraint runs opposite to the name order here. It is the same
/// silent failure as a view ordered before its stored source, one level up.
#[tokio::test]
async fn a_view_over_a_view_restores_after_the_view_it_reads() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE person SCHEMALESS;
		 CREATE person:1 SET age = 17;
		 CREATE person:2 SET age = 21;
		 DEFINE TABLE zz_by_age AS SELECT age, count() AS c FROM person GROUP BY age;
		 DEFINE TABLE aa_totals AS SELECT c, count() AS n FROM zz_by_age GROUP BY c;",
	)
	.await?;

	round_trip.assert_all().await?;
	let (_, target) = both(&round_trip, "SELECT VALUE n FROM aa_totals").await?;
	assert_eq!(
		target.to_sql(),
		"[2]",
		"the restored view over a view must hold the rows its own source view has"
	);
	Ok(())
}

/// An edge restores ahead of the endpoint records it names.
///
/// Records are replayed in table-name order, so an edge table that sorts first
/// is inserted while the records its `in` and `out` name do not exist. A
/// restore has to accept that — a dump has no order in which every reference is
/// already satisfied, since two tables can point at each other — while leaving
/// the constraint in force afterwards. Accepting it by dropping the constraint
/// would pass a records comparison and lose the enforcement instead.
#[tokio::test]
async fn an_edge_restores_ahead_of_its_endpoint_records() -> Result<()> {
	let round_trip = RoundTrip::run(
		"DEFINE TABLE zz_person SCHEMALESS;
		 DEFINE TABLE aa_edge TYPE RELATION IN zz_person OUT zz_person ENFORCED;
		 CREATE zz_person:1;
		 CREATE zz_person:2;
		 RELATE zz_person:1 -> aa_edge:e1 -> zz_person:2;",
	)
	.await?;

	round_trip.assert_all().await?;
	// The fixture only tests what it claims while the edge's records really do
	// precede its endpoints' in the dump.
	assert_eq!(
		data_sections(round_trip.export_a()),
		["aa_edge", "zz_person"],
		"the export must carry the edge's records before its endpoints':\n{}",
		round_trip.export_a()
	);

	// The graph is traversable from both ends, which is what the edge exists
	// for and what a record comparison on its own does not establish.
	let (source, target) =
		both(&round_trip, "SELECT VALUE ->aa_edge->zz_person FROM zz_person:1").await?;
	assert_eq!(target.to_sql(), source.to_sql(), "the restored edge must traverse forwards");
	assert_eq!(target.to_sql(), "[[zz_person:2]]", "and reach the endpoint the fixture related");
	let (source, target) =
		both(&round_trip, "SELECT VALUE <-aa_edge<-zz_person FROM zz_person:2").await?;
	assert_eq!(target.to_sql(), source.to_sql(), "the restored edge must traverse backwards");

	// And the enforcement the restore had to set aside is in force again: an
	// edge to a record that does not exist is refused on both sides.
	for (side, result) in [
		(
			"source",
			round_trip.source_query("RELATE zz_person:2 -> aa_edge:e2 -> zz_person:9").await,
		),
		(
			"target",
			round_trip.target_query("RELATE zz_person:2 -> aa_edge:e2 -> zz_person:9").await,
		),
	] {
		assert!(
			result.is_err(),
			"the {side} accepted an enforced edge to a record that does not exist"
		);
	}
	Ok(())
}
