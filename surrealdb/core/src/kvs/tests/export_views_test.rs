//! What an export carries for a view, and why it carries it whether or not the
//! definition will replay.
//!
//! A view is emitted as the same table without its `AS SELECT`, then its
//! records, then the definition. The three statements settle every outcome
//! between them: a definition that replays begins by clearing the table, so its
//! recomputed rows replace the ones the dump wrote and nothing collides, and one
//! that fails takes its own writes back with it, leaving the plain table and
//! those rows behind under that name.
//!
//! The plain table is emitted schemaless, whatever the view's own mode, and its
//! fields are left to the structure that follows the records. A view's rows
//! never passed through its own field definitions — maintenance runs the
//! table's triggers and nothing else — so a table carrying them would refuse
//! the rows it exists to keep, and a `REFERENCE` among them would write a
//! backlink into another table that the exported database never had.
//!
//! It is unconditional because whether a definition replays is not a property of
//! the database being exported. Three reasons it will not are visible to the
//! exporter and are reported in the dump:
//!
//! - the aggregation analysis no longer accepts the clauses, so the stored view degrades to the
//!   inert [`ViewDefinition::Select`] and the same analysis rejects the `DEFINE TABLE` that would
//!   recreate it;
//! - the views form a definition cycle, which `DEFINE TABLE OVERWRITE` can create and no emission
//!   order satisfies;
//! - a source table is one this dump does not define.
//!
//! Others belong to the target alone — recomputing a view is one transaction, so
//! a target enforcing `transaction_max_write_keys` rejects a view whose rows
//! outgrow the limit even where the source built them through writes that were
//! each within it. That case is pinned in `core/tests/it/export.rs`, where the
//! limit can be armed; what the tests here hold is the shape that makes it
//! survivable.
//!
//! The inert fixture cannot be written in SurrealQL — `DEFINE TABLE` runs the
//! same analysis before storing anything, so this version refuses to create the
//! view it degrades on reading — so it is built by storing the rejected clauses
//! directly, and the read back is asserted to really degrade rather than merely
//! be labelled.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_rpc::export::{Config as ExportConfig, TableConfig};

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{TableDefinition, ViewDefinition};
use crate::dbs::Session;
use crate::expr::Fields;
use crate::expr::field::Selector;
use crate::kvs::Datastore;
use crate::val::TableName;

async fn ds() -> Arc<Datastore> {
	Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap()
}

async fn export_text(ds: &Datastore, ses: &Session) -> String {
	export_text_with_config(ds, ses, ExportConfig::default()).await
}

async fn export_text_with_config(ds: &Datastore, ses: &Session, cfg: ExportConfig) -> String {
	let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
	let task = ds.export_with_config(ses, tx, cfg).await.unwrap();
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await.unwrap();
	String::from_utf8(collector.await.unwrap()).unwrap()
}

/// Asserts the dump lays `table` out as plain table, then records, then the
/// view definition.
///
/// That order is what settles both restores: the plain table makes the name
/// exist whether or not a row follows, and a definition that replays clears it
/// before recomputing, while one that fails rolls back onto it.
fn assert_fallback_then_records_then_definition(dump: &str, table: &str) {
	let fallback = dump
		.find(&format!("DEFINE TABLE {table} "))
		.unwrap_or_else(|| panic!("the dump defines no plain `{table}`:\n{dump}"));
	let data = dump
		.find(&format!("-- TABLE DATA: {table}"))
		.unwrap_or_else(|| panic!("the dump carries no records for `{table}`:\n{dump}"));
	let view = dump
		.rfind(&format!("DEFINE TABLE {table} "))
		.unwrap_or_else(|| panic!("the dump carries no definition for `{table}`:\n{dump}"));
	assert!(
		dump[fallback..data].find("AS SELECT").is_none(),
		"the first `DEFINE TABLE {table}` must be the plain one, so the name exists before \
		 anything that can fail:\n{dump}"
	);
	assert!(
		dump[view..].starts_with(&format!("DEFINE TABLE {table} "))
			&& dump[view..].contains("AS SELECT"),
		"the last `DEFINE TABLE {table}` must be the view:\n{dump}"
	);
	assert!(
		fallback < data && data < view,
		"`{table}` must be laid out as plain table, records, then view definition, or a replay \
		 that succeeds collides with the records and one that fails takes them with it:\n{dump}"
	);
}

/// Replays `dump` into a fresh datastore prepared by `prepare`, returning the
/// datastore and the messages of every statement the import rejected.
async fn restore(dump: &str, prepare: &str) -> (Arc<Datastore>, Session, Vec<String>) {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds.execute(prepare, &ses, None).await.unwrap() {
		result.result.unwrap();
	}
	let errors = ds
		.import(dump, &ses)
		.await
		.unwrap()
		.into_iter()
		.filter_map(|r| r.result.err().map(|e| e.to_string()))
		.collect();
	(ds, ses, errors)
}

/// The `field` of every row of `table`, rendered as SurrealQL.
async fn rows(ds: &Datastore, ses: &Session, table: &str, field: &str) -> String {
	let value = ds
		.execute(&format!("SELECT VALUE {field} FROM {table} ORDER BY id"), ses, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	surrealdb_types::ToSql::to_sql(&value)
}

/// The names of the tables the catalog holds.
async fn table_names(ds: &Datastore, ses: &Session) -> Vec<String> {
	let info = ds.execute("INFO FOR DB", ses, None).await.unwrap().remove(0).result.unwrap();
	let surrealdb_types::Value::Object(info) = info else {
		panic!("`INFO FOR DB` must answer with an object");
	};
	let Some(surrealdb_types::Value::Object(tables)) = info.get("tables") else {
		panic!("`INFO FOR DB` must carry a `tables` block");
	};
	tables.keys().cloned().collect()
}

/// Seeds a source table and a maintained aggregate over it, then rewrites the
/// view's stored clauses to a shape the analysis rejects — a `VALUE` selector
/// under a `GROUP`, which `view_to_definition` refuses outright.
///
/// Returns the datastore with `v` holding the rows the aggregate computed and a
/// definition that now reads back as inert.
async fn datastore_with_an_inert_view() -> (Arc<Datastore>, Session) {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");

	for result in ds
		.execute(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE src SCHEMALESS;
			 DEFINE TABLE v AS SELECT count() AS c FROM src GROUP ALL;
			 CREATE src:1 SET n = 1;",
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}

	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let before = txn
		.get_tb(db.namespace_id, db.database_id, &TableName::from("v"), None)
		.await
		.unwrap()
		.unwrap();
	let ViewDefinition::Aggregated {
		fields,
		tables,
		condition,
		groups,
		..
	} = before.view.clone().unwrap()
	else {
		panic!("the seeded view must be classified as an aggregate: {:?}", before.view);
	};
	// The same aggregate, selected through `VALUE` — a shape `DEFINE TABLE`
	// rejects, so the clauses store and then fail to classify on the way out.
	let selector = fields.iter_non_all_fields().next().unwrap().expr.clone();
	let rejected = TableDefinition {
		view: Some(ViewDefinition::Select {
			fields: Fields::Value(Box::new(Selector {
				expr: selector,
				alias: None,
			})),
			tables,
			condition,
			groups: Some(groups),
		}),
		..(*before).clone()
	};
	txn.replace_tb("test", "test", &rejected).await.unwrap();
	txn.commit().await.unwrap();

	(ds, ses)
}

/// The fixture is inert because the analysis says so, not because it was
/// labelled: reading the stored clauses back yields the degraded
/// classification, and the version that stored them refuses to define them.
#[tokio::test]
async fn the_fixture_degrades_on_read_and_cannot_be_redefined() {
	let (ds, ses) = datastore_with_an_inert_view().await;

	let txn = ds.transaction(Write).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let after = txn
		.get_tb(db.namespace_id, db.database_id, &TableName::from("v"), None)
		.await
		.unwrap()
		.unwrap();
	txn.cancel().await.unwrap();
	assert!(
		matches!(after.view, Some(ViewDefinition::Select { .. })),
		"the stored clauses must read back as the inert classification, got {:?}",
		after.view
	);

	// Replaying the definition raises the rejection rather than writing rows,
	// which is why the rows cannot come back any other way.
	let refused = ds
		.execute("DEFINE TABLE OVERWRITE v AS SELECT VALUE count() FROM src GROUP ALL;", &ses, None)
		.await
		.unwrap()
		.remove(0)
		.result;
	assert!(refused.is_err(), "this version must refuse to define the shape it degrades on");
}

/// An inert view's rows are carried by the export, because nothing on the
/// restoring side can produce them.
#[tokio::test]
async fn an_inert_views_records_are_exported() {
	let (ds, ses) = datastore_with_an_inert_view().await;

	let seeded =
		ds.execute("SELECT VALUE c FROM v", &ses, None).await.unwrap().remove(0).result.unwrap();
	assert_eq!(
		surrealdb_types::ToSql::to_sql(&seeded),
		"[1]",
		"the fixture must hold the row the aggregate computed"
	);

	let dump = export_text(&ds, &ses).await;
	assert!(
		dump.contains("INSERT [ { c: 1, id: v:"),
		"an inert view's records are the only copy of its state, so the dump must carry \
		 them:\n{dump}"
	);
	assert_fallback_then_records_then_definition(&dump, "v");
	assert!(
		dump.contains("-- NOTE: Table 'v' is a view this version can no longer maintain"),
		"the dump must say why it carries them, because the warning log belongs to the server \
		 and the operator reads the file:\n{dump}"
	);
}

/// A view whose source table this dump leaves out carries its records too. The
/// definition reads a table the restore may not have, so the rows are the only
/// thing that certainly survives.
#[tokio::test]
async fn a_view_whose_source_is_not_in_the_dump_carries_its_records() {
	let (ds, ses) = datastore_with_a_maintained_view().await;

	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned()]),
			..Default::default()
		},
	)
	.await;
	assert!(
		!dump.contains("DEFINE TABLE src "),
		"the fixture only holds if the source really is left out:\n{dump}"
	);
	assert!(
		dump.contains("INSERT [ { c: 1, id: v:"),
		"a view whose source the dump leaves out must carry its records:\n{dump}"
	);
	assert_fallback_then_records_then_definition(&dump, "v");
	assert!(
		dump.contains("-- NOTE: Table 'v' is a view over 'src', which this export does not carry"),
		"the dump must name the source it leaves out:\n{dump}"
	);
}

/// Restored where the source is absent, the definition fails and takes its own
/// writes back with it, so the exported rows are what is left.
#[tokio::test]
async fn a_restore_without_the_source_keeps_the_rows() {
	let (ds, ses) = datastore_with_a_maintained_view().await;
	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned()]),
			..Default::default()
		},
	)
	.await;

	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(
		errors.len(),
		1,
		"the definition is the one statement that cannot replay, and it must say so: {errors:?}"
	);
	assert!(
		errors[0].contains("src"),
		"the rejected statement must name the missing source: {errors:?}"
	);
	assert_eq!(
		rows(&target, &target_ses, "v", "c").await,
		"[1]",
		"the rows the definition could not recompute must survive it"
	);
}

/// Restored where the source is present, the definition succeeds — clearing the
/// rows this dump wrote and recomputing them from the target's own data, with
/// nothing rejected.
#[tokio::test]
async fn a_restore_with_the_source_recomputes_over_the_rows() {
	let (ds, ses) = datastore_with_a_maintained_view().await;
	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned()]),
			..Default::default()
		},
	)
	.await;

	// Two source records rather than the one the dump was taken over, so a
	// recomputation is distinguishable from the rows the dump carries.
	let (target, target_ses, errors) = restore(
		&dump,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE src SCHEMALESS;
		 CREATE src:1 SET n = 1;
		 CREATE src:2 SET n = 2;",
	)
	.await;
	assert!(errors.is_empty(), "the records must not collide with the recomputation: {errors:?}");
	assert_eq!(
		rows(&target, &target_ses, "v", "c").await,
		"[2]",
		"the definition must recompute from the target's rows, replacing the dump's"
	);
}

/// Seeds `src` and a maintained aggregate `v` over it, with one source record.
async fn datastore_with_a_maintained_view() -> (Arc<Datastore>, Session) {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds
		.execute(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE src SCHEMALESS;
			 DEFINE TABLE v AS SELECT count() AS c FROM src GROUP ALL;
			 CREATE src:1 SET n = 1;",
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}
	(ds, ses)
}

/// A view the dump can replay carries its records all the same, and says
/// nothing about them: there is no reason to report, and the rows are there for
/// the reasons only the target knows.
#[tokio::test]
async fn a_replayable_view_carries_its_records_without_a_note() {
	let (ds, ses) = datastore_with_a_maintained_view().await;

	let dump = export_text(&ds, &ses).await;
	assert_fallback_then_records_then_definition(&dump, "v");
	assert!(
		!dump.contains("-- NOTE:"),
		"nothing here can name a reason this definition will not replay, so the dump must not \
		 claim one:\n{dump}"
	);

	// And replaying it is clean: the definition clears the rows the dump wrote
	// before recomputing its own, so they never collide.
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert!(errors.is_empty(), "a replayable view must restore without a rejection: {errors:?}");
	assert_eq!(
		rows(&target, &target_ses, "v", "c").await,
		"[1]",
		"and it must hold the rows its own definition recomputed"
	);
}

/// A view with no rows still restores as a table. Nothing else in the dump
/// creates the name — the records would have, and there are none — so without
/// the plain table the restore drops the view outright.
#[tokio::test]
async fn an_empty_unreplayable_view_still_restores_as_a_table() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds
		.execute(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE src SCHEMALESS;
			 DEFINE TABLE v AS SELECT n FROM src;",
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}

	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned()]),
			..Default::default()
		},
	)
	.await;
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(errors.len(), 1, "only the view definition may be rejected: {errors:?}");
	assert!(
		table_names(&target, &target_ses).await.contains(&"v".to_owned()),
		"a view holding no rows must still restore as a table"
	);
}

/// A view reading a view whose definition cannot replay restores too, because
/// the name it reads is there as a plain table.
///
/// Its own sources are all in the dump, so nothing marks it unreplayable and it
/// carries no records of its own: what saves it is that the table it reads
/// exists to be recomputed from. The source holds no rows, which is the shape
/// that makes the loss cascade — with rows, `v`'s own `INSERT` would create the
/// name and `v2` would replay whether or not anything else defined it.
#[tokio::test]
async fn a_view_reading_an_unreplayable_view_still_restores() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds
		.execute(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE src SCHEMALESS;
			 DEFINE TABLE v AS SELECT n FROM src;
			 DEFINE TABLE v2 AS SELECT n FROM v;",
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}

	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned(), "v2".to_owned()]),
			..Default::default()
		},
	)
	.await;
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(
		errors.len(),
		1,
		"only `v`'s own definition may be rejected — a rejection for `v2` means the loss \
		 cascaded: {errors:?}"
	);
	let names = table_names(&target, &target_ses).await;
	assert!(names.contains(&"v2".to_owned()), "the dependent view must restore: {names:?}");

	// And it restored as a view rather than as a frozen table: a row written to
	// the table it reads produces a row in it, with nothing asking for that.
	target
		.execute("CREATE v:1 SET n = 7", &target_ses, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(
		rows(&target, &target_ses, "v2", "n").await,
		"[7]",
		"the dependent view must still be maintained from the table it reads"
	);
}

/// Views that define a cycle restore with their tables and their rows.
///
/// The catalog stores a cycle — `DEFINE TABLE OVERWRITE` can point a view's own
/// source back at it — and no emission order satisfies one: whichever member
/// comes first reads a name the dump has not defined yet. Every member is
/// therefore treated as unreplayable, which is what keeps the failure to one
/// definition instead of taking both tables and both sets of rows with it.
#[tokio::test]
async fn a_view_cycle_restores_with_its_tables_and_rows() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds
		.execute(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE b SCHEMALESS;
			 CREATE b:1 SET n = 1;
			 DEFINE TABLE a AS SELECT n FROM b;
			 DEFINE TABLE OVERWRITE b AS SELECT n FROM a;",
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}

	let dump = export_text(&ds, &ses).await;
	for table in ["a", "b"] {
		assert_fallback_then_records_then_definition(&dump, table);
	}

	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(
		errors.len(),
		1,
		"only the member emitted before its source may be rejected — two means the cycle took \
		 both tables with it: {errors:?}"
	);
	let names = table_names(&target, &target_ses).await;
	assert!(
		names.contains(&"a".to_owned()) && names.contains(&"b".to_owned()),
		"both members of the cycle must restore: {names:?}"
	);
	for table in ["a", "b"] {
		assert_eq!(
			rows(&target, &target_ses, table, "n").await,
			"[1]",
			"`{table}` must restore with its rows"
		);
	}
}

/// The plain table a view is emitted as is schemaless, whatever the view's own
/// mode, so that every row the view holds can be written back to it.
///
/// A view's rows never passed through its own field definitions — maintenance
/// runs the table's triggers and nothing else — so a schemafull plain table
/// would refuse the rows it exists to keep. What a fallback restore is left
/// holding is therefore a schemaless table carrying the view's fields and its
/// rows; a definition that replays supersedes all of it.
#[tokio::test]
async fn a_schemafull_views_records_restore_against_a_schemaless_plain_table() {
	let (ds, ses) = datastore_with_a_schemafull_view().await;
	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned()]),
			..Default::default()
		},
	)
	.await;

	let fallback = dump.find("DEFINE TABLE v ").unwrap();
	let data = dump.find("-- TABLE DATA: v").unwrap();
	assert!(
		dump[fallback..data].contains("SCHEMALESS"),
		"the plain table must be schemaless, or it refuses the records that follow it:\n{dump}"
	);
	assert!(
		dump[data..].contains("SCHEMAFULL AS SELECT"),
		"the definition must still carry the view's own mode:\n{dump}"
	);

	// Restored without the source, the definition fails and the dump's own rows
	// are what the table is left holding, under the view's fields.
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(
		errors.len(),
		1,
		"the definition is the one statement that cannot replay, and it must say so: {errors:?}"
	);
	assert_eq!(
		rows(&target, &target_ses, "v", "n").await,
		"[7]",
		"a schemafull view's records must survive a definition that cannot replay"
	);
	assert!(
		fields_of(&target, &target_ses, "v").await.contains("DEFINE FIELD n ON v TYPE int"),
		"the fields must be stated on the table the rows are left on"
	);

	// Restored with it, the definition recomputes over those rows and the table
	// is the schemafull view it was exported as.
	let (target, target_ses, errors) = restore(
		&dump,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE src SCHEMALESS;
		 CREATE src:1 SET n = 7;",
	)
	.await;
	assert!(errors.is_empty(), "the records must not collide with the recomputation: {errors:?}");
	assert_eq!(
		rows(&target, &target_ses, "v", "n").await,
		"[7]",
		"the definition must recompute the view from the target's own rows"
	);
	assert!(
		fields_of(&target, &target_ses, "v").await.contains("DEFINE FIELD n ON v TYPE int"),
		"the fields must outlive the definition that wiped them"
	);
}

/// A view's rows are written back as they were stored, without the side effects
/// its field definitions would have had on a write.
///
/// A `REFERENCE` field writes its backlink under the table it points at, which
/// no `DEFINE TABLE ... AS SELECT` clears — it wipes the view's own key range
/// and nothing else. View maintenance writes no such backlink, so one written
/// by the records this dump carries would be a link the exported database never
/// had, left behind for good.
#[tokio::test]
async fn a_views_records_restore_without_the_backlinks_its_fields_would_write() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for stmt in [
		"DEFINE NAMESPACE test;",
		"DEFINE DATABASE test;",
		"DEFINE TABLE t SCHEMALESS;",
		"DEFINE FIELD back ON t COMPUTED <~v;",
		"CREATE t:1;",
		"DEFINE TABLE src SCHEMALESS;",
		"CREATE src:1 SET r = t:1;",
		"DEFINE TABLE v SCHEMALESS AS SELECT r FROM src;",
		"DEFINE FIELD r ON v TYPE record<t> REFERENCE;",
	] {
		ds.execute(stmt, &ses, None).await.unwrap().remove(0).result.unwrap();
	}
	assert_eq!(
		rows(&ds, &ses, "t", "back").await,
		"[[]]",
		"maintaining a view writes no backlink, whatever its fields declare"
	);

	let dump = export_text(&ds, &ses).await;
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert!(errors.is_empty(), "the dump must replay whole: {errors:?}");
	assert_eq!(
		rows(&target, &target_ses, "v", "r").await,
		"[t:1]",
		"the view must restore holding the row it held"
	);
	assert_eq!(
		rows(&target, &target_ses, "t", "back").await,
		"[[]]",
		"restoring the view's records must not write a backlink the source never had, which the \
		 definition that follows them does not clear"
	);
}

/// Seeds `src` and a `SCHEMAFULL` view `v` over it carrying one declared field,
/// with one source record.
async fn datastore_with_a_schemafull_view() -> (Arc<Datastore>, Session) {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds
		.execute(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE src SCHEMALESS;
			 DEFINE TABLE v SCHEMAFULL AS SELECT n FROM src;
			 DEFINE FIELD n ON v TYPE int;
			 CREATE src:1 SET n = 7;",
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}
	(ds, ses)
}

/// The field definitions the catalog holds for `table`, rendered as SurrealQL.
async fn fields_of(ds: &Datastore, ses: &Session, table: &str) -> String {
	let info = ds
		.execute(&format!("INFO FOR TABLE {table}"), ses, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	let surrealdb_types::Value::Object(info) = info else {
		panic!("`INFO FOR TABLE` must answer with an object");
	};
	let Some(fields) = info.get("fields") else {
		panic!("`INFO FOR TABLE` must carry a `fields` block");
	};
	surrealdb_types::ToSql::to_sql(fields)
}

/// A `DROP` view's records restore, because the plain table the dump defines
/// does not carry the `DROP`.
///
/// `DROP` discards every record written to the table and reports nothing, so a
/// fallback that kept it would restore empty with no rejected statement for the
/// import to name — the one loss here that leaves no trace at all.
#[tokio::test]
async fn a_dropped_views_records_restore_against_a_plain_table() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for stmt in [
		"DEFINE NAMESPACE test;",
		"DEFINE DATABASE test;",
		"DEFINE TABLE src SCHEMALESS;",
		"CREATE src:1 SET n = 1;",
		"DEFINE TABLE v DROP AS SELECT n FROM src;",
	] {
		ds.execute(stmt, &ses, None).await.unwrap().remove(0).result.unwrap();
	}
	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned()]),
			..Default::default()
		},
	)
	.await;

	let data = dump.find("-- TABLE DATA: v").unwrap();
	assert!(
		!dump[..data].contains("DROP"),
		"the plain table must not carry the `DROP` that would discard its records:\n{dump}"
	);
	assert!(
		dump[data..].contains("DROP"),
		"the definition must still carry the view's own `DROP`:\n{dump}"
	);

	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(
		errors.len(),
		1,
		"the definition is the one statement that cannot replay, and it must say so: {errors:?}"
	);
	assert_eq!(
		rows(&target, &target_ses, "v", "n").await,
		"[1]",
		"a dropped view's records must survive a definition that cannot replay"
	);
}

/// A `TYPE RELATION` view's records restore, because the plain table the dump
/// defines is `TYPE ANY`.
///
/// A relation table rejects what a plain `INSERT` writes it, even a record
/// carrying `in` and `out`. `ANY` takes both, and takes them as the rows they
/// were: writing a record with `in` and `out` into it stores those as fields
/// and writes no edge, which is what maintaining the view did too.
#[tokio::test]
async fn a_relation_views_records_restore_against_a_plain_table() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for stmt in [
		"DEFINE NAMESPACE test;",
		"DEFINE DATABASE test;",
		"DEFINE TABLE src SCHEMALESS;",
		"DEFINE TABLE other SCHEMALESS;",
		"CREATE other:1;",
		"CREATE src:1 SET in = other:1, out = other:1;",
		"DEFINE TABLE v TYPE RELATION AS SELECT in, out FROM src;",
	] {
		ds.execute(stmt, &ses, None).await.unwrap().remove(0).result.unwrap();
	}
	assert_eq!(
		rows(&ds, &ses, "other", "->v").await,
		"[[]]",
		"maintaining a relation view writes no edge, whatever its table type declares"
	);
	let dump = export_text_with_config(
		&ds,
		&ses,
		ExportConfig {
			tables: TableConfig::Some(vec!["v".to_owned(), "other".to_owned()]),
			..Default::default()
		},
	)
	.await;

	let data = dump.find("-- TABLE DATA: v").unwrap();
	let fallback = dump[..data].rfind("DEFINE TABLE v ").unwrap();
	assert!(
		dump[fallback..data].contains("TYPE ANY"),
		"the plain table must be `TYPE ANY`, or it rejects the records that follow it:\n{dump}"
	);
	assert!(
		dump[data..].contains("TYPE RELATION"),
		"the definition must still carry the view's own type:\n{dump}"
	);

	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert_eq!(
		errors.len(),
		1,
		"the definition is the one statement that cannot replay, and it must say so: {errors:?}"
	);
	assert_eq!(
		rows(&target, &target_ses, "v", "out").await,
		"[other:1]",
		"a relation view's records must survive a definition that cannot replay"
	);
	assert_eq!(
		rows(&target, &target_ses, "other", "->v").await,
		"[[]]",
		"restoring those records must not write an edge the source never had"
	);
}

/// A `REFERENCE` pointing *at* a view's records survives the restore.
///
/// The backlink is written under the referenced table's key range — the same
/// range a view's `DEFINE TABLE ... AS SELECT` clears before recomputing — and
/// it is the referencing table's records that write it.
#[tokio::test]
async fn a_reference_into_a_views_records_restores() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for stmt in [
		"DEFINE NAMESPACE test;",
		"DEFINE DATABASE test;",
		"DEFINE TABLE src SCHEMALESS;",
		"CREATE src:1 SET n = 1;",
		"DEFINE TABLE v SCHEMALESS AS SELECT n FROM src;",
		"DEFINE TABLE t SCHEMALESS;",
		"DEFINE FIELD r ON t TYPE record<v> REFERENCE;",
		"CREATE t:1 SET r = v:1;",
	] {
		ds.execute(stmt, &ses, None).await.unwrap().remove(0).result.unwrap();
	}
	assert_eq!(
		rows(&ds, &ses, "v", "<~t").await,
		"[[t:1]]",
		"the exported database answers the back-reference"
	);

	let dump = export_text(&ds, &ses).await;
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert!(errors.is_empty(), "the dump must replay whole: {errors:?}");
	assert_eq!(
		rows(&target, &target_ses, "t", "r").await,
		"[v:1]",
		"the referencing record restores"
	);
	assert_eq!(
		rows(&target, &target_ses, "v", "<~t").await,
		"[[t:1]]",
		"the backlink must survive the view definition that follows the record writing it"
	);
}

/// A graph edge pointing *at* a view's record survives the restore.
///
/// An edge writes a key under each end's table, so one end lands in the view's
/// key range — which its `DEFINE TABLE ... AS SELECT` clears.
#[tokio::test]
async fn an_edge_into_a_views_records_restores() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	for stmt in [
		"DEFINE NAMESPACE test;",
		"DEFINE DATABASE test;",
		"DEFINE TABLE src SCHEMALESS;",
		"CREATE src:1 SET n = 1;",
		"DEFINE TABLE v SCHEMALESS AS SELECT n FROM src;",
		"DEFINE TABLE t SCHEMALESS;",
		"CREATE t:1;",
		"RELATE t:1->e->v:1;",
	] {
		ds.execute(stmt, &ses, None).await.unwrap().remove(0).result.unwrap();
	}
	assert_eq!(
		rows(&ds, &ses, "v", "count(<-e)").await,
		"[1]",
		"the exported database answers the inbound edge"
	);

	let dump = export_text(&ds, &ses).await;
	let (target, target_ses, errors) =
		restore(&dump, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	assert!(errors.is_empty(), "the dump must replay whole: {errors:?}");
	assert_eq!(
		rows(&target, &target_ses, "t", "count(->e)").await,
		"[1]",
		"the outbound half restores"
	);
	assert_eq!(
		rows(&target, &target_ses, "v", "count(<-e)").await,
		"[1]",
		"the inbound half must survive the view definition that follows the edge writing it"
	);
}
