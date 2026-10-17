//! What an export carries for a field whose kind implies definitions for the
//! elements of the arrays and sets it admits, and that a restore of it rebuilds
//! the same table.
//!
//! `DEFINE FIELD v ON t TYPE set<float> | string` also defines `v.*` as `float`
//! (`field_rules::implied_definitions`). The dump writes that definition beside
//! `v`'s, as it writes every definition the table holds, because one a kind
//! implies can carry clauses of its own. A restore replays `v`, which defines
//! `v.*` again, then the dumped `v.*` over it, which is admitted because it is
//! what `v`'s kind implies there. The tables here hold kinds whose implied
//! definitions no single member of the union states: a member with no elements,
//! members whose elements differ, elements whose own kind is a union, and a
//! member with no element kind, which makes the elements `any`. Beside them are
//! kinds that imply nothing, because a member holds elements no array or set
//! member describes: an object, and a literal array.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use crate::dbs::Session;
use crate::kvs::Datastore;

const DEFINITIONS: &str = "DEFINE NAMESPACE test;
	DEFINE DATABASE test;
	DEFINE TABLE scalar_arm SCHEMAFULL;
	DEFINE FIELD v ON scalar_arm TYPE set<float> | string;
	ALTER FIELD v.* ON scalar_arm ASSERT $value >= 0;
	CREATE scalar_arm:set SET v = [2, 1.5];
	CREATE scalar_arm:text SET v = 'text';
	DEFINE TABLE collection_arms SCHEMAFULL;
	DEFINE FIELD v ON collection_arms TYPE array<int> | array<string>;
	CREATE collection_arms:ints SET v = [1, 2];
	CREATE collection_arms:strings SET v = ['a'];
	DEFINE TABLE union_elements SCHEMAFULL;
	DEFINE FIELD v ON union_elements TYPE array<float | array<bool>>;
	CREATE union_elements:1 SET v = [1.5, [true]];
	DEFINE TABLE untyped_arm SCHEMAFULL;
	DEFINE FIELD v ON untyped_arm TYPE array | array<int>;
	CREATE untyped_arm:mixed SET v = ['a', 1];
	CREATE untyped_arm:nested SET v = [{ x: 1 }];
	DEFINE TABLE object_arm SCHEMALESS;
	DEFINE FIELD v ON object_arm TYPE object | array<int>;
	CREATE object_arm:object SET v = { x: 1 };
	CREATE object_arm:ints SET v = [1, 2];
	DEFINE TABLE literal_arm SCHEMAFULL;
	DEFINE FIELD v ON literal_arm TYPE [int, string] | array<bool>;
	CREATE literal_arm:bools SET v = [true];";

const TABLES: [&str; 6] =
	["scalar_arm", "collection_arms", "union_elements", "untyped_arm", "object_arm", "literal_arm"];

async fn ds() -> Arc<Datastore> {
	Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap()
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

async fn export_text(ds: &Datastore, ses: &Session) -> String {
	let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
	let task = ds.export_with_config(ses, tx, Default::default()).await.unwrap();
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

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> surrealdb_types::Value {
	ds.execute(sql, ses, None).await.unwrap().remove(0).result.unwrap()
}

async fn seeded() -> (Arc<Datastore>, Session) {
	let ds = ds().await;
	let ses = session();
	for result in ds.execute(DEFINITIONS, &ses, None).await.unwrap() {
		result.result.unwrap();
	}
	(ds, ses)
}

/// The dump defines each field, then each definition its kind implies, with
/// the kind and clauses the table holds for it.
#[tokio::test]
async fn the_dump_defines_each_field_before_the_definitions_its_kind_implies() {
	let (ds, ses) = seeded().await;
	let dump = export_text(&ds, &ses).await;

	for (field, element) in [
		(
			"DEFINE FIELD OVERWRITE v ON scalar_arm TYPE set<float> | string ",
			"DEFINE FIELD OVERWRITE v.* ON scalar_arm TYPE float ASSERT $value >= 0 ",
		),
		(
			"DEFINE FIELD OVERWRITE v ON collection_arms TYPE array<int> | array<string> ",
			"DEFINE FIELD OVERWRITE v.* ON collection_arms TYPE int | string ",
		),
		(
			"DEFINE FIELD OVERWRITE v ON union_elements TYPE array<float | array<bool>> ",
			"DEFINE FIELD OVERWRITE v.*.* ON union_elements TYPE bool ",
		),
		(
			"DEFINE FIELD OVERWRITE v ON untyped_arm TYPE array | array<int> ",
			"DEFINE FIELD OVERWRITE v.* ON untyped_arm TYPE any ",
		),
	] {
		let field = dump.find(field).unwrap_or_else(|| panic!("no `{field}`:\n{dump}"));
		let element = dump.find(element).unwrap_or_else(|| panic!("no `{element}`:\n{dump}"));
		assert!(field < element, "a field is defined before what its kind implies:\n{dump}");
	}
}

/// A kind with a member holding elements no array or set member describes
/// implies nothing, so the dump defines the field alone.
#[tokio::test]
async fn the_dump_defines_no_element_definition_for_a_kind_that_implies_none() {
	let (ds, ses) = seeded().await;
	let dump = export_text(&ds, &ses).await;

	for (field, element) in [
		(
			"DEFINE FIELD OVERWRITE v ON object_arm TYPE object | array<int> ",
			"DEFINE FIELD OVERWRITE v.* ON object_arm ",
		),
		(
			"DEFINE FIELD OVERWRITE v ON literal_arm TYPE [int, string] | array<bool> ",
			"DEFINE FIELD OVERWRITE v.* ON literal_arm ",
		),
	] {
		assert!(dump.contains(field), "no `{field}`:\n{dump}");
		assert!(!dump.contains(element), "`{element}` is not implied:\n{dump}");
	}
}

/// The dump replays whole, and the tables it rebuilds hold the same
/// definitions and records, and refuse what the originals refuse.
#[tokio::test]
async fn the_dump_replays_into_the_same_tables() {
	let (ds, ses) = seeded().await;
	let dump = export_text(&ds, &ses).await;

	let restored = self::ds().await;
	for result in
		restored.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", &ses, None).await.unwrap()
	{
		result.result.unwrap();
	}
	let rejected: Vec<String> = restored
		.import(&dump, &ses)
		.await
		.unwrap()
		.into_iter()
		.filter_map(|response| response.result.err().map(|error| error.to_string()))
		.collect();
	assert!(rejected.is_empty(), "the dump must replay whole: {rejected:?}\n{dump}");

	for table in TABLES {
		for sql in [format!("INFO FOR TABLE {table}"), format!("SELECT * FROM {table} ORDER BY id")]
		{
			let original = run(&ds, &ses, &sql).await;
			let replayed = run(&restored, &ses, &sql).await;
			assert_eq!(original, replayed, "{sql}");
		}
	}

	// The element definition the dump carried still judges the elements, and
	// says nothing of a value of the member that has none. Where the kind
	// implies none, each member's values are judged by the kind alone.
	for (sql, refused) in [
		("CREATE scalar_arm:negative SET v = [-1]", true),
		("CREATE scalar_arm:other SET v = 'other'", false),
		("CREATE untyped_arm:text SET v = 'text'", true),
		("CREATE object_arm:text SET v = { x: 'text' }", false),
		("CREATE object_arm:mixed SET v = [1, 'text']", true),
		("CREATE literal_arm:pair SET v = [1, 'a']", false),
	] {
		for ds in [&ds, &restored] {
			let result = ds.execute(sql, &ses, None).await.unwrap().remove(0).result;
			assert_eq!(result.is_err(), refused, "{sql}: {result:?}");
		}
	}
}
