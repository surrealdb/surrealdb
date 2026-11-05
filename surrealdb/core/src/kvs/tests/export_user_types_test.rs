//! What an export carries for a database's types.
//!
//! The PostgreSQL front end defines an enum, a domain over it and a composite type, and a table
//! whose columns use them. The dump defines each type as `DEFINE TYPE`, each after the types it
//! names and all before the tables, and a restore of it rebuilds the same types and table.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use crate::dbs::Session;
use crate::dbs::capabilities::{Capabilities, Targets};
use crate::kvs::{Datastore, QueryRequest, QuerySource};

const PGSQL: &str = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
	CREATE DOMAIN posint AS integer NOT NULL DEFAULT 1 CONSTRAINT positive CHECK (VALUE > 0) CHECK (VALUE < 1000);
	CREATE DOMAIN feeling AS mood;
	CREATE TYPE address AS (street text, zip int);
	CREATE TABLE person (id int PRIMARY KEY, age posint, mood feeling, home address);
	INSERT INTO person (id, age, mood, home) VALUES (1, 30, 'ok', ROW('Main St', 12345)::address);";

async fn ds() -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all().with_experimental(Targets::All))
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.unwrap()
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

async fn export_text(ds: &Datastore, ses: &Session) -> String {
	export_text_with(ds, ses, Default::default()).await
}

async fn export_text_with(
	ds: &Datastore,
	ses: &Session,
	cfg: surrealdb_rpc::export::Config,
) -> String {
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

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> surrealdb_types::Value {
	ds.execute(sql, ses, None).await.unwrap().remove(0).result.unwrap()
}

async fn defined(ds: &Datastore, ses: &Session) {
	for result in
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", ses, None).await.unwrap()
	{
		result.result.unwrap();
	}
}

async fn seeded() -> (Arc<Datastore>, Session) {
	let ds = ds().await;
	let ses = session();
	defined(&ds, &ses).await;
	for result in ds.run(QueryRequest::new(QuerySource::pgsql(PGSQL), &ses)).await.unwrap() {
		result.result.unwrap();
	}
	(ds, ses)
}

/// `INFO FOR DB` lists each type by name as the SurrealQL that defines it.
#[tokio::test]
async fn info_for_db_lists_the_types() {
	let (ds, ses) = seeded().await;
	let info = run(&ds, &ses, "INFO FOR DB").await;
	let surrealdb_types::Value::Object(info) = info else {
		panic!("INFO FOR DB answers an object");
	};
	let types = surrealdb_types::ToSql::to_sql(info.get("types").unwrap());
	assert!(types.contains("DEFINE TYPE mood AS 'sad' | 'ok' | 'happy'"), "{types}");
	assert!(types.contains("DEFINE TYPE posint AS i32 CONSTRAINT positive ASSERT "), "{types}");
	assert!(types.contains("DEFINE TYPE feeling AS "), "{types}");
	assert!(types.contains("DEFINE TYPE address AS { "), "{types}");
}

/// The dump defines each type after the types it names, and all of them before the tables.
#[tokio::test]
async fn the_dump_defines_the_types_in_dependency_order_before_the_tables() {
	let (ds, ses) = seeded().await;
	let dump = export_text(&ds, &ses).await;
	let position = |needle: &str| dump.find(needle).unwrap_or_else(|| panic!("{needle}:\n{dump}"));
	let types = position("-- TYPES");
	let mood = position("DEFINE TYPE mood ");
	let feeling = position("DEFINE TYPE feeling ");
	let address = position("DEFINE TYPE address ");
	let table = position("DEFINE TABLE");
	assert!(types < mood && mood < feeling && types < address, "{dump}");
	assert!(feeling < table && address < table, "{dump}");
}

/// The dump replays, and the restored database describes the same types and table and holds the
/// same rows.
#[tokio::test]
async fn the_dump_replays_into_the_same_types() {
	let (ds, ses) = seeded().await;
	let dump = export_text(&ds, &ses).await;

	let restored = self::ds().await;
	defined(&restored, &ses).await;
	let rejected: Vec<String> = restored
		.import(&dump, &ses)
		.await
		.unwrap()
		.into_iter()
		.filter_map(|response| response.result.err().map(|error| error.to_string()))
		.collect();
	assert!(rejected.is_empty(), "the dump must replay whole: {rejected:?}\n{dump}");

	for sql in [
		"INFO FOR DB",
		"INFO FOR DB STRUCTURE",
		"(INFO FOR TABLE person).fields",
		"SELECT * FROM person ORDER BY id",
	] {
		let original = run(&ds, &ses, sql).await;
		let replayed = run(&restored, &ses, sql).await;
		assert_eq!(original, replayed, "{sql}");
	}
	assert_eq!(export_text(&restored, &ses).await, dump);
}

/// Definitions that name a type in a cast, beside a type whose permissions name another type.
const NAMES_IN_CLAUSES: &str = "DEFINE TYPE z AS int;
	DEFINE TYPE a AS string PERMISSIONS WHERE <z>1 = 1;
	DEFINE PARAM $p VALUE 1 PERMISSIONS WHERE <a>'x' = 'x';
	DEFINE ACCESS acc ON DATABASE TYPE RECORD AUTHENTICATE { IF <a>'x' = 'x' { RETURN $auth } };";

/// A restore of `dump` into a fresh database, answering the statements it refuses.
async fn replay_rejections(dump: &str, ses: &Session) -> Vec<String> {
	let restored = self::ds().await;
	defined(&restored, ses).await;
	restored
		.import(dump, ses)
		.await
		.unwrap()
		.into_iter()
		.filter_map(|response| response.result.err().map(|error| error.to_string()))
		.collect()
}

/// A type is defined before the access methods and parameters whose clauses name it, and after
/// the types its permissions name, so the dump replays whole.
#[tokio::test]
async fn the_dump_defines_the_types_before_every_definition_that_names_one() {
	let ds = ds().await;
	let ses = session();
	defined(&ds, &ses).await;
	for result in ds.execute(NAMES_IN_CLAUSES, &ses, None).await.unwrap() {
		result.result.unwrap();
	}
	let dump = export_text(&ds, &ses).await;
	let position = |needle: &str| dump.find(needle).unwrap_or_else(|| panic!("{needle}:\n{dump}"));
	assert!(position("DEFINE TYPE z ") < position("DEFINE TYPE a "), "{dump}");
	assert!(position("-- TYPES") < position("-- ACCESSES"), "{dump}");
	assert!(position("-- TYPES") < position("-- PARAMS"), "{dump}");
	// An access method's key is redacted in every export, so its definition is refused on
	// restore whatever it names; the rest of the dump must replay.
	let rejected: Vec<String> = replay_rejections(&dump, &ses)
		.await
		.into_iter()
		.filter(|error| !error.contains("redaction placeholder"))
		.collect();
	assert!(rejected.is_empty(), "the dump must replay whole: {rejected:?}\n{dump}");
}

/// An export of the parameters alone carries the types their clauses may name.
#[tokio::test]
async fn an_export_of_the_parameters_alone_carries_the_types() {
	let ds = ds().await;
	let ses = session();
	defined(&ds, &ses).await;
	for result in ds.execute(NAMES_IN_CLAUSES, &ses, None).await.unwrap() {
		result.result.unwrap();
	}
	let cfg = surrealdb_rpc::export::Config {
		database_definition: false,
		users: false,
		accesses: false,
		params: true,
		functions: false,
		analyzers: false,
		apis: false,
		buckets: false,
		modules: false,
		configs: false,
		tables: surrealdb_rpc::export::TableConfig::None,
		versions: false,
		records: false,
		sequences: false,
	};
	let dump = export_text_with(&ds, &ses, cfg).await;
	assert!(dump.contains("DEFINE TYPE a "), "{dump}");
	let rejected = replay_rejections(&dump, &ses).await;
	assert!(rejected.is_empty(), "the dump must replay whole: {rejected:?}\n{dump}");
}
