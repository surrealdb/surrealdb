//! What an export carries for a table whose record id names its components.
//!
//! `DEFINE FIELD id ON reading TYPE [city: string, time: datetime]` is the one
//! definition of `city` and `time`: they are derived from the id on every read
//! and never stored. The dump therefore holds the `id` definition and the
//! records, and no definition and no stored value of a component, and a restore
//! of it rebuilds the same table.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use crate::dbs::Session;
use crate::kvs::Datastore;

const DEFINITIONS: &str = "DEFINE NAMESPACE test;
	DEFINE DATABASE test;
	DEFINE TABLE reading SCHEMAFULL;
	DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
	DEFINE FIELD temp ON reading TYPE int;
	CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4;
	CREATE reading:['Paris', d'2024-01-03T00:00:00Z'] SET temp = 7;";

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

/// The `id` is the only definition of the components in the dump, and their
/// values appear nowhere but in the record ids.
#[tokio::test]
async fn the_dump_defines_the_id_and_none_of_its_components() {
	let (ds, ses) = seeded().await;
	let dump = export_text(&ds, &ses).await;

	assert!(
		dump.contains("DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime]"),
		"{dump}"
	);
	assert!(dump.contains("DEFINE FIELD OVERWRITE temp ON reading TYPE int"), "{dump}");
	assert_eq!(dump.matches("DEFINE FIELD OVERWRITE").count(), 2, "{dump}");
	for component in ["city", "time"] {
		assert!(
			!dump.contains(&format!("DEFINE FIELD OVERWRITE {component} ")),
			"a component is defined by the id, not by a field of its own:\n{dump}"
		);
	}

	let data = &dump[dump.find("-- TABLE DATA: reading").expect(&dump)..];
	assert!(data.contains("reading:['London'"), "{dump}");
	assert!(data.contains("reading:['Paris'"), "{dump}");
	for component in ["city", "time"] {
		assert!(!data.contains(component), "a component is not a stored value:\n{dump}");
	}
}

/// The dump replays, and the table it rebuilds reads the components of every
/// record and describes them as the original did.
#[tokio::test]
async fn the_dump_replays_into_the_same_table() {
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

	for sql in [
		"SELECT city, time, temp FROM reading ORDER BY id",
		"SELECT * FROM reading ORDER BY id",
		"INFO FOR TABLE reading STRUCTURE",
	] {
		let original = run(&ds, &ses, sql).await;
		let replayed = run(&restored, &ses, sql).await;
		assert_eq!(original, replayed, "{sql}");
	}
	assert_eq!(
		surrealdb_types::ToSql::to_sql(
			&run(&restored, &ses, "SELECT VALUE city FROM reading ORDER BY id").await
		),
		"['London', 'Paris']"
	);
}
