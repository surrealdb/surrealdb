//! The stored record formats through the whole engine: which bytes a table's
//! rows are written in, and that rows of every generation read as their schema
//! says in one table, before and after they are rewritten.
//!
//! The storage layer's own tests cover the envelope, the descriptors and the
//! packed rows one at a time; these tests cover what a statement leaves in the
//! keyspace and what the next statement reads back.

use surrealdb_types::ToSql;

use crate::exec::operators::test_util::TestDb;

/// The header byte's layout bits: 0 is a document, 1 to 3 are packed rows.
const LAYOUT_BITS: u8 = 0b0000_0111;

/// The stored values of table `tb`'s records, in key order. A record key
/// spells its table, a terminator, `*`, then the id.
async fn rows(db: &TestDb, tb: &str) -> Vec<Vec<u8>> {
	let mut needle = tb.as_bytes().to_vec();
	needle.extend_from_slice(&[0, b'*']);
	keyspace(db)
		.await
		.into_iter()
		.filter(|(key, _)| key.windows(needle.len()).any(|w| w == needle.as_slice()))
		.map(|(_, value)| value)
		.collect()
}

/// The number of layout descriptors (`!lg` keys) table `tb` holds.
async fn descriptors(db: &TestDb, tb: &str) -> usize {
	let mut table = tb.as_bytes().to_vec();
	table.push(0);
	keyspace(db)
		.await
		.into_iter()
		.filter(|(key, _)| {
			key.windows(table.len()).any(|w| w == table.as_slice())
				&& key.windows(3).any(|w| w == b"!lg")
		})
		.count()
}

/// The answers of `sql`, one per statement, as text.
async fn answers(db: &TestDb, sql: &str) -> Vec<String> {
	db.datastore()
		.execute(sql, &TestDb::owner(), None)
		.await
		.expect("the statements run")
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		})
		.collect()
}

/// The generation of a row in the v1 envelope (`FF`, a header byte, the
/// generation as LEB128) whose generation is below 128.
fn generation(row: &[u8]) -> u8 {
	assert_eq!(row[0], 0xFF, "a row in the v1 envelope: {row:02x?}");
	assert!(row[2] < 0x80, "a generation below 128: {row:02x?}");
	row[2]
}

/// A `SCHEMAFULL` table's rows are packed and every other table's are
/// documents. Every table holds a descriptor per layout it has had: a
/// `SCHEMALESS` table one, in the document layout, which its rows name, and a
/// `SCHEMAFULL` table one per field set that changed its packed layout.
#[tokio::test]
async fn a_schemafull_table_is_written_packed_and_a_schemaless_one_as_documents() {
	let db = TestDb::new(
		"DEFINE TABLE doc SCHEMALESS;
		 CREATE doc:1 SET n = 1;
		 CREATE doc:2 SET n = 2, s = 'a string longer than twelve bytes';
		 DEFINE TABLE person SCHEMAFULL;
		 DEFINE FIELD name ON person TYPE string;
		 DEFINE FIELD age ON person TYPE int;
		 CREATE person:1 SET name = 'Ann', age = 41;
		 CREATE person:2 SET name = 'a name longer than twelve bytes', age = 37;",
	)
	.await;
	let docs = rows(&db, "doc").await;
	assert_eq!(docs.len(), 2);
	for row in &docs {
		assert_eq!(row[0], 0xFF, "a document is in the v1 envelope: {row:02x?}");
		assert_eq!(row[1] & LAYOUT_BITS, 0, "a schemaless row is a document: {row:02x?}");
		assert_eq!(generation(row), 1, "a schemaless row names its table's one generation");
	}
	let people = rows(&db, "person").await;
	assert_eq!(people.len(), 2);
	for row in &people {
		assert_eq!(row[0], 0xFF, "a packed row is in the v1 envelope: {row:02x?}");
		assert!(
			(1..=3).contains(&(row[1] & LAYOUT_BITS)),
			"a SCHEMAFULL row is packed: {row:02x?}"
		);
		assert!(generation(row) >= 1, "a packed row names a generation: {row:02x?}");
	}
	assert!(descriptors(&db, "person").await >= 1, "the table minted its generations");
	assert_eq!(
		descriptors(&db, "doc").await,
		1,
		"a schemaless table holds one descriptor, whatever fields its rows carry"
	);
}

/// Rows of every generation read as their schema says in one table, whether
/// they were written before a field was added, after one was removed, or
/// rewritten since, and each schema change that alters the layout mints a
/// generation the rows after it carry.
#[tokio::test]
async fn rows_of_every_generation_read_as_their_schema_says() {
	let db = TestDb::new(
		"DEFINE TABLE p SCHEMAFULL;
		 DEFINE FIELD name ON p TYPE string;
		 DEFINE FIELD age ON p TYPE int;
		 DEFINE FIELD legacy ON p TYPE option<string>;
		 CREATE p:1 SET name = 'Ann', age = 41, legacy = 'old';
		 CREATE p:2 SET name = 'Bob', age = 37;",
	)
	.await;
	let before = descriptors(&db, "p").await;
	let out = answers(
		&db,
		"DEFINE FIELD email ON p TYPE option<string>;
		 CREATE p:3 SET name = 'Cy', age = 29, email = 'cy@x';
		 REMOVE FIELD legacy ON p;
		 CREATE p:4 SET name = 'Di', age = 52;
		 SELECT * FROM p ORDER BY id;
		 UPDATE p:2 SET age = 38 RETURN AFTER;
		 SELECT name, email FROM p WHERE age > 30 ORDER BY name;
		 SELECT count() AS c FROM p GROUP ALL;",
	)
	.await;
	assert_eq!(
		out[4],
		"[{ age: 41, id: p:1, legacy: 'old', name: 'Ann' }, { age: 37, id: p:2, name: 'Bob' }, \
		 { age: 29, email: 'cy@x', id: p:3, name: 'Cy' }, { age: 52, id: p:4, name: 'Di' }]"
	);
	assert_eq!(out[5], "[{ age: 38, id: p:2, name: 'Bob' }]", "a row rewritten at the head");
	assert_eq!(
		out[6],
		"[{ email: NONE, name: 'Ann' }, { email: NONE, name: 'Bob' }, { email: NONE, name: 'Di' }]"
	);
	assert_eq!(out[7], "[{ c: 4 }]");
	assert!(descriptors(&db, "p").await >= before + 2, "adding and removing a field each mint one");
	let held = rows(&db, "p").await;
	assert_eq!(held.len(), 4);
	let (first, third, fourth) = (generation(&held[0]), generation(&held[2]), generation(&held[3]));
	assert!(first < third, "p:3 was written after a field was added: {first} against {third}");
	assert!(third < fourth, "p:4 was written after a field was removed: {third} against {fourth}");
}

/// Every key and value the datastore holds, read in one transaction.
async fn keyspace(db: &TestDb) -> Vec<(Vec<u8>, Vec<u8>)> {
	let read = db
		.datastore()
		.transaction(crate::kvs::TransactionType::Read)
		.await
		.expect("a read transaction opens");
	let all = read
		.getr_raw(surrealdb_kvs::key::RawRange::every_key(), None)
		.await
		.expect("the keyspace reads");
	read.cancel().await.expect("the read transaction ends");
	all
}
