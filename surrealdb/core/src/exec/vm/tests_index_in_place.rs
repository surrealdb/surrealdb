//! The records an index lists, run on their stored bytes.
//!
//! A read of a B-tree index whose body runs in place runs each record it lists on the record's
//! stored bytes, as a scan of the table's range does, so a record the body emits unchanged into a
//! row frame is transcoded from its bytes there rather than decoded and encoded again. These tests
//! hold every statement to the same statement with its records decoded first, row by row and byte
//! for byte in every row format, under the table's and its fields' permissions, `COMPUTED`,
//! `VALUE` and `DEFAULT` fields, packed and document records, paging, projections, `FETCH`, sorts,
//! and the transaction's own writes. A table whose rows are put through its declarations as values
//! (`COMPUTED` fields, field permissions) decodes its records whichever way the index lists them.

use std::collections::BTreeMap;

use surrealdb_rpc::{EncodedRows, QueryStreamItem, RowFormat};

use super::host::{INDEX_DECODES, INDEX_IN_PLACE};
use crate::catalog::StoredRecord;
use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::QueryRequest;

const FORMATS: [RowFormat; 3] = [RowFormat::Cbor, RowFormat::Cbor2, RowFormat::Json];

const SETUP: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice CONTENT { name: 'Alice' };
	CREATE user:bob CONTENT { name: 'Bob' };
	DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth OR public = true;
	DEFINE INDEX d_n ON d FIELDS n;
	FOR $i IN 1..=300 {
		CREATE type::record('d', $i) CONTENT {
			n: $i % 7,
			owner: IF $i % 2 = 0 THEN user:alice ELSE user:bob END,
			public: $i % 5 = 0,
			nested: { a: [$i, 'x', { b: $i * 1.5 }], s: <string> $i },
			link: IF $i % 3 = 0 THEN user:alice ELSE user:bob END,
			at: <datetime> '2024-01-01T00:00:00Z' + <duration> (<string> $i + 's'),
			maybe: IF $i % 4 = 0 THEN NONE ELSE NULL END,
		};
	};
	DEFINE TABLE p SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD a ON p TYPE int;
	DEFINE FIELD v ON p TYPE string VALUE string::concat('v', <string> a);
	DEFINE FIELD dflt ON p TYPE int DEFAULT 7;
	DEFINE FIELD s ON p TYPE option<string>;
	DEFINE FIELD tags ON p TYPE array<string>;
	DEFINE FIELD f ON p TYPE float;
	DEFINE FIELD at ON p TYPE datetime;
	DEFINE FIELD link ON p TYPE option<record<user>>;
	DEFINE INDEX p_a ON p FIELDS a;
	FOR $i IN 1..=200 {
		CREATE type::record('p', $i) CONTENT {
			a: $i % 11, s: IF $i % 3 = 0 THEN NONE ELSE 'v' END, tags: ['x', <string> $i],
			f: $i / 3.0, at: <datetime> '2024-01-01T00:00:00Z' + <duration> (<string> $i + 'ms'),
			link: IF $i % 2 = 0 THEN user:alice ELSE NONE END,
		};
	};
	DEFINE TABLE q SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD a ON q TYPE int;
	DEFINE FIELD c ON q COMPUTED a * 2;
	DEFINE FIELD v ON q TYPE string VALUE string::concat('v', <string> a);
	DEFINE FIELD dflt ON q TYPE int DEFAULT 7;
	DEFINE FIELD s ON q TYPE option<string>;
	DEFINE FIELD owner ON q TYPE option<record<user>>;
	DEFINE FIELD secret ON q TYPE string PERMISSIONS FOR select NONE;
	DEFINE FIELD mine ON q TYPE option<int> PERMISSIONS FOR select WHERE owner = $auth;
	DEFINE INDEX q_a ON q FIELDS a;
	FOR $i IN 1..=200 {
		CREATE type::record('q', $i) CONTENT {
			a: $i % 11, secret: 'hidden', s: IF $i % 3 = 0 THEN NONE ELSE 'v' END,
			owner: IF $i % 2 = 0 THEN user:alice ELSE NONE END, mine: $i,
		};
	};
";

/// Statements whose plain `SELECT *` over an index must run its records in place.
const IN_PLACE: &[&str] = &[
	"SELECT * FROM d WHERE n = 3",
	"SELECT * FROM d WHERE n >= 2 AND n <= 4",
	"SELECT * FROM d WHERE n = 3 LIMIT 5",
	"SELECT * FROM d WHERE n = 3 START 4 LIMIT 5",
	"SELECT * FROM p WHERE a = 4",
	"SELECT * FROM p WHERE a >= 3 AND a < 6",
	"SELECT * FROM p WHERE a = 4 LIMIT 3",
];

/// Statements over an index answered the same whichever way their records are read.
const OTHERS: &[&str] = &[
	"SELECT * FROM d WHERE n = 3 AND public = true",
	"SELECT n, nested.a FROM d WHERE n = 3",
	"SELECT *, n * 2 AS twice FROM d WHERE n = 3",
	"SELECT * OMIT nested FROM d WHERE n = 3",
	"SELECT VALUE nested FROM d WHERE n = 3",
	"SELECT * FROM d WHERE n = 3 FETCH link",
	"SELECT * FROM d WHERE n = 3 ORDER BY at DESC LIMIT 7",
	"SELECT count() FROM d WHERE n = 3 GROUP ALL",
	"SELECT id, (SELECT * FROM p WHERE a = $parent.n) AS ps FROM d WHERE n = 3",
	"SELECT id, (SELECT a, v FROM p WHERE a = $parent.n LIMIT 2) AS ps FROM d WHERE n >= 5",
	"SELECT id FROM d WHERE n = 3",
	"SELECT a, v, dflt FROM p WHERE a = 4",
	"SELECT * FROM p WHERE a = 4 FETCH link",
	"SELECT * FROM p WHERE a = 99",
	"SELECT * FROM q WHERE a = 4",
	"SELECT * FROM q WHERE a >= 3 AND a < 6",
	"SELECT c, dflt, v FROM q WHERE a = 4",
	"SELECT * FROM q WHERE a = 4 AND s = 'v'",
	"SELECT * FROM q WHERE a = 99",
	"BEGIN; UPDATE d:3 SET extra = 1; CREATE d:1000 CONTENT { n: 3, public: true }; \
	 DELETE d:10; SELECT * FROM d WHERE n = 3; CANCEL;",
];

/// What one statement answered when streamed: each row's bytes, how many rows each frame held, the
/// rows and value that left as values, and its error.
#[derive(Debug, Default, PartialEq)]
struct Answer {
	rows: Vec<Vec<u8>>,
	frames: Vec<usize>,
	/// For each frame, how many interruption checks the thread had made when it arrived.
	checked: Vec<usize>,
	values: Vec<String>,
	error: Option<String>,
}

/// The bytes of each row of `frame`, in order.
fn rows_of(frame: &EncodedRows) -> Vec<Vec<u8>> {
	let bytes = frame.bytes();
	let mut start = 0;
	let mut rows = Vec::new();
	for &end in frame.ends() {
		let end = end as usize;
		let row = &bytes[start..end];
		// A JSON row after the first is joined to the one before by `,`.
		rows.push(row.strip_prefix(b",").unwrap_or(row).to_vec());
		start = end;
	}
	rows
}

/// Every statement of `sql` as `session`, streamed with rows in `format`, its records read in place
/// unless `decode`.
async fn stream(
	db: &TestDb,
	session: &Session,
	sql: &str,
	format: RowFormat,
	decode: bool,
) -> Vec<Answer> {
	INDEX_DECODES.with(|decodes| decodes.set(decode));
	let (items, received) = async_channel::unbounded();
	let job = db
		.datastore()
		.run_streaming(QueryRequest::new(sql, session).with_row_format(Some(format)), items)
		.expect("the query starts");
	let mut answers: BTreeMap<usize, Answer> = BTreeMap::new();
	let drain = async {
		while let Ok(item) = received.recv().await {
			match item {
				QueryStreamItem::Frame {
					index,
					rows,
				} => {
					let answer = answers.entry(index).or_default();
					answer.frames.push(rows.len());
					answer.checked.push(super::fuel::INTERRUPT_CHECKS.with(std::cell::Cell::get));
					answer.rows.extend(rows_of(&rows));
				}
				QueryStreamItem::Rows {
					index,
					values,
				} => answers
					.entry(index)
					.or_default()
					.values
					.extend(values.iter().map(|value| format!("{value:?}"))),
				QueryStreamItem::Value {
					index,
					value,
				} => answers.entry(index).or_default().values.push(format!("{value:?}")),
				QueryStreamItem::Finished {
					index,
					error,
					..
				} => answers.entry(index).or_default().error = error.map(|e| e.to_string()),
				QueryStreamItem::Trace {
					..
				} => {}
			}
		}
	};
	let (ran, ()) = futures::join!(job.run, drain);
	INDEX_DECODES.with(|decodes| decodes.set(false));
	ran.expect("the query runs");
	answers.into_values().collect()
}

/// `sql` as `session` answers the same, byte for byte in every row format, with its records read
/// in place as decoded. Answers how many index reads ran their records in place, and how many rows
/// the statements answered.
///
/// Every test here runs on `#[tokio::test]`'s current-thread runtime, so the statement runs on this
/// thread and the thread-local switch and count are its own.
async fn assert_same(db: &TestDb, session: &Session, sql: &str) -> (usize, usize) {
	let mut in_place = 0;
	let mut rows = 0;
	for format in FORMATS {
		let before = INDEX_IN_PLACE.with(std::cell::Cell::get);
		let placed = stream(db, session, sql, format, false).await;
		in_place += INDEX_IN_PLACE.with(std::cell::Cell::get) - before;
		let decoded = stream(db, session, sql, format, true).await;
		assert_eq!(placed.len(), decoded.len(), "{sql} {format:?}: statements");
		for (at, (p, d)) in placed.iter().zip(&decoded).enumerate() {
			assert_eq!(p.error, d.error, "{sql} {format:?}: statement {at}'s error");
			assert_eq!(p.values, d.values, "{sql} {format:?}: statement {at}'s values");
			assert_eq!(p.rows.len(), d.rows.len(), "{sql} {format:?}: statement {at}'s rows");
			for (row, (p, d)) in p.rows.iter().zip(&d.rows).enumerate() {
				assert_eq!(
					String::from_utf8_lossy(p),
					String::from_utf8_lossy(d),
					"{sql} {format:?}: statement {at}, row {row}"
				);
				assert_eq!(p, d, "{sql} {format:?}: statement {at}, row {row}");
			}
			rows += p.rows.len() + p.values.len();
		}
	}
	(in_place, rows)
}

fn owner() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

fn user(name: &str) -> Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", name));
	Session::for_record("test", "test", "user", rid)
}

/// The stored values of `table`'s records.
async fn stored(db: &TestDb, table: &str) -> Vec<Vec<u8>> {
	super::tests_packed_reads::rows(db, table).await.into_iter().map(|(_, value)| value).collect()
}

#[tokio::test]
async fn an_index_read_in_place_answers_as_its_records_decoded() {
	let db = TestDb::new_with_auth(SETUP).await;
	for table in ["p", "q"] {
		let stored = stored(&db, table).await;
		assert!(
			stored.iter().all(|value| StoredRecord::is_packed_bytes(value)),
			"{table} is packed"
		);
	}
	assert!(!stored(&db, "d").await.iter().any(|value| StoredRecord::is_packed_bytes(value)));
	for (session, checked) in [(owner(), false), (user("alice"), true), (user("bob"), true)] {
		for sql in IN_PLACE {
			let (in_place, rows) = assert_same(&db, &session, sql).await;
			// A caller the table's `PERMISSIONS FOR select` checks has its rows put through it as
			// values, so its records are decoded.
			let expected = if checked && sql.contains("FROM d") {
				0
			} else {
				FORMATS.len()
			};
			assert_eq!(in_place, expected, "{sql} as {:?}: runs in place", session.au);
			assert!(rows > 0, "{sql}: answers rows");
		}
		for sql in OTHERS {
			assert_same(&db, &session, sql).await;
		}
	}
}

#[tokio::test]
async fn an_index_read_in_place_honours_the_table_permission() {
	let db = TestDb::new_with_auth(SETUP).await;
	let sql = "SELECT * FROM d WHERE n = 3";
	let (_, all) = assert_same(&db, &owner(), sql).await;
	let (_, alice) = assert_same(&db, &user("alice"), sql).await;
	assert!(0 < alice && alice < all, "alice sees {alice} of {all} rows");
	// No row alice may not select reaches her, whichever way it is read.
	let answers = stream(&db, &user("alice"), sql, RowFormat::Json, false).await;
	for row in &answers[0].rows {
		let row = String::from_utf8_lossy(row);
		assert!(
			row.contains("user:alice") || row.contains("\"public\":true"),
			"alice may not select {row}"
		);
	}
}

#[tokio::test]
async fn an_index_read_in_place_hides_the_fields_it_must() {
	let db = TestDb::new_with_auth(SETUP).await;
	let answers =
		stream(&db, &user("bob"), "SELECT * FROM q WHERE a = 4", RowFormat::Json, false).await;
	assert!(!answers[0].rows.is_empty());
	for row in &answers[0].rows {
		let row = String::from_utf8_lossy(row);
		assert!(!row.contains("hidden"), "a field no one may select is cut: {row}");
		assert!(!row.contains("\"mine\""), "a field bob may not select is cut: {row}");
		assert!(row.contains("\"c\""), "a computed field is derived: {row}");
	}
}

#[tokio::test]
async fn an_index_read_in_place_hands_its_rows_on_at_the_batch_size() {
	let config = surrealdb_cnf::ConfigMap::empty().with_key_value("scan_batch_size", "1");
	let db = TestDb::new_with_config(SETUP, config).await;
	let sql = "SELECT * FROM d WHERE n = 3";
	let before = INDEX_IN_PLACE.with(std::cell::Cell::get);
	let answers = stream(&db, &owner(), sql, RowFormat::Cbor, false).await;
	assert_eq!(
		INDEX_IN_PLACE.with(std::cell::Cell::get) - before,
		1,
		"the index read runs in place"
	);
	let frames = &answers[0].frames;
	assert!(frames.len() > 1, "{sql}: answers rows in frames: {frames:?}");
	assert!(frames.iter().all(|&rows| rows == 1), "{sql}: a frame holds one row: {frames:?}");
}

#[tokio::test]
async fn an_index_read_in_place_yields_as_it_spends_its_fuel() {
	// A frame a row, and a quantum of four units.
	let config = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("vm_quantum", "4")
		.with_key_value("scan_batch_size", "1");
	let db = TestDb::new_with_config(SETUP, config).await;
	let sql = "SELECT * FROM d WHERE n = 3";
	let before = INDEX_IN_PLACE.with(std::cell::Cell::get);
	let answers = stream(&db, &owner(), sql, RowFormat::Cbor, false).await;
	assert_eq!(INDEX_IN_PLACE.with(std::cell::Cell::get) - before, 1, "{sql} runs in place");
	let checked = &answers[0].checked;
	assert!(checked.len() > 20, "{sql}: answers {} frames", checked.len());
	// The statement and the reader of its frames share this thread, so the reader takes frames
	// only while the statement waits at a yield point, which checks the deadline and
	// cancellation. The frames it takes between two checks are the rows run between them: each
	// record spends a unit at least, so no more than four run before the run yields.
	let longest = checked.chunk_by(|a, b| a == b).map(<[usize]>::len).max().unwrap_or(0);
	assert!(longest <= 4, "{sql}: {longest} rows ran between yield points: {checked:?}");
}
