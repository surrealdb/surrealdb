//! Packed rows read in place.
//!
//! A packed row that a body reads fields of is read where its columns lie
//! ([`PackedRowReader`]), once the whole row has passed the check that is its
//! whole decode run to build nothing. These tests hold that reader to the whole
//! decode over every row a datastore of mixed generations holds, hold a corrupt
//! row to the refusal the whole decode raises, and show which scans take the
//! in-place path.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::row::{FieldName, RowReader, StoredRow, ValueRow};
use super::row_packed::{PackedRowReader, observed};
use super::scalar::Scalar;
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::operators::test_util::TestDb;
use crate::key::KVKeyDecode as _;
use crate::key::schema::RecordKey;
use crate::kvs::{LayoutSet, TransactionType};
use crate::val::{Strand, Value};

/// The tables of the dataset, which holds packed rows of several generations, document rows of
/// the same table (written before it was `SCHEMAFULL`), string and array keys, and a table whose
/// name does not fit a strand inline.
pub(in crate::exec) const LONG: &str = "a_table_whose_name_is_longer_than_twenty_three_bytes";

/// The statements that build the dataset.
///
/// The database is at compatibility level 3, which lets the table take a schema over rows that
/// were written before it was `SCHEMAFULL` and later field changes without a `MIGRATE`
/// clause: the dataset needs rows of several generations beside document rows.
pub(in crate::exec) fn dataset() -> String {
	format!(
		"
		ALTER DATABASE COMPATIBILITY 3;
		DEFINE TABLE p SCHEMALESS;
		CREATE p:100 CONTENT {{ i: 100, s: 'a document', extra: 'x' }};
		CREATE p:101 CONTENT {{ i: 101, s: 'another document', flex: {{ k: 1 }} }};
		DEFINE TABLE OVERWRITE p SCHEMAFULL;
		DEFINE FIELD i ON p TYPE int;
		DEFINE FIELD f ON p TYPE float;
		DEFINE FIELD dec ON p TYPE decimal;
		DEFINE FIELD b ON p TYPE bool;
		DEFINE FIELD s ON p TYPE string;
		DEFINE FIELD t ON p TYPE datetime;
		DEFINE FIELD du ON p TYPE duration;
		DEFINE FIELD u ON p TYPE uuid;
		DEFINE FIELD by ON p TYPE option<bytes>;
		DEFINE FIELD o ON p TYPE option<int>;
		DEFINE FIELD nl ON p TYPE int | null;
		DEFINE FIELD tags ON p TYPE array<string>;
		DEFINE FIELD link ON p TYPE option<record<p>>;
		DEFINE FIELD lit ON p TYPE 'a' | 'b' | 'c';
		DEFINE FIELD obj ON p TYPE object;
		DEFINE FIELD obj.x ON p TYPE int;
		DEFINE FIELD obj.y ON p TYPE option<string>;
		DEFINE FIELD flex ON p TYPE object FLEXIBLE;
		DEFINE FIELD pt ON p TYPE option<geometry<point>>;
		CREATE p:1 CONTENT {{ i: 1, f: 1.5f, dec: 2.25dec, b: true, s: 'one', t: d'2024-01-02T03:04:05Z',
			du: 90s, u: u'00000000-0000-0000-0000-000000000001', by: <bytes>'ab', o: 7, nl: 3,
			tags: ['x', 'y'], link: p:2, lit: 'a', obj: {{ x: 1, y: 'in' }}, flex: {{ extra: 1 }},
			pt: (1.0, 2.0) }};
		CREATE p:2 CONTENT {{ i: -9223372036854775807, f: -0.0f, dec: 0dec, b: false, s: '',
			t: d'9999-01-01T00:00:00Z', du: 600y, u: u'ffffffff-ffff-ffff-ffff-ffffffffffff',
			nl: NULL, tags: [], lit: 'b', obj: {{ x: 0 }}, flex: {{}} }};
		CREATE p:3 CONTENT {{ i: 3, f: 3f, dec: 3dec, b: true, s: 'a string longer than twelve bytes',
			t: d'1000-01-01T00:00:00Z', du: 0s, u: u'00000000-0000-0000-0000-000000000003',
			nl: 4, tags: ['a'], lit: 'c', obj: {{ x: 5, y: 'z' }}, flex: {{ a: {{ b: 1 }}, c: [1, 2] }} }};
		DEFINE FIELD added ON p TYPE option<int>;
		CREATE p:4 CONTENT {{ i: 4, f: 4f, dec: 4dec, b: false, s: 'four', t: d'2001-02-03T04:05:06Z',
			du: 1h, u: u'00000000-0000-0000-0000-000000000004', added: 11, nl: NULL, tags: ['q'],
			lit: 'a', obj: {{ x: 4 }}, flex: {{}} }};
		CREATE p:5 CONTENT {{ i: 5, f: 5f, dec: 5dec, b: true, s: 'five', t: d'2002-02-03T04:05:06Z',
			du: 2h, u: u'00000000-0000-0000-0000-000000000005', nl: 5, tags: ['q', 'r'],
			lit: 'b', obj: {{ x: 6, y: 'w' }}, flex: {{ k: 'v' }} }};
		DEFINE FIELD OVERWRITE o ON p TYPE option<int | string>;
		CREATE p:6 CONTENT {{ i: 6, f: 6f, dec: 6dec, b: false, s: 'six', t: d'2003-02-03T04:05:06Z',
			du: 3h, u: u'00000000-0000-0000-0000-000000000006', o: 'text', nl: 6, tags: ['s'],
			lit: 'c', obj: {{ x: 7 }}, flex: {{}} }};
		CREATE p:7 CONTENT {{ i: 7, f: 7f, dec: 7dec, b: true, s: 'seven', t: d'2016-12-31T23:59:59Z',
			du: 4h, u: u'00000000-0000-0000-0000-000000000007', o: 8, nl: NULL, tags: [],
			lit: 'a', obj: {{ x: 8 }}, flex: {{}} }};
		CREATE type::record('p', 'text key') CONTENT {{ i: 8, f: 8f, dec: 8dec, b: false, s: 'eight', t: time::now(),
			du: 5h, u: rand::uuid::v7(), nl: 8, tags: ['t'], lit: 'b', obj: {{ x: 9 }}, flex: {{}} }};
		CREATE type::record('p', [1, 'k']) CONTENT {{ i: 9, f: 9f, dec: 9dec, b: true, s: 'nine', t: time::now(),
			du: 6h, u: rand::uuid::v7(), nl: 9, tags: ['n'], lit: 'c', obj: {{ x: 10 }}, flex: {{}} }};
		DEFINE TABLE {LONG} SCHEMAFULL;
		DEFINE FIELD x ON {LONG} TYPE int;
		DEFINE FIELD s ON {LONG} TYPE string;
		CREATE {LONG}:1 CONTENT {{ x: 1, s: 'long' }};
		CREATE type::record('{LONG}', 'k') CONTENT {{ x: 2, s: 'longer' }};
		"
	)
}

/// Every key and value of table `table`'s records, in key order. A record key spells its table, a
/// terminator, `*`, then the id.
pub(in crate::exec) async fn rows(db: &TestDb, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
	let read =
		db.datastore().transaction(TransactionType::Read).await.expect("a read transaction opens");
	let all = read
		.getr_raw(surrealdb_kvs::key::RawRange::every_key(), None)
		.await
		.expect("the keyspace reads");
	read.cancel().await.expect("the read transaction ends");
	let mut needle = table.as_bytes().to_vec();
	needle.extend_from_slice(&[0, b'*']);
	all.into_iter()
		.filter(|(key, _)| {
			key.windows(needle.len()).any(|w| w == needle.as_slice())
				&& RecordKey::decode_key(key).is_ok_and(|k| k.tb.as_str() == table)
		})
		.collect()
}

/// The layouts that admit each of `rows`, loaded through a read transaction as a scan loads them.
pub(in crate::exec) async fn layouts_of(
	db: &TestDb,
	table: &str,
	rows: &[(Vec<u8>, Vec<u8>)],
) -> Arc<TableLayouts> {
	let read =
		db.datastore().transaction(TransactionType::Read).await.expect("a read transaction opens");
	let (first, _) = rows.first().expect("the table has rows");
	let key = RecordKey::decode_key(first).expect("a record key");
	let mut set = LayoutSet::new(key.ns, key.db, None);
	let table = crate::val::TableName::new(table);
	for (_, value) in rows {
		set.admit(&read, &table, value).await.expect("a row is admitted");
	}
	let layouts = set.layouts_of(&table).map(Arc::clone).expect("a packed table has layouts");
	read.cancel().await.expect("the read transaction ends");
	layouts
}

/// [`layouts_of`] as a reader that trusts no verification reads them
/// ([`TableLayouts::without_verification`]): every row is checked before it is read in place, so a
/// test that damages rows holds the check to the whole decode whatever generation the rows carry.
pub(in crate::exec) async fn unverified_layouts_of(
	db: &TestDb,
	table: &str,
	rows: &[(Vec<u8>, Vec<u8>)],
) -> Arc<TableLayouts> {
	Arc::new(layouts_of(db, table, rows).await.without_verification())
}

/// A datastore holding `setup` whose scans never split, so every scan runs on the statement's
/// task and the counters of [`observed`], which each thread keeps, count its rows.
async fn serial_db(setup: &str) -> TestDb {
	TestDb::new_with_config(
		setup,
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("morsel_split_rows", usize::MAX.to_string()),
	)
	.await
}

/// The names a reader is asked for: every root field of every generation's descriptor, a name
/// that no descriptor has, and `id`.
fn names(layouts: &TableLayouts, rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
	let mut names = BTreeSet::new();
	for (_, value) in rows {
		let Ok(Some(record)) = StoredRecord::parse(value) else {
			continue;
		};
		if !record.is_packed() {
			continue;
		}
		let Ok(Some(generation)) = record.generation() else {
			continue;
		};
		if let Ok(packed) = layouts.admit_packed(generation) {
			for column in &packed.columns {
				if let [name] = column.path.as_slice() {
					names.insert(name.clone());
				}
			}
		}
	}
	names.insert("absent_everywhere".to_owned());
	names.insert("id".to_owned());
	names.into_iter().collect()
}

/// The scalar's variant, which fixes the fast path the interpreter takes over it.
fn variant(scalar: &Scalar<'_>) -> &'static str {
	match scalar {
		Scalar::None => "none",
		Scalar::Null => "null",
		Scalar::Bool(_) => "bool",
		Scalar::Number(_) => "number",
		Scalar::Str(_) => "str",
		Scalar::Duration(_) => "duration",
		Scalar::Datetime(_) => "datetime",
		Scalar::Uuid(_) => "uuid",
		Scalar::Bytes(_) => "bytes",
		Scalar::Value(_) => "value",
	}
}

/// Asserts that `a` and `b` are the same scalar: of one variant, denoting one value. A composite
/// may be held borrowed by one and owned by the other.
fn assert_same_scalar(a: &Scalar<'_>, b: &Scalar<'_>, what: &str) {
	assert_eq!(variant(a), variant(b), "{what}: {a:?} against {b:?}");
	assert!(
		crate::val::same_value(&a.to_value(), &b.to_value()),
		"{what}: {:?} against {:?}",
		a.to_value(),
		b.to_value()
	);
}

/// Asserts that `reader` reads as the value row of the whole decode of the same bytes does.
fn assert_reads_as_the_whole_decode(
	reader: &dyn RowReader,
	decoded: &Value,
	names: &[String],
	what: &str,
) {
	let value_row = ValueRow(decoded);
	for name in names {
		let field = FieldName::new(name);
		let got = reader.field(&field).expect("a field reads");
		let want = value_row.field(&field).expect("a field reads");
		assert_same_scalar(&got, &want, &format!("{what}: field {name}"));
		assert_eq!(
			reader.field_int(&field).expect("a field reads"),
			value_row.field_int(&field).expect("a field reads"),
			"{what}: field_int {name}"
		);
		let name = [Strand::from(name.as_str())];
		assert_eq!(
			reader.has_any_field(&name).expect("reads"),
			value_row.has_any_field(&name).expect("reads"),
			"{what}: has_any_field {name:?}"
		);
	}
	assert_eq!(&reader.record().expect("decodes"), decoded, "{what}: the whole record");
	let all: Vec<Strand> = names.iter().map(|name| Strand::from(name.as_str())).collect();
	assert_eq!(
		reader.has_any_field(&all).expect("reads"),
		value_row.has_any_field(&all).expect("reads"),
		"{what}: has_any_field of every name"
	);
}

/// Every row of every table of the dataset reads, field by field and whole, as the value row of
/// its whole decode does. A packed row is read in place, a document row by [`StoredRow`].
#[tokio::test]
async fn a_row_reads_in_place_as_its_whole_decode_holds_it() {
	let db = serial_db(&dataset()).await;
	let mut packed_generations = BTreeSet::new();
	let (mut packed, mut documents) = (0usize, 0usize);
	for table in ["p", LONG] {
		let rows = rows(&db, table).await;
		assert!(!rows.is_empty(), "{table} holds rows");
		let layouts = layouts_of(&db, table, &rows).await;
		let names = names(&layouts, &rows);
		for (key, value) in &rows {
			let what = format!("{table} {:?}", RecordKey::decode_key(key).map(|k| k.id));
			let decoded = decode_record_value(key, value, Some(&layouts)).expect("a row decodes");
			if StoredRecord::is_packed_bytes(value) {
				packed += 1;
				let record = StoredRecord::parse(value).expect("parses").expect("v1");
				packed_generations.insert((table, record.stamp()));
				let reader = PackedRowReader::new(key, value, Some(&layouts)).expect("reads");
				assert!(reader.borrowed().is_none(), "no field read decodes the record");
				assert_reads_as_the_whole_decode(&reader, &decoded, &names, &what);
				let reader = PackedRowReader::new(key, value, Some(&layouts)).expect("reads");
				for name in &names {
					reader.field(&FieldName::new(name)).expect("a field reads");
				}
				assert!(reader.borrowed().is_none(), "field reads decode nothing whole");
				assert_eq!(reader.into_value().expect("decodes"), decoded, "{what}: into_value");
			} else {
				documents += 1;
				let row = StoredRow::new(key, value);
				assert_reads_as_the_whole_decode(&row, &decoded, &names, &what);
			}
		}
	}
	let generations = packed_generations.iter().filter(|(table, _)| *table == "p").count();
	assert!(generations >= 3, "packed rows of several generations: {packed_generations:?}");
	assert!(packed > 10, "{packed} packed rows");
	assert!(documents >= 1, "a row the packed layout cannot hold is a document: {documents}");
}

/// The error a refused row raises, as text and as the type at the head of its chain.
fn refusal(error: &anyhow::Error) -> (String, String) {
	(format!("{error:#}"), format!("{error:?}"))
}

/// A packed row whose bytes are corrupt, whose key does not decode or is another table's, or whose
/// layouts are missing is refused before any field is read, with the error the whole decode of the
/// same bytes raises. A row the whole decode accepts is read as it is.
#[tokio::test]
async fn a_refused_packed_row_raises_the_whole_decodes_error() {
	let db = serial_db(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = unverified_layouts_of(&db, "p", &rows).await;
	let names = names(&layouts, &rows);
	let packed: Vec<&(Vec<u8>, Vec<u8>)> =
		rows.iter().filter(|(_, value)| StoredRecord::is_packed_bytes(value)).collect();
	assert!(!packed.is_empty());
	let other = rows_of_another_table(&db).await;
	let (mut accepted, mut refused) = (0usize, 0usize);
	let mut check = |key: &[u8], value: &[u8], layouts: Option<&TableLayouts>| {
		let decoded = decode_record_value(key, value, layouts);
		match (PackedRowReader::new(key, value, layouts), decoded) {
			(Ok(reader), Ok(decoded)) => {
				accepted += 1;
				assert_reads_as_the_whole_decode(&reader, &decoded, &names, "a mutant");
			}
			(Err(error), Err(want)) => {
				refused += 1;
				assert_eq!(refusal(&error), refusal(&want), "the refusal of {value:02x?}");
			}
			(Ok(_), Err(want)) => panic!("read in place, but the decode refuses: {want:?}"),
			(Err(error), Ok(_)) => panic!("refused ({error:?}), but the decode accepts"),
		}
	};
	for (key, value) in packed {
		check(key, value, Some(&layouts));
		for at in 3..value.len() {
			for bit in 0..8 {
				let mut mutant = value.clone();
				mutant[at] ^= 1 << bit;
				check(key, &mutant, Some(&layouts));
			}
		}
		for length in 0..value.len() {
			check(key, &value[..length], Some(&layouts));
		}
		let mut longer = value.clone();
		longer.push(0);
		check(key, &longer, Some(&layouts));
		// The key of another table, a key that is no key, and no layouts at all.
		check(&other, value, Some(&layouts));
		check(&[0xFF, 0xFF], value, Some(&layouts));
		check(key, value, None);
		// Every flip and truncation of the key, and a byte appended to it: a key the key decode
		// refuses is refused as the whole decode refuses it, whichever part of it is corrupt.
		for at in 0..key.len() {
			for bit in 0..8 {
				let mut mutant = key.clone();
				mutant[at] ^= 1 << bit;
				check(&mutant, value, Some(&layouts));
			}
			check(&key[..at], value, Some(&layouts));
		}
		let mut longer = key.clone();
		longer.push(0);
		check(&longer, value, Some(&layouts));
	}
	assert!(accepted > 100, "{accepted} mutants read");
	assert!(refused > 1_000, "{refused} mutants refused");
}

/// The key of a record of the long-named table.
async fn rows_of_another_table(db: &TestDb) -> Vec<u8> {
	rows(db, LONG).await.into_iter().next().expect("the table has rows").0
}

/// A scan whose body reads a field of a packed row reads it in place, once per row, and decodes
/// whole only the rows the body leaves pending. A scan whose body reads no field reads nothing in
/// place.
#[tokio::test]
async fn a_scan_reads_a_packed_row_in_place_when_its_body_reads_a_field() {
	let db = serial_db(&dataset()).await;
	let owner = TestDb::owner();
	let run = |sql: &'static str| {
		let db = &db;
		let owner = &owner;
		async move {
			observed::reset();
			let responses = db.datastore().execute(sql, owner, None).await.expect("runs");
			for response in &responses {
				response.result.as_ref().expect("the statement answers");
			}
			(observed::in_place(), observed::whole())
		}
	};
	let packed_rows = rows(&db, "p")
		.await
		.iter()
		.filter(|(_, value)| StoredRecord::is_packed_bytes(value))
		.count();
	assert!(packed_rows >= 8);
	// The bodies that run in place are those of the filters over a field and the emit of the row
	// or of one field; a projection, an aggregate and a sort are not in place.
	for sql in [
		"SELECT * FROM p WHERE i = 4",
		"SELECT * FROM p WHERE i > 2",
		"SELECT * FROM p WHERE s = 'five'",
		"SELECT * FROM p WHERE b = true",
		"SELECT * FROM p WHERE o IS NONE",
		"SELECT * FROM p WHERE nl IS NULL",
		"SELECT * FROM p WHERE i IN [1, 2, 3]",
		"SELECT * FROM p WHERE i = 4 OR b = true",
		"SELECT * FROM p WHERE f > 2.5",
		"SELECT * FROM p WHERE lit = 'a'",
		"SELECT * FROM p WHERE tags CONTAINS 'q'",
		"SELECT VALUE s FROM p WHERE i = 4",
		"SELECT * FROM p WHERE t > d'2002-01-01T00:00:00Z'",
		"SELECT * FROM p WHERE u = u'00000000-0000-0000-0000-000000000004'",
		"SELECT * FROM p WHERE by = <bytes>'ab'",
		"SELECT * FROM p WHERE du > 1h",
		"SELECT * FROM p WHERE dec > 3dec",
	] {
		let (in_place, whole) = run(sql).await;
		assert_eq!(in_place, packed_rows, "`{sql}` reads every packed row in place");
		assert_eq!(whole, 0, "`{sql}` decodes no packed row whole before its body");
		assert_scans_read_in_place(sql);
	}
	let (in_place, _) = run("SELECT * FROM p").await;
	assert_eq!(in_place, 0, "a body that reads no field reads nothing in place");
	let scans = observed::pipelines();
	assert!(
		scans.iter().any(|shape| shape.scan && shape.in_place && !shape.reads_row),
		"a scan whose body reads no field is in place but reads no row: {scans:?}"
	);
}

/// The scan pipelines the statements just run compiled, each a scan the body of which runs on the
/// stored row, reads fields of it and takes it whole nowhere, with no count source or enforcing
/// step beside them.
fn assert_scans_read_in_place(sql: &str) {
	let shapes = observed::pipelines();
	let scans: Vec<_> = shapes.iter().filter(|shape| shape.scan).collect();
	assert!(!scans.is_empty(), "`{sql}` scans a table: {shapes:?}");
	for shape in &shapes {
		assert!(!shape.count, "`{sql}` has a count source: {shapes:?}");
		assert!(!shape.enforced, "`{sql}` enforces its rows: {shapes:?}");
	}
	for shape in scans {
		assert!(shape.in_place && shape.reads_row, "`{sql}` is not read in place: {shapes:?}");
	}
}

/// The shapes of `vm_alloc` that run in place, over the same columns, compile to scans that read
/// the packed row in place, and the shapes that decode first compile to pipelines that do not.
#[tokio::test]
async fn the_allocation_shapes_compile_to_the_path_they_measure() {
	let db = serial_db(
		"
		DEFINE TABLE p SCHEMAFULL;
		DEFINE FIELD a ON p TYPE int;
		DEFINE FIELD b ON p TYPE int;
		DEFINE FIELD s ON p TYPE string;
		DEFINE FIELD lo ON p TYPE int;
		CREATE |p:1..20| SET a = rand::int(), b = 3, s = 'abc', lo = 1;
		CREATE |t:1..20| SET a = rand::int(), b = 3, s = 'abc', lo = 1;
		DEFINE TABLE ps SCHEMAFULL;
		DEFINE FIELD b ON ps TYPE int;
		FOR $i IN 0..20 {
			CREATE type::record('ps', 'a_string_key_longer_than_23_bytes_' + <string> $i) SET b = $i % 7;
		};
		DEFINE TABLE pa SCHEMAFULL;
		DEFINE FIELD b ON pa TYPE int;
		FOR $i IN 0..20 {
			CREATE type::record('pa', [$i, 'an_element_longer_than_23_bytes']) SET b = $i % 7;
		};
		DEFINE TABLE pe SCHEMAFULL;
		DEFINE FIELD b ON pe TYPE int;
		FOR $i IN 0..20 {
			CREATE type::record('pe', 'a_key_with_a_nul_byte_\\u{0}_longer_than_23_bytes_' + <string> $i) SET b = $i % 7;
		};
		DEFINE TABLE pn SCHEMAFULL;
		DEFINE FIELD b ON pn TYPE int;
		FOR $i IN 0..20 {
			CREATE type::record('pn', [$i, 1.5f + $i, 2.5dec + $i, 'an_element_longer_than_23_bytes', rand::uuid::v4()]) SET b = $i % 7;
		};
		DEFINE TABLE pu SCHEMAFULL;
		DEFINE FIELD b ON pu TYPE int;
		FOR $i IN 0..20 {
			CREATE type::record('pu', rand::uuid::v4()) SET b = $i % 7;
		};
		DEFINE TABLE po SCHEMAFULL;
		DEFINE FIELD b ON po TYPE int;
		FOR $i IN 0..20 {
			CREATE type::record('po', { n: $i, f: 1.5f + $i, d: 2.5dec + $i, s: 'an_entry_longer_than_23_bytes', u: rand::uuid::v4() }) SET b = $i % 7;
		};
		",
	)
	.await;
	let owner = TestDb::owner();
	for (sql, in_place) in [
		("SELECT * FROM p WHERE b = 99", true),
		("SELECT * FROM ps WHERE b = 99", true),
		("SELECT * FROM pa WHERE b = 99", true),
		("SELECT * FROM pe WHERE b = 99", true),
		("SELECT * FROM pn WHERE b = 99", true),
		("SELECT * FROM pu WHERE b = 99", true),
		("SELECT * FROM po WHERE b = 99", true),
		("SELECT * FROM p WHERE b = 3", true),
		("SELECT VALUE s FROM p WHERE b = 3", true),
		("SELECT count() FROM p WHERE b = 99 GROUP ALL", true),
		("SELECT lo, count() AS c FROM p GROUP BY lo", true),
		("SELECT * FROM p ORDER BY a DESC LIMIT 10", false),
		("SELECT * FROM p ORDER BY a ASC LIMIT 10", false),
		("SELECT * FROM p WHERE b < 7 ORDER BY a ASC LIMIT 10", false),
	] {
		observed::reset();
		for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
			response.result.expect("the statement answers");
		}
		if in_place {
			assert_scans_read_in_place(sql);
		} else {
			let shapes = observed::pipelines();
			assert!(
				shapes.iter().filter(|shape| shape.scan).all(|shape| !shape.reads_row),
				"`{sql}` decodes its rows first: {shapes:?}"
			);
			assert_eq!(observed::in_place(), 0, "`{sql}`");
		}
	}
	// A body that decodes its rows first has a source that judges stored bytes before the decode
	// where the scan's filter allows it, and only a document answers that judgement: a packed row
	// is always decoded. The same statement over the documents of `t` compiles to the same shape.
	let packed =
		"SELECT count() AS c, math::max(string::len(s)) AS m FROM p WHERE b = 99 GROUP ALL";
	let twin = "SELECT count() AS c, math::max(string::len(s)) AS m FROM t WHERE b = 99 GROUP ALL";
	let mut seen = Vec::new();
	for sql in [packed, twin] {
		observed::reset();
		for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
			response.result.expect("the statement answers");
		}
		let scans: Vec<_> = observed::pipelines().into_iter().filter(|shape| shape.scan).collect();
		assert!(
			scans.iter().all(|shape| !shape.in_place && shape.prefilter),
			"`{sql}` decodes its rows after a prefilter: {scans:?}"
		);
		seen.push(scans);
	}
	assert_eq!(seen[0], seen[1], "`{packed}` and `{twin}` compile alike");
}

/// A PostgreSQL `COUNT(column)`, which counts the rows whose column is neither NULL nor NONE, folds
/// on the stored row as a grouped `count()` does.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_postgres_count_of_a_column_folds_on_the_stored_row() {
	let db = serial_db(
		"
		DEFINE TABLE p SCHEMAFULL;
		DEFINE FIELD b ON p TYPE option<int | null>;
		DEFINE FIELD lo ON p TYPE int;
		CREATE |p:1..21| SET b = 0, lo = 1;
		CREATE |p:21..31| SET b = NULL, lo = 1;
		CREATE |p:31..41| SET lo = 2;
		",
	)
	.await;
	let owner = TestDb::owner();
	let sql = "SELECT lo, count(b) AS c FROM p GROUP BY lo ORDER BY lo";
	observed::reset();
	let results = db
		.datastore()
		.run(crate::kvs::QueryRequest::new(crate::kvs::QuerySource::pgsql(sql), &owner))
		.await
		.expect("runs");
	let answers: Vec<_> = results
		.into_iter()
		.map(|response| response.result.expect("the statement answers"))
		.collect();
	// Zero counts and NULL does not; a missing column counts nothing.
	let answers = format!("{answers:?}");
	assert!(answers.contains(r#"{"c": Number(Int(20)), "lo": Number(Int(1))}"#), "{answers}");
	assert!(answers.contains(r#"{"c": Number(Int(0)), "lo": Number(Int(2))}"#), "{answers}");
	assert_scans_read_in_place(sql);
}

/// The filters of `packed_reads/errors_in_place.surql`, whose arithmetic over fields fails on some
/// rows, compile to scans that read the packed row in place, so the errors they raise come from a
/// body running on the stored row.
#[tokio::test]
async fn the_failing_arithmetic_filters_compile_to_scans_read_in_place() {
	let db = serial_db(
		"
		DEFINE TABLE ip SCHEMAFULL;
		DEFINE FIELD n ON ip TYPE int;
		DEFINE FIELD big ON ip TYPE int;
		DEFINE FIELD s ON ip TYPE string;
		CREATE ip:1 CONTENT { n: 1, big: 1, s: 'a' };
		CREATE ip:2 CONTENT { n: 2, big: 2, s: 'b' };
		CREATE ip:3 CONTENT { n: 3, big: 9223372036854775807, s: 'c' };
		CREATE ip:4 CONTENT { n: 4, big: 9223372036854775807, s: 'd' };
		CREATE ip:5 CONTENT { n: 5, big: 5, s: 'e' };
		",
	)
	.await;
	let owner = TestDb::owner();
	for sql in [
		"SELECT VALUE n FROM ip WHERE big + n > 0",
		"SELECT * FROM ip WHERE n < 3 OR big + n > 0",
		"SELECT * FROM ip WHERE n > 3 AND big + n > 0",
		"SELECT VALUE id FROM ip WHERE big * n > 0",
		"SELECT VALUE id FROM ip WHERE n < 3 AND big + n > 0",
		"SELECT VALUE id FROM ip WHERE big - n > 0",
		"SELECT VALUE id FROM ip WHERE n + big < 0",
	] {
		observed::reset();
		// The statement may fail: it is the compiled scan that is held.
		for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
			let _ = response.result;
		}
		assert_scans_read_in_place(sql);
	}
}

/// The statements of a bench dataset under `language-tests/tests/bench/util/`, without its
/// frontmatter, for `rows` records in place of the thousands it holds.
fn bench_dataset(source: &str, count: usize, rows: usize) -> String {
	let body = source.split_once("*/").map_or(source, |(_, body)| body);
	body.replace(&format!("|record:{count}|"), &format!("|record:{rows}|"))
}

/// The bench datasets of a packed table write packed rows, as the shapes measured over them
/// require: every row of the first is packed at one generation, and the second holds rows of two.
#[tokio::test]
async fn the_bench_datasets_are_written_packed() {
	let schemafull = bench_dataset(
		include_str!(
			"../../../../../language-tests/tests/bench/util/records-10000-schemafull.surql"
		),
		10_000,
		200,
	);
	let db = serial_db(&schemafull).await;
	let held = rows(&db, "record").await;
	assert_eq!(held.len(), 200);
	let mut generations = BTreeSet::new();
	for (_, value) in &held {
		assert!(StoredRecord::is_packed_bytes(value), "a bench row is packed: {value:02x?}");
		generations.insert(StoredRecord::parse(value).expect("parses").expect("v1").stamp());
	}
	assert_eq!(generations.len(), 1, "one generation: {generations:?}");
	// The bench shapes: which read the packed rows in place and which decode them first.
	let owner = TestDb::owner();
	for (sql, in_place) in [
		("SELECT * FROM record WHERE number = 21", true),
		("SELECT count() FROM record WHERE number = 21 GROUP ALL", true),
		("SELECT count() FROM record WHERE city = 'Tokyo' GROUP ALL", true),
		(
			"SELECT count() FROM record WHERE geography.region.details.continent = 'Asia' GROUP ALL",
			false,
		),
		("SELECT count() FROM record WHERE note IS NONE GROUP ALL", true),
		("SELECT city, count() AS c, math::mean(score) AS m FROM record GROUP BY city", true),
		("SELECT * FROM record", false),
		("SELECT * FROM record ORDER BY score DESC", false),
	] {
		observed::reset();
		for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
			response.result.expect("the statement answers");
		}
		assert_eq!(
			observed::in_place() == held.len(),
			in_place,
			"`{sql}`: {}",
			observed::in_place()
		);
		if in_place {
			assert_scans_read_in_place(sql);
		}
	}
	let mixed = bench_dataset(
		include_str!(
			"../../../../../language-tests/tests/bench/util/records-10000-schemafull-mixed.surql"
		),
		5_000,
		100,
	);
	let db = serial_db(&mixed).await;
	let held = rows(&db, "record").await;
	assert_eq!(held.len(), 200);
	let mut generations = BTreeSet::new();
	for (_, value) in &held {
		assert!(StoredRecord::is_packed_bytes(value), "a bench row is packed: {value:02x?}");
		generations.insert(StoredRecord::parse(value).expect("parses").expect("v1").stamp());
	}
	assert_eq!(generations.len(), 2, "two generations: {generations:?}");
}

/// The bench shapes over packed tables (`bench/scans/packed/*`) reach the filter and the probe over
/// packed rows: over the bench table with a computed field, a projection that calls a function
/// decodes its rows, and the pre-decode filter judges a packed row from its columns where its
/// predicate is selective;
/// the top-k scan of the table without one holds a probe that may refuse a packed row and refuses
/// them.
#[tokio::test]
async fn the_filter_and_top_k_bench_shapes_reach_the_filter_and_the_probe_of_a_packed_table() {
	let computed = bench_dataset(
		include_str!(
			"../../../../../language-tests/tests/bench/util/records-10000-schemafull-computed.surql"
		),
		10_000,
		1_200,
	);
	let db = serial_db(&computed).await;
	let held = rows(&db, "record").await;
	assert_eq!(held.len(), 1_200);
	assert!(held.iter().all(|(_, value)| StoredRecord::is_packed_bytes(value)));
	let owner = TestDb::owner();
	let many = (1..=40).map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
	// The filter judges the packed rows of a scan only where the physical layer states its
	// predicate selective: an equality and a set of integers refuse most rows, which an
	// inequality does not, so no packed row is checked for it and each is left to the decode.
	for (sql, judges_packed) in [
		("SELECT name, age, string::len(name) AS l FROM record WHERE number = 21".to_owned(), true),
		(format!("SELECT name, string::len(name) AS l FROM record WHERE number IN [{many}]"), true),
		("SELECT name, string::len(name) AS l FROM record WHERE number != 21".to_owned(), false),
	] {
		observed::reset();
		for response in db.datastore().execute(&sql, &owner, None).await.expect("runs") {
			response.result.expect("the statement answers");
		}
		let shapes = observed::pipelines();
		let scans: Vec<_> = shapes.iter().filter(|shape| shape.scan).collect();
		assert!(!scans.is_empty(), "`{sql}` scans a table: {shapes:?}");
		assert!(
			scans.iter().all(|shape| {
				!shape.in_place && shape.prefilter && shape.judges_packed == judges_packed
			}),
			"`{sql}` reaches the filter: {shapes:?}"
		);
		if judges_packed {
			assert!(observed::filtered() > held.len() / 2, "`{sql}`: {}", observed::filtered());
		} else {
			assert_eq!(observed::filtered(), 0, "`{sql}`");
		}
	}
	// The twin of the table with a computed field over documents, which the prefilter shapes are
	// compared with, decodes its rows after the same filter: its scans are not in place and hold a
	// prefilter, and it holds no packed row.
	let twin = bench_dataset(
		include_str!(
			"../../../../../language-tests/tests/bench/util/records-10000-schemaless-computed.surql"
		),
		10_000,
		1_200,
	);
	let db = serial_db(&twin).await;
	let held = rows(&db, "record").await;
	assert_eq!(held.len(), 1_200);
	assert!(held.iter().all(|(_, value)| !StoredRecord::is_packed_bytes(value)));
	for sql in [
		"SELECT name, age, string::len(name) AS l FROM record WHERE number = 21".to_owned(),
		format!("SELECT name, string::len(name) AS l FROM record WHERE number IN [{many}]"),
		"SELECT name, string::len(name) AS l FROM record WHERE number != 21".to_owned(),
	] {
		observed::reset();
		for response in db.datastore().execute(&sql, &owner, None).await.expect("runs") {
			response.result.expect("the statement answers");
		}
		let shapes = observed::pipelines();
		let scans: Vec<_> = shapes.iter().filter(|shape| shape.scan).collect();
		assert!(
			!scans.is_empty() && scans.iter().all(|shape| !shape.in_place && shape.prefilter),
			"`{sql}` over documents decodes its rows after a prefilter: {shapes:?}"
		);
	}
	let schemafull = bench_dataset(
		include_str!(
			"../../../../../language-tests/tests/bench/util/records-10000-schemafull.surql"
		),
		10_000,
		1_200,
	);
	let db = serial_db(&schemafull).await;
	let sql = "SELECT * FROM record ORDER BY score DESC LIMIT 100";
	observed::reset();
	for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
		response.result.expect("the statement answers");
	}
	let shapes = observed::pipelines();
	let scans: Vec<_> = shapes.iter().filter(|shape| shape.scan).collect();
	assert!(
		!scans.is_empty() && scans.iter().all(|shape| shape.topk_packed == Some(true)),
		"`{sql}`: {shapes:?}"
	);
	assert!(observed::probed() > 100, "`{sql}` refuses {} packed rows", observed::probed());
}

/// The path a bench shape of `bench/scans/packed/` takes over a packed table.
#[derive(Clone, Copy, Debug)]
enum BenchPath {
	/// The scan's body runs on the stored row and reads fields of it.
	InPlace,
	/// The scan decodes its rows, and its pre-decode filter refuses most of them from their
	/// columns first.
	Filtered,
	/// The scan decodes every row whole, with no filter to judge it first.
	Decoded,
	/// The scan's top-k probe refuses most of its rows from their columns before the sort takes
	/// them.
	TopK,
}

/// The query of a bench file: the statement after its frontmatter.
fn bench_query(source: &str) -> &str {
	source.split_once("*/").map_or(source, |(_, query)| query).trim()
}

/// Every bench shape of `bench/scans/packed/` takes, over each dataset it names, the path its
/// file states: the shapes that read the packed row in place compile to scans whose body runs on
/// the stored row, reads fields of it and takes it whole nowhere, with no count source or
/// enforcing step; the others read no packed row in place, and each states where its rows are
/// judged before they are decoded, if anywhere.
#[tokio::test]
async fn every_bench_shape_takes_the_path_it_claims() {
	macro_rules! shape {
		($name:literal, $path:expr) => {
			(
				$name,
				include_str!(concat!(
					"../../../../../language-tests/tests/bench/scans/packed/",
					$name,
					".surql"
				)),
				$path,
			)
		};
	}
	use BenchPath::*;
	let shapes = [
		shape!("where_integer_eq_full", InPlace),
		shape!("where_integer_eq_count", InPlace),
		shape!("where_string_eq_count", InPlace),
		shape!("where_option_none_count", InPlace),
		shape!("where_nested_eq_count", Filtered),
		shape!("aggregate_group_by_city", InPlace),
		shape!("scan_full", Decoded),
		shape!("order_by_score_full", Decoded),
		shape!("order_by_score_limit", TopK),
	];
	let owner = TestDb::owner();
	for (source, count) in
		[("records-10000-schemafull", 10_000), ("records-10000-schemafull-mixed", 5_000)]
	{
		let dataset = match source {
			"records-10000-schemafull" => include_str!(
				"../../../../../language-tests/tests/bench/util/records-10000-schemafull.surql"
			),
			_ => include_str!(
				"../../../../../language-tests/tests/bench/util/records-10000-schemafull-mixed.surql"
			),
		};
		let loaded = if count == 10_000 {
			1_200
		} else {
			600
		};
		let db = serial_db(&bench_dataset(dataset, count, loaded)).await;
		let held = rows(&db, "record").await;
		assert!(held.iter().all(|(_, value)| StoredRecord::is_packed_bytes(value)));
		for (name, file, path) in shapes {
			let sql = bench_query(file);
			observed::reset();
			for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
				response.result.expect("the statement answers");
			}
			let what = format!("`{name}` over `{source}`: {:?}", observed::pipelines());
			let pipelines = observed::pipelines();
			let scans: Vec<_> = pipelines.iter().filter(|shape| shape.scan).collect();
			assert!(!scans.is_empty(), "{what}");
			assert!(pipelines.iter().all(|shape| !shape.count && !shape.enforced), "{what}");
			let (in_place, filtered) = (observed::in_place(), observed::filtered());
			match path {
				InPlace => {
					assert!(scans.iter().all(|shape| shape.in_place && shape.reads_row), "{what}");
					assert_eq!(in_place, held.len(), "{what}");
				}
				Filtered => {
					assert!(
						scans.iter().all(|shape| {
							!shape.in_place && shape.prefilter && shape.judges_packed
						}),
						"{what}"
					);
					assert!(filtered > held.len() / 2, "{what}: filtered {filtered}");
				}
				Decoded => {
					assert!(
						scans.iter().all(|shape| {
							!shape.reads_row && !shape.prefilter && shape.topk_packed.is_none()
						}),
						"{what}"
					);
					assert_eq!(in_place + filtered + observed::probed(), 0, "{what}");
				}
				TopK => {
					assert!(scans.iter().all(|shape| shape.topk_packed == Some(true)), "{what}");
					assert!(observed::probed() > 0, "{what}");
				}
			}
		}
	}
}

/// The answers of `statements`, as text, one per statement.
async fn answers_of(db: &TestDb, statements: &[&str]) -> Vec<String> {
	use surrealdb_types::ToSql;
	let owner = TestDb::owner();
	let mut out = Vec::new();
	for sql in statements {
		for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
			out.push(match response.result {
				Ok(value) => value.to_sql(),
				Err(error) => format!("error: {error}"),
			});
		}
	}
	out
}

/// A scan of a packed table split into morsels answers what the serial scan answers, in the same
/// order. The config that splits every range into entries makes the planner choose morsels
/// wherever it may; whether a given statement ran as morsels is the planner's, so this holds the
/// answers equal and does not itself prove that any ran so.
#[tokio::test]
async fn a_packed_scan_split_into_morsels_answers_the_serial_scan() {
	let statements = [
		"SELECT * FROM p WHERE i < 8",
		"SELECT VALUE id FROM p WHERE i > 2",
		"SELECT VALUE s FROM p WHERE b = true",
		"SELECT * FROM p WHERE o IS NONE AND i < 8",
		"SELECT * FROM p WHERE nl IS NULL AND i < 8",
		"SELECT VALUE i FROM p WHERE i IN [1, 3, 5, 7, 100]",
		"SELECT * FROM p WHERE s = 'five'",
		"SELECT * FROM p WHERE i = 99",
	];
	let serial = serial_db(&dataset()).await;
	let morsels = TestDb::new_with_config(
		&dataset(),
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("morsel_split_rows", "1")
			.with_key_value("morsel_split_pass_ppm", "1000000")
			.with_key_value("morsel_target_bytes", "1"),
	)
	.await;
	assert_eq!(answers_of(&morsels, &statements).await, answers_of(&serial, &statements).await);
}

/// A field read followed by a path into the value read is a field read of the row and takes it
/// nowhere, so a body that holds only such reads reads fields of the row; beside an instruction
/// that takes the row whole it does not.
#[test]
fn a_field_then_path_read_is_a_field_read_of_the_row() {
	use super::program::{Instr, reads_row};
	let read = || Instr::LoadPath {
		dst: 1,
		field: 0,
		rest: 0,
	};
	assert!(reads_row(&[read()]));
	assert!(!reads_row(&[
		read(),
		Instr::LoadRow {
			dst: 2
		}
	]));
}

/// A scan beneath a top-k sort may reject a packed row from its columns only where every filter the
/// rows cross on the way to the sort judges a predicate that is infallible, deterministic and a
/// function of the row; a predicate that steps into the value of a field may fetch a linked record
/// and is not.
#[tokio::test]
async fn a_top_k_scan_rejects_packed_rows_only_beneath_row_local_ops() {
	let db = serial_db(
		"
		DEFINE TABLE t SCHEMAFULL;
		DEFINE FIELD a ON t TYPE int;
		DEFINE FIELD b ON t TYPE int;
		DEFINE FIELD s ON t TYPE string;
		DEFINE FUNCTION fn::same($x: int) { RETURN $x; };
		CREATE |t:1..20| SET a = rand::int(), b = 3, s = '7';
		DEFINE TABLE o SCHEMAFULL;
		DEFINE FIELD x ON o TYPE int;
		CREATE o:1 SET x = 1;
		DEFINE TABLE l SCHEMAFULL;
		DEFINE FIELD a ON l TYPE int;
		DEFINE FIELD link ON l TYPE record<o>;
		CREATE |l:1..20| SET a = rand::int(), link = o:1;
		",
	)
	.await;
	let owner = TestDb::owner();
	for (sql, packed_rows) in [
		("SELECT * FROM t ORDER BY b LIMIT 5", true),
		("SELECT * FROM t ORDER BY b DESC LIMIT 5", true),
		("SELECT * FROM t WHERE a > 1 ORDER BY b LIMIT 5", true),
		("SELECT * FROM t WHERE a > 1 AND b < 9 ORDER BY b, a LIMIT 5", true),
		("SELECT * FROM t WHERE type::int(s) > 1 ORDER BY b LIMIT 5", false),
		("SELECT * FROM t WHERE fn::same(a) > 1 ORDER BY b LIMIT 5", false),
		("SELECT * FROM t WHERE rand::int() > 1 ORDER BY b LIMIT 5", false),
		("SELECT * FROM t ORDER BY b, s LIMIT 5", true),
		("SELECT * FROM l WHERE a > 1 ORDER BY a LIMIT 5", true),
		("SELECT * FROM l WHERE link.x = 1 ORDER BY a LIMIT 5", false),
		("SELECT * FROM l WHERE a > 1 AND link.x = 1 ORDER BY a LIMIT 5", false),
	] {
		observed::reset();
		for response in db.datastore().execute(sql, &owner, None).await.expect("runs") {
			response.result.expect("the statement answers");
		}
		let keys: Vec<_> =
			observed::pipelines().into_iter().filter_map(|shape| shape.topk_packed).collect();
		// A scan whose body may raise for a row carries no threshold at all, since the serial run
		// runs the body of every row; any other carries one that judges packed rows only beneath
		// row-local ops.
		let judged = if packed_rows {
			keys == [true]
		} else {
			keys.is_empty() || keys == [false]
		};
		assert!(judged, "`{sql}`: {keys:?} {:?}", observed::pipelines());
	}
}

/// A packed row is admitted exactly where its whole decode accepts it: whatever corrupts its
/// bytes, its key or its layouts, the row is refused where the decode refuses it, and a record in
/// no packed layout is its bytes.
#[tokio::test]
async fn an_admitted_row_is_refused_where_the_whole_decode_refuses_it() {
	use crate::exec::packed_view::AdmittedRow;
	let db = serial_db(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = unverified_layouts_of(&db, "p", &rows).await;
	let other = rows_of_another_table(&db).await;
	let (mut packed, mut refused, mut bytes) = (0usize, 0usize, 0usize);
	let mut check = |key: &[u8], value: &[u8], layouts: Option<&TableLayouts>| {
		let admitted = AdmittedRow::of(key, value, layouts);
		let decoded = decode_record_value(key, value, layouts);
		match (admitted, decoded.is_ok()) {
			(
				AdmittedRow::Packed {
					..
				},
				true,
			) => packed += 1,
			(AdmittedRow::Refused, false) => refused += 1,
			(AdmittedRow::Bytes(held), _) => {
				assert!(!StoredRecord::is_packed_bytes(value), "a packed row is not its bytes");
				assert_eq!(held, value);
				bytes += 1;
			}
			(admitted, accepted) => {
				panic!("{admitted:?} where the whole decode accepts is {accepted}: {value:02x?}")
			}
		}
	};
	for (key, value) in &rows {
		check(key, value, Some(&layouts));
		if !StoredRecord::is_packed_bytes(value) {
			continue;
		}
		for at in 3..value.len() {
			for bit in 0..8 {
				let mut mutant = value.clone();
				mutant[at] ^= 1 << bit;
				check(key, &mutant, Some(&layouts));
			}
		}
		for length in 0..value.len() {
			check(key, &value[..length], Some(&layouts));
		}
		check(&other, value, Some(&layouts));
		check(&[0xFF, 0xFF], value, Some(&layouts));
		check(key, value, None);
		for at in 0..key.len() {
			for bit in 0..8 {
				let mut mutant = key.clone();
				mutant[at] ^= 1 << bit;
				check(&mutant, value, Some(&layouts));
			}
			check(&key[..at], value, Some(&layouts));
		}
	}
	assert!(packed > 100, "{packed} packed rows admitted");
	assert!(refused > 1_000, "{refused} refused");
	assert!(bytes >= 1, "a document is its bytes");
}

/// The scans that the packed shapes of `vm_alloc` take their rows from judge a packed row from its
/// columns before they decode it: the filter of a count refuses every row it is false of, and the
/// probe of a top-k scan refuses every row that cannot enter the sort, the same shape over a table
/// of documents doing as much.
#[tokio::test]
async fn the_filter_and_the_probe_refuse_packed_rows_from_their_columns() {
	let db = serial_db(
		"
		DEFINE TABLE p SCHEMAFULL;
		DEFINE FIELD a ON p TYPE int;
		DEFINE FIELD b ON p TYPE int;
		DEFINE FIELD s ON p TYPE string;
		FOR $i IN 0..2000 {
			CREATE type::record('p', $i) SET a = $i, b = $i % 7, s = 'abc';
		};
		DEFINE TABLE t SCHEMALESS;
		FOR $i IN 0..2000 {
			CREATE type::record('t', $i) SET a = $i, b = $i % 7, s = 'abc';
		};
		",
	)
	.await;
	let owner = TestDb::owner();
	let run = |sql: String| {
		let (db, owner) = (&db, &owner);
		async move {
			observed::reset();
			for response in db.datastore().execute(&sql, owner, None).await.expect("runs") {
				response.result.expect("the statement answers");
			}
			(
				observed::filtered(),
				observed::probed(),
				observed::probed_bytes(),
				observed::pipelines(),
			)
		}
	};
	// The filter refuses a row it judges false, and keeps the rest for the body.
	let (filtered, probed, _, shapes) =
		run("SELECT count() AS c, math::max(string::len(s)) AS m FROM p WHERE b = 99 GROUP ALL"
			.into())
		.await;
	assert_eq!((filtered, probed), (2000, 0), "{shapes:?}");
	let (filtered, ..) =
		run("SELECT count() AS c, math::max(string::len(s)) AS m FROM p WHERE b = 3 GROUP ALL"
			.into())
		.await;
	assert_eq!(filtered, 2000 - (0..2000).filter(|i| i % 7 == 3).count());
	// The probe refuses the rows that come after the sort holds its ten, best first.
	for sql in [
		"SELECT * FROM p ORDER BY a ASC LIMIT 10",
		"SELECT * FROM p WHERE b < 7 ORDER BY a ASC LIMIT 10",
	] {
		let (_, probed, _, shapes) = run(sql.into()).await;
		let scans: Vec<_> = shapes.iter().filter(|shape| shape.scan).collect();
		assert!(scans.iter().all(|shape| shape.topk_packed == Some(true)), "`{sql}`: {shapes:?}");
		assert!(probed > 1_000, "`{sql}` refuses {probed} packed rows");
		let twin = run(sql.replace(" p ", " t ")).await;
		assert_eq!(twin.2, probed, "`{sql}` refuses as many rows as its twin");
	}
	// A key that cannot beat the sort's worst is refused whichever way the order goes, and a row
	// the sort has not room for is never lost.
	let (_, probed, ..) = run("SELECT * FROM p ORDER BY a DESC LIMIT 10".into()).await;
	assert_eq!(probed, 0, "rows arrive worst first");
	// A fallible predicate above the scan keeps the probe from refusing a packed row.
	let (_, probed, _, shapes) =
		run("SELECT * FROM p WHERE type::int(b) > 1 ORDER BY a ASC LIMIT 10".into()).await;
	assert_eq!(probed, 0, "{shapes:?}");
}

/// A query answers what it answers with the filter and the probe kept out of it by a predicate
/// that is true of every row but is a call: the same rows in the same order, over every kind of
/// column of the dataset, nested and open objects and absent values included.
#[tokio::test]
async fn the_filter_and_the_probe_change_no_answer_over_packed_rows() {
	// Past the first batch of a scan, whose rows are read before any threshold is published.
	let bulk = "
		FOR $n IN 0..1200 {
			CREATE type::record('p', 1000 + $n) CONTENT {
				i: $n % 50, f: 1.5f, dec: 1dec, b: $n % 2 = 0, s: 's' + <string> ($n % 13),
				t: d'2020-01-01T00:00:00Z', du: 1h, u: u'00000000-0000-0000-0000-000000000009',
				nl: IF $n % 5 = 0 { NULL } ELSE { $n % 11 }, tags: [], lit: 'a',
				obj: { x: $n % 9 }, flex: {}
			};
		};
	";
	let db = serial_db(&format!("{}{bulk}", dataset())).await;
	let mut statements: Vec<(String, String)> = Vec::new();
	let predicates = [
		"i < 8",
		"i = 3",
		"i != 3",
		"i IN [1, 3, 5, 100]",
		"s = 'five'",
		"s != 'five'",
		"s CONTAINS 'e'",
		"b = true",
		"f > 3.0",
		"dec = 3dec",
		"t > d'2002-01-01T00:00:00Z'",
		"du >= 2h",
		"u = u'00000000-0000-0000-0000-000000000003'",
		"o IS NONE",
		"o IS NOT NONE AND o != 8",
		"nl IS NULL",
		"nl > 4",
		"added = 11",
		"added IS NONE",
		"lit = 'a'",
		"lit IN ['a', 'c']",
		"tags CONTAINS 'q'",
		"obj.x > 5",
		"obj.y = 'z'",
		"obj.y IS NONE",
		"flex.k = 'v'",
		"flex.a.b = 1",
		"flex.extra = 1",
		"absent = 1",
		"absent IS NONE",
		"NOT (i < 4 OR b = false)",
		"i < 4 AND (s = 'one' OR s = 'four')",
		"link = p:2",
		"pt IS NONE",
	];
	for predicate in predicates {
		for shape in [
			"SELECT count() FROM p WHERE {} GROUP ALL",
			"SELECT s, count() AS c FROM p WHERE {} GROUP BY s",
			"SELECT i, s FROM p WHERE {} ORDER BY i",
			"SELECT * FROM p WHERE {} ORDER BY s DESC",
		] {
			let kept = shape.replace("{}", predicate);
			let hidden = shape.replace("{}", &format!("type::bool({predicate})"));
			statements.push((kept, hidden));
		}
	}
	let keys = [
		"i",
		"s",
		"f",
		"dec",
		"t",
		"du",
		"o",
		"nl",
		"lit",
		"obj.x",
		"obj.y",
		"flex.k",
		"added",
		"absent",
		"nl, i",
		"o, s",
		"lit, i DESC",
	];
	for key in keys {
		for direction in ["ASC", "DESC"] {
			for limit in [1, 2, 3, 5] {
				let order = key
					.split(", ")
					.map(|part| {
						if part.contains(' ') {
							part.to_owned()
						} else {
							format!("{part} {direction}")
						}
					})
					.collect::<Vec<_>>()
					.join(", ");
				statements.push((
					format!("SELECT i, s FROM p ORDER BY {order} LIMIT {limit}"),
					format!(
						"SELECT i, s FROM p WHERE type::bool(i IS NONE OR i IS NOT NONE) ORDER BY {order} LIMIT {limit}"
					),
				));
				statements.push((
					format!("SELECT i, s FROM p WHERE i > 0 ORDER BY {order} LIMIT {limit}"),
					format!(
						"SELECT i, s FROM p WHERE type::bool(i > 0) ORDER BY {order} LIMIT {limit}"
					),
				));
			}
		}
	}
	observed::reset();
	for (kept, hidden) in &statements {
		let kept_answers = answers_of(&db, &[kept]).await;
		let hidden_answers = answers_of(&db, &[hidden]).await;
		assert_eq!(kept_answers, hidden_answers, "`{kept}` against `{hidden}`");
	}
	assert!(observed::filtered() > 100, "the filter refused {} packed rows", observed::filtered());
	assert!(observed::probed() > 20, "the probe refused {} packed rows", observed::probed());
}

/// A ranking's vector read from a packed row in place is the vector `extract_vector` reads from the
/// record decoded whole, number for number and kind for kind, through either packed reader and
/// whether or not the scan checked the row first, for every column shape a vector, or a path to
/// one, can take. Neither reader decodes the record, except for a path through an object node,
/// whose fields are columns of their own, a field step over an array that holds values, and `id`,
/// which are read from the decoded record.
#[tokio::test]
async fn a_packed_rows_knn_vector_reads_in_place_as_its_whole_decode_holds_it() {
	use super::row_packed::LazyPackedRow;
	use crate::exec::operators::extract_vector;
	use crate::exec::packed_view::AdmittedRow;
	use crate::val::Number;
	use crate::val::field_path::{FieldPath, FieldPathPart};
	let db = serial_db(
		"
		DEFINE TABLE pv SCHEMAFULL;
		DEFINE FIELD f32 ON pv TYPE array<float>;
		DEFINE FIELD f64 ON pv TYPE option<array<float>>;
		DEFINE FIELD ints ON pv TYPE option<array<int>>;
		DEFINE FIELD anyv ON pv TYPE option<array>;
		DEFINE FIELD n ON pv TYPE option<int>;
		DEFINE FIELD s ON pv TYPE option<string>;
		DEFINE FIELD lit ON pv TYPE option<1 | 2>;
		DEFINE FIELD o ON pv TYPE option<object>;
		DEFINE FIELD o.v ON pv TYPE option<array<float>>;
		DEFINE FIELD flex ON pv TYPE option<object> FLEXIBLE;
		CREATE pv:1 CONTENT { f32: [1f, 0f, 0f], f64: [0.1f, 0.2f, 0.3f], ints: [2, 0, 0],
			anyv: [1, 0.5f, [2]], n: 3, s: 'x', lit: 1, o: { v: [1f, 2f, 3f] },
			flex: { v: [4, 5, 6], w: { v: [1f] } } };
		CREATE pv:2 CONTENT { f32: [], anyv: [[], []], o: {}, flex: {} };
		CREATE pv:3 CONTENT { f32: [1f, 2f], anyv: [{ v: [1f] }], flex: { v: 'x' } };
		",
	)
	.await;
	let rows = rows(&db, "pv").await;
	assert_eq!(rows.len(), 3);
	let layouts = layouts_of(&db, "pv", &rows).await;
	let paths: &[&[&str]] = &[
		&["f32"],
		&["f64"],
		&["ints"],
		&["anyv"],
		&["n"],
		&["n", "v"],
		&["s"],
		&["lit"],
		&["flex"],
		&["flex", "v"],
		&["flex", "w", "v"],
		&["f32", "v"],
		&["missing"],
		&["missing", "v"],
		// Read from the decoded record: a field step over an array holding values may project
		// the field of objects among them.
		&["anyv", "v"],
		&["o"],
		&["o", "v"],
		&["id"],
	];
	let decodes = |parts: &[&str]| matches!(parts, ["o", ..] | ["id"] | ["anyv", _, ..]);
	let path = |parts: &[&str]| {
		FieldPath(parts.iter().map(|p| FieldPathPart::Field((*p).to_owned())).collect())
	};
	let (mut got, mut want) = (vec![Number::Int(-1)], vec![Number::Int(-1)]);
	for (key, value) in &rows {
		assert!(StoredRecord::is_packed_bytes(value), "a packed row");
		let decoded = decode_record_value(key, value, Some(&layouts)).expect("a row decodes");
		let checked = AdmittedRow::of(key, value, Some(&layouts));
		let unchecked = AdmittedRow::Bytes(value);
		let reader = PackedRowReader::new(key, value, Some(&layouts)).expect("reads");
		let lazy_checked = LazyPackedRow::new(key, value, Some(&layouts), &checked);
		let lazy_unchecked = LazyPackedRow::new(key, value, Some(&layouts), &unchecked);
		observed::reset();
		let mut checked_in_place = false;
		let in_place = paths.iter().filter(|parts| !decodes(parts));
		for parts in in_place.chain(paths.iter().filter(|parts| decodes(parts))) {
			if decodes(parts) && !checked_in_place {
				checked_in_place = true;
				assert!(reader.borrowed().is_none(), "the packed reader decoded nothing whole");
				assert_eq!(observed::whole(), 0, "no lazy row was decoded whole");
			}
			let (full, rest) = (path(parts), path(&parts[1..]));
			let name = FieldName::new(parts[0]);
			for limit in [0, 1, 2, 3, 100] {
				let expected = extract_vector(&decoded, &full, &mut want, limit);
				let readers: [(&str, &dyn RowReader); 3] = [
					("packed", &reader),
					("lazy over a checked row", &lazy_checked),
					("lazy over its bytes", &lazy_unchecked),
				];
				for (what, row) in readers {
					let read = row.knn_vector(&full, &name, &rest, &mut got, limit).expect("reads");
					assert_eq!(read, expected, "{what}: {parts:?} at most {limit}");
					if expected {
						assert_eq!(
							format!("{got:?}"),
							format!("{want:?}"),
							"{what}: {parts:?} at most {limit}"
						);
					}
				}
			}
		}
		assert_eq!(observed::admitted(), 1, "a lazy row over its bytes is checked once");
	}
}

/// A brute-force KNN over a SCHEMAFULL table, whose rows are packed, reads each row's vector where
/// its column lies: only the rows the ranking keeps for its sort are decoded, and the answer is the
/// one the same rows give as documents.
#[tokio::test]
async fn a_brute_force_knn_over_packed_rows_decodes_only_the_rows_it_keeps() {
	const ROWS: usize = 2_000;
	let db = serial_db(&format!(
		"
		DEFINE TABLE packed SCHEMAFULL;
		DEFINE FIELD v ON packed TYPE array<float>;
		DEFINE FIELD n ON packed TYPE int;
		DEFINE TABLE loose SCHEMALESS;
		FOR $i IN 0..{ROWS} {{
			LET $v = [($i * 7919 % 1000) * 0.5f, ($i % 13) * 1f];
			CREATE type::record('packed', $i) SET v = $v, n = $i;
			CREATE type::record('loose', $i) SET v = $v, n = $i;
		}};
		"
	))
	.await;
	let owner = TestDb::owner();
	let answer = |sql: String| {
		let db = &db;
		let owner = &owner;
		async move {
			let mut responses = db.datastore().execute(&sql, owner, None).await.expect("runs");
			Value::from(responses.pop().expect("a response").result.expect("answers"))
		}
	};
	let query = |table: &str| {
		format!("SELECT VALUE record::id(id) FROM {table} WHERE v <|5,EUCLIDEAN|> [250f, 6f]")
	};
	let expected = answer(query("loose")).await;
	observed::reset();
	let got = answer(query("packed")).await;
	let whole = observed::whole();
	assert_eq!(got, expected);
	assert!(matches!(&got, Value::Array(ids) if ids.len() == 5), "{got:?}");
	// The rows decoded are the ones the sort kept at some point: a few dozen of 2000, where a
	// ranking that decoded every row would decode them all.
	assert!(whole > 0 && whole < ROWS / 10, "{whole} of {ROWS} rows decoded whole");
}

/// Every generation this build mints is verified as it is minted, so its rows, documents and
/// packed rows alike, are admitted for an in-place read without a walk; a reader that trusts no
/// verification vouches for none of them and walks each.
#[tokio::test]
async fn the_generations_a_build_mints_are_verified() {
	let db = serial_db(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &rows).await;
	let unverified = layouts.without_verification();
	let (mut documents, mut packed) = (0usize, 0usize);
	for (_, value) in &rows {
		let record = StoredRecord::parse(value).expect("parses").expect("a v1 record");
		assert!(layouts.vouches_for(record.stamp()), "stamp {} is vouched for", record.stamp());
		assert!(!unverified.vouches_for(record.stamp()));
		if record.is_packed() {
			packed += 1;
		} else {
			documents += 1;
			let document =
				record.admit_document(Some(&layouts)).expect("admitted").expect("a document");
			assert!(document.is_verified(), "a document of a verified generation is unwalked");
			let walked =
				record.admit_document(Some(&unverified)).expect("admitted").expect("a document");
			assert!(!walked.is_verified(), "without verification the document is walked");
		}
	}
	assert!(documents > 0 && packed > 0, "{documents} documents, {packed} packed rows");
}
