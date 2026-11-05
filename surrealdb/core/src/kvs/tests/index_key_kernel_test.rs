//! Every path that writes or reads a B-tree index entry agrees on its bytes.
//!
//! An entry is written by a statement's own index maintenance and by an index
//! build over existing records; it is read through the ranges a scan computes
//! for a predicate. All three take their keys from the B-tree key kernels
//! (`idx::kernels::btree`). These tests write records through each write path
//! and check, byte for byte, that:
//!
//! - both write paths store exactly the keys the kernel encodes, and the same keys as each other;
//! - each entry names a record whose values the kernel encodes to that entry's key;
//! - each entry lies in the equality range the read path computes for its values, and the indexed
//!   read answers what the unindexed read answers.
//!
//! The values cover every kind an index column holds, the numbers whose key
//! bytes are shared across kinds or sit at the edges of the numeric order
//! (signed zeros, NaNs, infinities, the `i64` bounds and their float
//! neighbours, decimals finer than any `f64`), nested arrays in a compound
//! index, and record ids whose keys are objects or differ only in number kind.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use geo_types::{LineString, MultiPoint, Point, Polygon};
use rust_decimal::Decimal;
use surrealdb_types::Variables;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, TableProvider};
use crate::dbs::Session;
use crate::expr::BinaryOperator;
use crate::idx::kernels::btree::IndexKeyspace;
use crate::kvs::{Datastore, TransactionType};
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::{
	Array, Geometry, Number, RecordId, RecordIdKey, TableName, Value, convert_value_to_public_value,
};

/// One literal per value kind an index column can hold, each distinct from
/// the others under the index format so a `UNIQUE` index takes them all.
/// An array fans out into one entry per element.
const KINDS: &[&str] = &[
	"NONE",
	"NULL",
	"false",
	"true",
	"-7",
	"0",
	"2.5f",
	"3.75dec",
	"1e100",
	"'a'",
	"'a\\u{0}b'",
	"'zz'",
	"''",
	"1h30m",
	"d'2024-01-02T03:04:05Z'",
	"u'0189f2b8-6a60-7000-8000-000000000000'",
	"[10, 'x']",
	"[]",
	"{ k: 1, n: [2] }",
	"other:1",
	"other:['a', 2]",
	"other:{ a: 1, b: 'x' }",
	"(1.5, 2.5)",
	"b'00ff'",
];

fn int(v: i64) -> Value {
	Value::Number(Number::Int(v))
}

fn float(v: f64) -> Value {
	Value::Number(Number::Float(v))
}

fn dec(v: &str) -> Value {
	Value::Number(Number::Decimal(Decimal::from_str(v).unwrap()))
}

fn other(key: Value) -> Value {
	Value::RecordId(RecordId::new(TableName::from("other"), RecordIdKey::Array(Array(vec![key]))))
}

/// `2^63`, the least `f64` above `i64::MAX`.
const TWO_POW_63: f64 = (1u64 << 63) as f64;

/// The `f64` next to `v`, towards `+inf` when `up`.
fn next_f64(v: f64, up: bool) -> f64 {
	let bits = v.to_bits();
	// Moving away from zero increments the magnitude bits.
	let away = up == v.is_sign_positive();
	f64::from_bits(if away {
		bits + 1
	} else {
		bits - 1
	})
}

/// A NaN with its sign bit set.
fn negative_nan() -> f64 {
	f64::from_bits(f64::NAN.to_bits() | (1 << 63))
}

/// A positive NaN with a payload.
fn payload_nan() -> f64 {
	f64::from_bits(0x7ff8_0000_0000_0001)
}

/// Values a SurrealQL literal does not spell exactly, built here, each
/// distinct from the others and from [`KINDS`] under the index format.
fn special_values() -> Vec<Value> {
	let square = LineString::from(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.0, 0.0)]);
	vec![
		float(f64::NAN),
		float(f64::INFINITY),
		float(f64::NEG_INFINITY),
		// A decimal finer than any `f64`, next to the integer it rounds to.
		int(1),
		dec("1.0000000000000000000000000001"),
		int(i64::MAX),
		int(i64::MIN),
		float(TWO_POW_63),
		float(next_f64(TWO_POW_63, false)),
		float(next_f64(-TWO_POW_63, true)),
		float(next_f64(-TWO_POW_63, false)),
		other(int(1)),
		Value::Geometry(Geometry::Line(LineString::from(vec![(0.0, 0.0), (1.0, 1.0)]))),
		Value::Geometry(Geometry::Polygon(Polygon::new(square, vec![]))),
		Value::Geometry(Geometry::MultiPoint(MultiPoint::from(vec![
			Point::new(1.0, 2.0),
			Point::new(3.0, 4.0),
		]))),
		Value::Geometry(Geometry::Collection(vec![
			Geometry::Point(Point::new(5.0, 6.0)),
			Geometry::Line(LineString::from(vec![(7.0, 8.0), (9.0, 10.0)])),
		])),
	]
}

/// Sets of values the index format spells with the same bytes, so a `UNIQUE`
/// index holds one of each set.
fn byte_equal_sets() -> Vec<Vec<Value>> {
	vec![
		vec![int(0), float(0.0), float(-0.0), dec("0")],
		vec![float(f64::NAN), float(negative_nan()), float(payload_nan())],
		vec![other(int(1)), other(float(1.0)), other(dec("1"))],
		vec![int(i64::MIN), float(-TWO_POW_63), dec("-9223372036854775808")],
		vec![float(TWO_POW_63), dec("9223372036854775808")],
	]
}

/// Every value at most once under the index format: the first set holds
/// [`KINDS`] and [`special_values`], and each later set the byte-equal
/// partners of values in earlier sets.
async fn distinct_sets(ds: &Datastore, ses: &Session) -> Vec<Vec<Value>> {
	let mut first = Vec::new();
	for lit in KINDS {
		first.push(run(ds, ses, &format!("RETURN {lit};"), &[]).await.remove(0));
	}
	first.extend(special_values());
	let mut sets = vec![first];
	for partners in byte_equal_sets() {
		for (i, v) in partners.into_iter().skip(1).enumerate() {
			if sets.len() <= i + 1 {
				sets.push(Vec::new());
			}
			sets[i + 1].push(v);
		}
	}
	sets
}

/// One record: its id's key, and its values for the index's columns.
struct Row {
	id: RecordIdKey,
	values: Vec<Value>,
}

fn rows(values: impl IntoIterator<Item = Value>) -> Vec<Row> {
	values
		.into_iter()
		.enumerate()
		.map(|(i, v)| Row {
			id: RecordIdKey::Number(i as i64),
			values: vec![v],
		})
		.collect()
}

fn bind(vars: &[(&str, &Value)]) -> Variables {
	let mut out = Variables::new();
	for (k, v) in vars {
		out.insert(*k, convert_value_to_public_value((*v).clone()).unwrap());
	}
	out
}

/// Each statement's result, or its error's message.
async fn try_run(
	ds: &Datastore,
	ses: &Session,
	sql: &str,
	vars: &[(&str, &Value)],
) -> Vec<Result<Value, String>> {
	ds.execute(sql, ses, Some(bind(vars)))
		.await
		.unwrap()
		.into_iter()
		.map(|r| r.result.map(convert_public_value_to_internal).map_err(|e| e.to_string()))
		.collect()
}

async fn run(ds: &Datastore, ses: &Session, sql: &str, vars: &[(&str, &Value)]) -> Vec<Value> {
	try_run(ds, ses, sql, vars)
		.await
		.into_iter()
		.map(|r| r.unwrap_or_else(|e| panic!("{sql}: {e}")))
		.collect()
}

async fn datastore() -> (Arc<Datastore>, Session) {
	let ds = Datastore::new("memory").await.unwrap();
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
	}
	(ds, Session::owner().with_ns("test").with_db("test"))
}

/// A keyspace for comparing values' key bytes outside any datastore: the
/// bytes after the keyspace's prefix do not depend on the prefix.
fn keyspace(tb: &TableName) -> IndexKeyspace<'_> {
	IndexKeyspace::new(
		crate::catalog::NamespaceId(1),
		crate::catalog::DatabaseId(2),
		tb,
		crate::catalog::IndexId(3),
		crate::catalog::INDEX_FORMAT_VERSION,
	)
}

fn record(tb: &str, id: &RecordIdKey) -> Value {
	Value::RecordId(RecordId::new(TableName::from(tb), id.clone()))
}

fn column_vars(values: &[Value]) -> Vec<(String, &Value)> {
	values.iter().enumerate().map(|(i, v)| (format!("c{i}"), v)).collect()
}

/// `a = $c0, b = $c1`, or with `AND` between the terms.
fn terms(cols: &[&str], sep: &str) -> String {
	cols.iter().enumerate().map(|(i, c)| format!("{c} = $c{i}")).collect::<Vec<_>>().join(sep)
}

/// Writes each row as its own statement, which maintains any index the
/// table already has.
async fn create(ds: &Datastore, ses: &Session, tb: &str, cols: &[&str], rows: &[Row]) {
	let sql = format!("CREATE $id SET {};", terms(cols, ", "));
	for row in rows {
		let id = record(tb, &row.id);
		let named = column_vars(&row.values);
		let mut vars: Vec<(&str, &Value)> = named.iter().map(|(k, v)| (k.as_str(), *v)).collect();
		vars.push(("id", &id));
		run(ds, ses, &sql, &vars).await;
	}
}

/// A record id compared with its number kinds, which `Value` equality
/// erases.
fn exact(rid: &RecordId) -> String {
	format!("{rid:?}")
}

/// A value compared with its float bits, sign and NaN payload included,
/// which `Value` equality and `Debug` both erase.
fn bits(v: &Value) -> String {
	if let Value::Number(Number::Float(f)) = v {
		format!("f64({:#018x})", f.to_bits())
	} else {
		format!("{v:?}")
	}
}

/// The ids a query answers, compared with their number kinds.
fn exact_ids(values: Vec<Value>) -> Vec<String> {
	let mut out: Vec<String> = values
		.into_iter()
		.flat_map(|v| {
			if let Value::Array(Array(items)) = v {
				items
			} else {
				vec![v]
			}
		})
		.map(|v| {
			let Value::RecordId(rid) = v else {
				panic!("not a record id: {v:?}");
			};
			exact(&rid)
		})
		.collect();
	out.sort();
	out
}

/// The entries of index `ix` on `tb`, keyed past the index's own prefix.
struct Entries {
	/// Each stored entry, with the record id its value names.
	stored: BTreeMap<Vec<u8>, RecordId>,
	/// Each key the kernel encodes for the table's records, with every record
	/// encoded to it.
	encoded: BTreeMap<Vec<u8>, Vec<RecordId>>,
}

async fn entries(ds: &Datastore, ses: &Session, tb: &str, cols: &[&str], rows: &[Row]) -> Entries {
	let read = format!("SELECT VALUE [{}] FROM ONLY $id;", cols.join(", "));
	let mut stored_values = Vec::with_capacity(rows.len());
	for row in rows {
		let id = record(tb, &row.id);
		let Value::Array(Array(values)) = run(ds, ses, &read, &[("id", &id)]).await.remove(0)
		else {
			panic!("{read}: not an array");
		};
		let written: Vec<String> = row.values.iter().map(bits).collect();
		let read_back: Vec<String> = values.iter().map(bits).collect();
		assert_eq!(read_back, written, "{tb}: a record does not store the values written");
		stored_values.push(values);
	}
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let db = tx.expect_db_by_name("test", "test").await.unwrap();
	let table = TableName::from(tb);
	let ix = tx.expect_tb_index(db.namespace_id, db.database_id, &table, "ix").await.unwrap();
	let unique = matches!(ix.index, crate::catalog::Index::Uniq);
	let ks = IndexKeyspace::of(db.namespace_id, db.database_id, &ix);
	let all = ks.all().unwrap();
	let prefix = all.start().as_slice().to_vec();
	let stored = tx
		.scan(all, 10_000, 0, None)
		.await
		.unwrap()
		.into_iter()
		.map(|(k, entry)| {
			assert!(k.starts_with(&prefix), "an entry outside the index prefix");
			(k[prefix.len()..].to_vec(), entry.rid)
		})
		.collect();
	let mut encoded: BTreeMap<Vec<u8>, Vec<RecordId>> = BTreeMap::new();
	for (row, values) in rows.iter().zip(stored_values) {
		for fd in crate::idx::index::Indexable::new(values, &ix) {
			let key = ks.stored_key(unique, &fd, &row.id).unwrap();
			let key = key.key.as_slice();
			// The read path's range for the entry's values holds it: the
			// equality range on a single column, and on a compound index both
			// the range on its leading values and the one on all of them.
			let ranges = match fd.as_slice() {
				[value] => vec![ks.equality(unique, value).unwrap()],
				[first, .., last] => vec![
					ks.compound(
						&fd.as_slice()[..fd.len() - 1],
						Some((&BinaryOperator::Equal, last)),
					)
					.unwrap(),
					ks.compound(std::slice::from_ref(first), None).unwrap(),
				],
				[] => vec![],
			};
			for range in ranges {
				assert!(
					range.start().as_slice() <= key && key < range.end().as_slice(),
					"{tb}: the read range on {fd:?} misses its entry"
				);
			}
			encoded
				.entry(key[prefix.len()..].to_vec())
				.or_default()
				.push(RecordId::new(table.clone(), row.id.clone()));
		}
	}
	Entries {
		stored,
		encoded,
	}
}

/// The stored entries are exactly the kernel's keys, and each names a record
/// the kernel encodes to it.
fn assert_kernel_bytes(tb: &str, path: &str, e: &Entries) {
	assert!(!e.stored.is_empty(), "{tb}: no entries");
	let stored: BTreeSet<_> = e.stored.keys().collect();
	let encoded: BTreeSet<_> = e.encoded.keys().collect();
	assert_eq!(stored, encoded, "{tb}: {path}'s writes differ from the kernel's keys");
	for (key, rid) in &e.stored {
		let owners: Vec<String> = e.encoded[key].iter().map(exact).collect();
		assert!(
			owners.contains(&exact(rid)),
			"{tb}: an entry names {rid:?}, not one of the records encoded to it: {owners:?}"
		);
	}
}

/// Indexes `rows` over `cols` through a statement and through a build, and
/// checks both against the kernel and the indexed reads against unindexed
/// ones. `name` prefixes the two tables.
async fn written_and_built_agree(
	ds: &Datastore,
	ses: &Session,
	name: &str,
	cols: &[&str],
	rows: &[Row],
	unique: bool,
) {
	let kind = if unique {
		" UNIQUE"
	} else {
		""
	};
	let fields = cols.join(", ");
	let (w, b) = (format!("{name}_w"), format!("{name}_b"));
	// `w`: entries written by each record's own statement.
	run(ds, ses, &format!("DEFINE TABLE {w}; DEFINE INDEX ix ON {w} FIELDS {fields}{kind};"), &[])
		.await;
	create(ds, ses, &w, cols, rows).await;
	// `b`: entries written by building the index over existing records.
	run(ds, ses, &format!("DEFINE TABLE {b};"), &[]).await;
	create(ds, ses, &b, cols, rows).await;
	run(ds, ses, &format!("DEFINE INDEX ix ON {b} FIELDS {fields}{kind};"), &[]).await;

	let written = entries(ds, ses, &w, cols, rows).await;
	let built = entries(ds, ses, &b, cols, rows).await;
	assert_kernel_bytes(&w, "a statement", &written);
	assert_kernel_bytes(&b, "an index build", &built);
	assert_eq!(
		written.stored.keys().collect::<Vec<_>>(),
		built.stored.keys().collect::<Vec<_>>(),
		"{name}: a statement and an index build wrote different entries"
	);

	// Every row reads the same through the index as without it. An array is
	// indexed by its elements, so an equality to a whole array is answered by
	// comparing whole values, never by a lookup of the element entries.
	let cond = terms(cols, " AND ");
	for row in rows {
		let named = column_vars(&row.values);
		let vars: Vec<(&str, &Value)> = named.iter().map(|(k, v)| (k.as_str(), *v)).collect();
		let indexed = format!("SELECT VALUE id FROM {w} WITH INDEX ix WHERE {cond};");
		let indexed = run(ds, ses, &indexed, &vars).await;
		let scanned = format!("SELECT VALUE id FROM {w} WITH NOINDEX WHERE {cond};");
		let scanned = run(ds, ses, &scanned, &vars).await;
		assert_eq!(exact_ids(indexed), exact_ids(scanned), "{w}: WHERE {cond} on {:?}", row.values);
	}
}

/// The ids `SELECT ... WHERE {cond}` answers on `tb`, checked to be the same
/// through the index as without it.
async fn read_both_ways(
	ds: &Datastore,
	ses: &Session,
	tb: &str,
	cond: &str,
	vars: &[(&str, &Value)],
) -> Vec<String> {
	let indexed = format!("SELECT VALUE id FROM {tb} WITH INDEX ix WHERE {cond};");
	let indexed = exact_ids(run(ds, ses, &indexed, vars).await);
	let scanned = format!("SELECT VALUE id FROM {tb} WITH NOINDEX WHERE {cond};");
	let scanned = exact_ids(run(ds, ses, &scanned, vars).await);
	assert_eq!(indexed, scanned, "{tb}: WHERE {cond} on {vars:?}");
	indexed
}

async fn every_kind_agrees(unique: bool) {
	let (ds, ses) = datastore().await;
	let sets = distinct_sets(&ds, &ses).await;
	if unique {
		for (i, set) in sets.into_iter().enumerate() {
			written_and_built_agree(&ds, &ses, &format!("s{i}"), &["v"], &rows(set), true).await;
		}
	} else {
		let all = rows(sets.into_iter().flatten());
		written_and_built_agree(&ds, &ses, "s", &["v"], &all, false).await;
	}
}

#[tokio::test]
async fn a_non_unique_index_is_written_built_and_read_with_the_same_bytes() {
	every_kind_agrees(false).await;
}

#[tokio::test]
async fn a_unique_index_is_written_built_and_read_with_the_same_bytes() {
	every_kind_agrees(true).await;
}

/// Every number in the special-value and byte-equal sets, with the
/// neighbours a range bound sits between.
fn numbers() -> Vec<Value> {
	let mut out: Vec<Value> = special_values()
		.into_iter()
		.chain(byte_equal_sets().into_iter().flatten())
		.filter(|v| matches!(v, Value::Number(_)))
		.collect();
	out.extend([int(-1), float(1e300), float(-1e300), float(2.5), dec("2.5")]);
	out
}

/// A range over numbers reads through the index what it reads without it:
/// NaN sorts above `+inf`, the two zeros are one value, and a decimal finer
/// than an `f64` keeps its place.
async fn a_range_crossing_special_numbers_agrees(unique: bool) {
	let (ds, ses) = datastore().await;
	let kind = if unique {
		" UNIQUE"
	} else {
		""
	};
	// A `UNIQUE` index holds one of each byte-equal set.
	let values: Vec<Value> = if unique {
		let mut seen = BTreeSet::new();
		let tb = TableName::from("r");
		let ks = keyspace(&tb);
		numbers()
			.into_iter()
			.filter(|v| {
				seen.insert(ks.unique_key(std::slice::from_ref(v)).unwrap().as_slice().to_vec())
			})
			.collect()
	} else {
		numbers()
	};
	let rows = rows(values);
	run(&ds, &ses, &format!("DEFINE TABLE r; DEFINE INDEX ix ON r FIELDS v{kind};"), &[]).await;
	create(&ds, &ses, "r", &["v"], &rows).await;
	let id_of = |pred: &dyn Fn(&Value) -> bool| -> Vec<String> {
		let mut out: Vec<String> = rows
			.iter()
			.filter(|r| pred(&r.values[0]))
			.map(|r| exact(&RecordId::new(TableName::from("r"), r.id.clone())))
			.collect();
		out.sort();
		out
	};
	let is_nan = |v: &Value| matches!(v, Value::Number(Number::Float(f)) if f.is_nan());

	let bounds = [
		int(0),
		float(-0.0),
		float(1e300),
		float(f64::INFINITY),
		float(f64::NEG_INFINITY),
		float(f64::NAN),
		float(negative_nan()),
		int(1),
		dec("1.0000000000000000000000000001"),
		int(i64::MAX),
		int(i64::MIN),
		float(TWO_POW_63),
		float(-TWO_POW_63),
	];
	for x in &bounds {
		for op in ["<", "<=", "=", ">=", ">"] {
			read_both_ways(&ds, &ses, "r", &format!("v {op} $x"), &[("x", x)]).await;
		}
	}

	// Above every finite number: `+inf`, then every NaN.
	let above = read_both_ways(&ds, &ses, "r", "v > $x", &[("x", &float(1e300))]).await;
	let inf_or_nan = |v: &Value| {
		if let Value::Number(Number::Float(f)) = v {
			f.is_nan() || *f == f64::INFINITY
		} else {
			false
		}
	};
	assert_eq!(above, id_of(&inf_or_nan), "v > 1e300");
	assert!(above.iter().any(|id| id_of(&is_nan).contains(id)), "v > 1e300 finds no NaN");
	// No NaN is below zero.
	let below = read_both_ways(&ds, &ses, "r", "v < $x", &[("x", &int(0))]).await;
	let nans = id_of(&is_nan);
	assert!(!below.iter().any(|id| nans.contains(id)), "v < 0 finds a NaN: {below:?}");
	assert!(!below.is_empty(), "v < 0 finds nothing");
	// Both zeros, of every kind, equal zero.
	let zero = read_both_ways(&ds, &ses, "r", "v = $x", &[("x", &int(0))]).await;
	let is_zero = |v: &Value| {
		if let Value::Number(n) = v {
			match n {
				Number::Int(i) => *i == 0,
				Number::Float(f) => *f == 0.0,
				Number::Decimal(d) => d.is_zero(),
				Number::BigDecimal(d) => d.is_zero(),
				Number::I8(i) => *i == 0,
				Number::U8(u) => *u == 0,
				Number::I16(i) => *i == 0,
				Number::U16(u) => *u == 0,
				Number::I32(i) => *i == 0,
				Number::U32(u) => *u == 0,
				Number::U64(u) => *u == 0,
				Number::F16(f) => f.to_bits() == 0 || f.to_bits() == 0x8000,
				Number::F32(f) => *f == 0.0,
			}
		} else {
			false
		}
	};
	assert_eq!(zero, id_of(&is_zero), "v = 0");
}

#[tokio::test]
async fn a_non_unique_range_crossing_special_numbers_reads_as_a_scan() {
	a_range_crossing_special_numbers_agrees(false).await;
}

#[tokio::test]
async fn a_unique_range_crossing_special_numbers_reads_as_a_scan() {
	a_range_crossing_special_numbers_agrees(true).await;
}

/// The key bytes of the numbers the index format equates, and of those it
/// keeps apart however near.
#[test]
fn byte_equal_numbers_share_key_bytes_and_near_ones_do_not() {
	let tb = TableName::from("k");
	let ks = keyspace(&tb);
	let key = |v: &Value| ks.unique_key(std::slice::from_ref(v)).unwrap().as_slice().to_vec();
	for set in byte_equal_sets() {
		let first = key(&set[0]);
		for v in &set[1..] {
			assert_eq!(key(v), first, "{v:?} and {:?} spell different keys", set[0]);
		}
	}
	// Ascending, each strictly below the next.
	let ascending = [
		float(f64::NEG_INFINITY),
		float(next_f64(-TWO_POW_63, false)),
		int(i64::MIN),
		float(next_f64(-TWO_POW_63, true)),
		int(-1),
		int(0),
		dec("0.1"),
		float(0.1),
		int(1),
		dec("1.0000000000000000000000000001"),
		float(next_f64(TWO_POW_63, false)),
		int(i64::MAX),
		float(TWO_POW_63),
		float(1e300),
		float(f64::INFINITY),
		float(f64::NAN),
	];
	for pair in ascending.windows(2) {
		assert!(key(&pair[0]) < key(&pair[1]), "{:?} does not sort below {:?}", pair[0], pair[1]);
	}
}

/// On a `UNIQUE` index, values the index format spells with the same bytes
/// are one key: a second record holding any of them is refused, by a
/// statement and by an index build.
#[tokio::test]
async fn a_unique_index_refuses_byte_equal_values() {
	let (ds, ses) = datastore().await;
	for (s, set) in byte_equal_sets().into_iter().enumerate() {
		for (i, first) in set.iter().enumerate() {
			for (j, second) in set.iter().enumerate() {
				if i == j {
					continue;
				}
				let tb = format!("u{s}_{i}_{j}");
				run(
					&ds,
					&ses,
					&format!("DEFINE TABLE {tb}; DEFINE INDEX ix ON {tb} FIELDS v UNIQUE;"),
					&[],
				)
				.await;
				let first_id = record(&tb, &RecordIdKey::Number(1));
				run(&ds, &ses, "CREATE $id SET v = $v;", &[("id", &first_id), ("v", first)]).await;
				let second_id = record(&tb, &RecordIdKey::Number(2));
				let res = try_run(
					&ds,
					&ses,
					"CREATE $id SET v = $v;",
					&[("id", &second_id), ("v", second)],
				)
				.await;
				match &res[..] {
					[Err(e)] => assert!(e.contains("already contains"), "{tb}: {e}"),
					res => panic!("{tb}: {second:?} after {first:?} was not refused: {res:?}"),
				}
			}
		}
		let tb = format!("ub{s}");
		run(&ds, &ses, &format!("DEFINE TABLE {tb};"), &[]).await;
		create(&ds, &ses, &tb, &["v"], &rows(set.clone())).await;
		let res =
			try_run(&ds, &ses, &format!("DEFINE INDEX ix ON {tb} FIELDS v UNIQUE;"), &[]).await;
		match &res[..] {
			[Err(e)] => assert!(e.contains("already contains"), "{tb}: {e}"),
			res => panic!("{tb}: building over {set:?} was not refused: {res:?}"),
		}
	}
}

/// A compound index over a column of nested arrays holds one entry per
/// element of the outer array, each element whole.
async fn nested_arrays_in_a_compound_column(unique: bool) {
	let (ds, ses) = datastore().await;
	let arr = |items: Vec<Value>| Value::Array(Array(items));
	let pair = |a: i64, b: i64| arr(vec![int(a), int(b)]);
	let rows: Vec<Row> = [
		(int(1), arr(vec![pair(1, 2), arr(vec![int(3)])])),
		(int(1), arr(vec![arr(vec![int(4)])])),
		(int(2), arr(vec![pair(1, 2), arr(vec![int(3)])])),
		(int(1), arr(vec![arr(vec![int(1), arr(vec![int(2)])])])),
		(float(-0.0), arr(vec![arr(vec![float(f64::NAN)]), arr(vec![])])),
	]
	.into_iter()
	.enumerate()
	.map(|(i, (a, b))| Row {
		id: RecordIdKey::Number(i as i64),
		values: vec![a, b],
	})
	.collect();
	let name = if unique {
		"cu"
	} else {
		"cn"
	};
	written_and_built_agree(&ds, &ses, name, &["a", "b"], &rows, unique).await;
	let tb = format!("{name}_w");
	for row in &rows {
		let a = &row.values[0];
		read_both_ways(&ds, &ses, &tb, "a = $a", &[("a", a)]).await;
		let Value::Array(Array(elements)) = &row.values[1] else {
			unreachable!()
		};
		for e in elements {
			read_both_ways(&ds, &ses, &tb, "a = $a AND b CONTAINS $e", &[("a", a), ("e", e)]).await;
		}
	}
}

#[tokio::test]
async fn a_non_unique_compound_index_holds_nested_arrays_with_the_same_bytes() {
	nested_arrays_in_a_compound_column(false).await;
}

#[tokio::test]
async fn a_unique_compound_index_holds_nested_arrays_with_the_same_bytes() {
	nested_arrays_in_a_compound_column(true).await;
}

/// Record ids whose keys differ only in a nested number's kind.
fn kind_ids() -> Vec<RecordIdKey> {
	[int(1), float(1.0), dec("1")].into_iter().map(|v| RecordIdKey::Array(Array(vec![v]))).collect()
}

/// An entry's value names its record exactly, number kind included, so
/// records whose ids differ only in a nested number's kind each read back as
/// themselves when their indexed values differ.
#[tokio::test]
async fn an_entry_names_its_record_with_its_number_kind() {
	let (ds, ses) = datastore().await;
	for unique in [false, true] {
		let rows: Vec<Row> = kind_ids()
			.into_iter()
			.enumerate()
			.map(|(i, id)| Row {
				id,
				values: vec![int(10 + i as i64)],
			})
			.collect();
		let name = if unique {
			"ku"
		} else {
			"kn"
		};
		written_and_built_agree(&ds, &ses, name, &["v"], &rows, unique).await;
		let e = entries(&ds, &ses, &format!("{name}_w"), &["v"], &rows).await;
		let mut named: Vec<String> = e.stored.values().map(exact).collect();
		named.sort();
		let mut expected: Vec<String> = rows
			.iter()
			.map(|r| exact(&RecordId::new(TableName::from(format!("{name}_w")), r.id.clone())))
			.collect();
		expected.sort();
		assert_eq!(named, expected, "{name}: an entry lost its record's number kind");
	}
}

/// From the stamp that spells it as a `RecordIdentity`, a non-unique entry's
/// key keeps the number kind of its record's id: records whose ids differ only
/// there, holding the same indexed value, have an entry each, and an indexed
/// read answers every one of them, as a scan does.
#[tokio::test]
async fn records_differing_only_in_id_number_kind_have_an_entry_each() {
	let (ds, ses) = datastore().await;
	run(&ds, &ses, "DEFINE TABLE kd; DEFINE INDEX ix ON kd FIELDS v;", &[]).await;
	assert!(crate::catalog::spells_identity_entry_ids(index_format(&ds, "kd").await));
	let rows: Vec<Row> = kind_ids()
		.into_iter()
		.map(|id| Row {
			id,
			values: vec![int(5)],
		})
		.collect();
	create(&ds, &ses, "kd", &["v"], &rows).await;
	let e = entries(&ds, &ses, "kd", &["v"], &rows).await;
	assert_kernel_bytes("kd", "a statement", &e);
	assert_eq!(e.stored.len(), rows.len(), "kd: {:?}", e.stored);
	let mut every: Vec<String> =
		rows.iter().map(|r| exact(&RecordId::new(TableName::from("kd"), r.id.clone()))).collect();
	every.sort();
	let read = |noindex: bool| {
		let with = if noindex {
			"NOINDEX"
		} else {
			"INDEX ix"
		};
		format!("SELECT VALUE id FROM kd WITH {with} WHERE v = $x;")
	};
	for noindex in [false, true] {
		let mut ids = exact_ids(run(&ds, &ses, &read(noindex), &[("x", &int(5))]).await);
		ids.sort();
		assert_eq!(ids, every, "noindex={noindex}");
	}
}

/// An index stamped before ids are spelled as a `RecordIdentity` keeps the
/// spelling it was written with: records whose ids differ only in a nested
/// number's kind share one entry, which names the record written last, and an
/// indexed read answers that record alone. `REBUILD INDEX` restamps it, and
/// the rebuilt index has an entry for each.
#[tokio::test]
async fn an_older_index_shares_one_entry_for_ids_differing_in_number_kind_until_rebuilt() {
	let (ds, ses) = datastore().await;
	run(&ds, &ses, "DEFINE TABLE kd; DEFINE INDEX ix ON kd FIELDS v;", &[]).await;
	let older = crate::catalog::BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION - 1;
	stamp_index_format(&ds, "kd", older).await;
	assert_eq!(index_format(&ds, "kd").await, older);
	let rows: Vec<Row> = kind_ids()
		.into_iter()
		.map(|id| Row {
			id,
			values: vec![int(5)],
		})
		.collect();
	create(&ds, &ses, "kd", &["v"], &rows).await;
	let e = entries(&ds, &ses, "kd", &["v"], &rows).await;
	assert_kernel_bytes("kd", "a statement", &e);
	assert_eq!(e.stored.len(), 1, "kd: {:?}", e.stored);
	let last = exact(&RecordId::new(TableName::from("kd"), rows.last().unwrap().id.clone()));
	assert_eq!(e.stored.values().map(exact).collect::<Vec<_>>(), vec![last.clone()]);
	let indexed =
		run(&ds, &ses, "SELECT VALUE id FROM kd WITH INDEX ix WHERE v = $x;", &[("x", &int(5))])
			.await;
	assert_eq!(exact_ids(indexed), vec![last]);
	let scanned =
		run(&ds, &ses, "SELECT VALUE id FROM kd WITH NOINDEX WHERE v = $x;", &[("x", &int(5))])
			.await;
	assert_eq!(exact_ids(scanned).len(), rows.len());

	run(&ds, &ses, "REBUILD INDEX ix ON kd;", &[]).await;
	assert!(crate::catalog::spells_identity_entry_ids(index_format(&ds, "kd").await));
	let e = entries(&ds, &ses, "kd", &["v"], &rows).await;
	assert_kernel_bytes("kd", "a rebuild", &e);
	assert_eq!(e.stored.len(), rows.len(), "kd after the rebuild: {:?}", e.stored);
	let indexed =
		run(&ds, &ses, "SELECT VALUE id FROM kd WITH INDEX ix WHERE v = $x;", &[("x", &int(5))])
			.await;
	assert_eq!(exact_ids(indexed).len(), rows.len());
}

/// A float and a decimal that agree to an `f64`'s precision but not exactly
/// are two keys, each spelling its exact value, and two values: value
/// comparison orders a float by the expansion its key spells. So an indexed
/// and an unindexed read answer the same records, for an equality and for
/// every bound on either of the pair, on a plain index and on a unique one,
/// over an undeclared column and over one declared a number kind. A range the
/// index answers by its keys alone and one it rechecks against the values
/// admit the same records, as the key order is the value order.
#[tokio::test]
async fn a_float_and_a_finer_decimal_are_two_keys_read_by_their_exact_values() {
	let (ds, ses) = datastore().await;
	let values = [float(0.1), dec("0.1"), float(1.0), dec("1.0000000000000000000000000001")];
	let tb = TableName::from("x");
	let ks = keyspace(&tb);
	let key = |v: &Value| ks.unique_key(std::slice::from_ref(v)).unwrap().as_slice().to_vec();
	assert!(key(&dec("0.1")) < key(&float(0.1)));
	assert!(key(&float(1.0)) < key(&dec("1.0000000000000000000000000001")));
	for a in &values {
		for b in &values {
			assert_eq!(a.cmp(b), key(a).cmp(&key(b)), "{a:?} against {b:?}");
		}
	}
	for (unique, declared) in [(false, false), (true, false), (false, true), (true, true)] {
		let tb = match (unique, declared) {
			(false, false) => "xn",
			(true, false) => "xu",
			(false, true) => "xnd",
			(true, true) => "xud",
		};
		let kind = if unique {
			" UNIQUE"
		} else {
			""
		};
		let field = if declared {
			format!("DEFINE FIELD v ON {tb} TYPE number;")
		} else {
			String::new()
		};
		let rows = rows(values.clone());
		run(
			&ds,
			&ses,
			&format!("DEFINE TABLE {tb}; {field} DEFINE INDEX ix ON {tb} FIELDS v{kind};"),
			&[],
		)
		.await;
		create(&ds, &ses, tb, &["v"], &rows).await;
		assert_kernel_bytes(tb, "a statement", &entries(&ds, &ses, tb, &["v"], &rows).await);
		let ids = |admit: &dyn Fn(&Value) -> bool| -> Vec<String> {
			let mut out: Vec<String> = rows
				.iter()
				.filter(|r| admit(&r.values[0]))
				.map(|r| exact(&RecordId::new(TableName::from(tb), r.id.clone())))
				.collect();
			out.sort();
			out
		};
		for x in &values {
			for op in ["<", "<=", "=", ">=", ">"] {
				let admits = |o: std::cmp::Ordering| match op {
					"<" => o.is_lt(),
					"<=" => o.is_le(),
					"=" => o.is_eq(),
					">=" => o.is_ge(),
					_ => o.is_gt(),
				};
				let expected = ids(&|v| admits(v.cmp(x)));
				let sql = format!("SELECT VALUE id FROM {tb} WITH INDEX ix WHERE v {op} $x;");
				let indexed = exact_ids(run(&ds, &ses, &sql, &[("x", x)]).await);
				assert_eq!(indexed, expected, "{tb}: indexed v {op} {x:?}");
				let sql = format!("SELECT VALUE id FROM {tb} WITH NOINDEX WHERE v {op} $x;");
				let scanned = exact_ids(run(&ds, &ses, &sql, &[("x", x)]).await);
				assert_eq!(scanned, expected, "{tb}: scanned v {op} {x:?}");
			}
		}
		let sql = |hint: &str| format!("SELECT VALUE id FROM {tb} WITH {hint} WHERE v = $x;");
		let x = float(0.1);
		let indexed = exact_ids(run(&ds, &ses, &sql("INDEX ix"), &[("x", &x)]).await);
		let scanned = exact_ids(run(&ds, &ses, &sql("NOINDEX"), &[("x", &x)]).await);
		assert_eq!((indexed.len(), scanned.len()), (1, 1), "{tb}: v = 0.1f");
	}
}

// ------------------------------
// Decimals spelled with trailing zeros
// ------------------------------
//
// `1.25dec * 2` is the decimal 2.50, which compares equal to the literal
// `2.5dec` and has different bytes under the index format unless the index
// respells it. An index stamped with the normalised-decimal format version
// does, and an older index keeps the spelling it was written with.

/// The index format version before decimals are spelled without trailing zeros.
const SPELLS_TRAILING_ZEROS: u16 = crate::catalog::BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION - 1;

/// Stamps index `ix` of `tb` with `format_version`, as a binary that wrote it
/// at that version would have. Called while the table is empty, so only the
/// definition changes.
async fn stamp_index_format(ds: &Datastore, tb: &str, format_version: u16) {
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let db = tx.expect_db_by_name("test", "test").await.unwrap();
	let table = TableName::from(tb);
	let ix = tx.expect_tb_index(db.namespace_id, db.database_id, &table, "ix").await.unwrap();
	let mut stamped = (*ix).clone();
	stamped.format_version = format_version;
	tx.put_tb_index(db.namespace_id, db.database_id, &table, &stamped).await.unwrap();
	let tb_def = tx.expect_tb(db.namespace_id, db.database_id, &table).await.unwrap();
	tx.put_tb("test", "test", &tb_def).await.unwrap();
	tx.commit().await.unwrap();
}

async fn index_format(ds: &Datastore, tb: &str) -> u16 {
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let db = tx.expect_db_by_name("test", "test").await.unwrap();
	let table = TableName::from(tb);
	let ix = tx.expect_tb_index(db.namespace_id, db.database_id, &table, "ix").await.unwrap();
	tx.cancel().await.unwrap();
	ix.format_version
}

/// The decimal 2.50, computed, and the literal 2.5: equal values, two spellings.
async fn two_spellings(ds: &Datastore, ses: &Session) -> (Value, Value) {
	let untidy = run(ds, ses, "RETURN 1.25dec * 2;", &[]).await.remove(0);
	let tidy = run(ds, ses, "RETURN 2.5dec;", &[]).await.remove(0);
	assert_eq!(untidy, tidy);
	assert_ne!(format!("{untidy:?}"), format!("{tidy:?}"), "the computed decimal keeps its zero");
	(untidy, tidy)
}

/// The ids of the records `tb:key`, in the order a result lists them.
fn ids_of(tb: &str, keys: &[i64]) -> Vec<String> {
	let mut out: Vec<String> = keys
		.iter()
		.map(|k| exact(&RecordId::new(TableName::from(tb), RecordIdKey::Number(*k))))
		.collect();
	out.sort();
	out
}

/// Checks `cond` is answered from the index.
async fn assert_indexed(
	ds: &Datastore,
	ses: &Session,
	tb: &str,
	cond: &str,
	vars: &[(&str, &Value)],
) {
	let plan =
		run(ds, ses, &format!("EXPLAIN SELECT VALUE id FROM {tb} WHERE {cond};"), vars).await;
	assert!(format!("{plan:?}").contains("IndexScan"), "{tb}: WHERE {cond} is not an index scan");
}

/// Writes `values` as records `0..` holding `c`.
async fn fill(ds: &Datastore, ses: &Session, tb: &str, values: Vec<Value>) -> Vec<Row> {
	let rows = rows(values);
	create(ds, ses, tb, &["c"], &rows).await;
	rows
}

async fn equal_decimals_read_the_same_through_a_current_index(unique: bool) {
	let (ds, ses) = datastore().await;
	let (untidy, tidy) = two_spellings(&ds, &ses).await;
	let kind = if unique {
		" UNIQUE"
	} else {
		""
	};
	run(&ds, &ses, &format!("DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS c{kind};"), &[]).await;
	assert_eq!(index_format(&ds, "t").await, crate::catalog::INDEX_FORMAT_VERSION);
	// A `UNIQUE` index holds one of the pair; records 0 and 1 hold the pair
	// otherwise, and record 2 sits below it and record 3 above it.
	let (equal, below, above): (Vec<i64>, i64, i64) = if unique {
		(vec![0], 1, 2)
	} else {
		(vec![0, 1], 2, 3)
	};
	let mut values = vec![untidy.clone()];
	if !unique {
		values.push(tidy.clone());
	}
	values.extend([dec("2.4"), dec("2.6")]);
	let rows = fill(&ds, &ses, "t", values).await;
	assert_kernel_bytes("t", "a statement", &entries(&ds, &ses, "t", &["c"], &rows).await);
	let expect = |ks: &[&[i64]]| ids_of("t", &ks.concat());
	for probe in [&untidy, &tidy] {
		let v = [("x", probe)];
		for cond in ["c = $x", "c >= $x AND c <= $x"] {
			assert_indexed(&ds, &ses, "t", cond, &v).await;
			assert_eq!(read_both_ways(&ds, &ses, "t", cond, &v).await, expect(&[&equal]), "{cond}");
		}
		let cases = [
			("c >= $x", expect(&[&equal, &[above]])),
			("c > $x", expect(&[&[above]])),
			("c <= $x", expect(&[&equal, &[below]])),
			("c < $x", expect(&[&[below]])),
		];
		for (cond, expected) in cases {
			assert_eq!(
				read_both_ways(&ds, &ses, "t", cond, &v).await,
				expected,
				"{cond} on {probe:?}"
			);
		}
	}
	if unique {
		// The other spelling of a value the index holds is refused, by a create
		// as by an update, whichever spelling was written first.
		let res = try_run(&ds, &ses, "CREATE t:10 SET c = $x;", &[("x", &tidy)]).await;
		assert!(matches!(&res[..], [Err(e)] if e.contains("already contains")), "{res:?}");
		let res = try_run(&ds, &ses, "UPDATE t:1 SET c = $x;", &[("x", &tidy)]).await;
		assert!(matches!(&res[..], [Err(e)] if e.contains("already contains")), "{res:?}");
		run(&ds, &ses, "DEFINE TABLE s; DEFINE INDEX ix ON s FIELDS c UNIQUE;", &[]).await;
		fill(&ds, &ses, "s", vec![tidy.clone()]).await;
		let res = try_run(&ds, &ses, "CREATE s:10 SET c = $x;", &[("x", &untidy)]).await;
		assert!(matches!(&res[..], [Err(e)] if e.contains("already contains")), "{res:?}");
		// Building over both spellings is refused too.
		run(&ds, &ses, "DEFINE TABLE b;", &[]).await;
		fill(&ds, &ses, "b", vec![untidy.clone(), tidy.clone()]).await;
		let res = try_run(&ds, &ses, "DEFINE INDEX ix ON b FIELDS c UNIQUE;", &[]).await;
		assert!(matches!(&res[..], [Err(e)] if e.contains("already contains")), "{res:?}");
	}
}

#[tokio::test]
async fn equal_decimals_are_one_key_to_a_current_non_unique_index() {
	equal_decimals_read_the_same_through_a_current_index(false).await;
}

#[tokio::test]
async fn equal_decimals_are_one_key_to_a_current_unique_index() {
	equal_decimals_read_the_same_through_a_current_index(true).await;
}

/// A decimal nested in an object, a record id, or the columns of a compound
/// index is respelled as a bare one is.
#[tokio::test]
async fn decimals_nested_in_a_value_are_one_key_on_a_current_index() {
	let (ds, ses) = datastore().await;
	let cases = [
		("obj", "c", "{ k: 1.25dec * 2 }", "{ k: 2.5dec }", "c = { k: 2.5dec }"),
		("rid", "c", "other:[1.25dec * 2]", "other:[2.5dec]", "c = other:[2.5dec]"),
	];
	for (tb, cols, untidy, tidy, cond) in cases {
		let sql = format!(
			"DEFINE TABLE {tb}; DEFINE INDEX ix ON {tb} FIELDS {cols};
			 CREATE {tb}:1 SET c = {untidy};
			 CREATE {tb}:2 SET c = {tidy};
			 CREATE {tb}:3 SET c = {tidy};
			 CREATE {tb}:4 SET c = 0;"
		);
		run(&ds, &ses, &sql, &[]).await;
		// The planner answers an equality to a record id by a scan, so the
		// kernel's own tests carry that case.
		if tb == "obj" {
			assert_indexed(&ds, &ses, tb, cond, &[]).await;
		}
		let ids = read_both_ways(&ds, &ses, tb, cond, &[]).await;
		assert_eq!(ids, ids_of(tb, &[1, 2, 3]), "{tb}: {cond}");
	}
	// A compound index respells its leading values and its range value.
	run(
		&ds,
		&ses,
		"DEFINE TABLE pair; DEFINE INDEX ix ON pair FIELDS c, d;
		 CREATE pair:1 SET c = 1.25dec * 2, d = 3dec * 2.5;
		 CREATE pair:2 SET c = 2.5dec, d = 7.5dec;
		 CREATE pair:3 SET c = 2.5dec, d = 8dec;
		 CREATE pair:4 SET c = 3dec, d = 7.5dec;",
		&[],
	)
	.await;
	for (cond, expected) in [
		("c = 2.5dec AND d = 7.5dec", vec![1, 2]),
		("c = 2.50dec AND d >= 7.5dec", vec![1, 2, 3]),
		("c = 2.5dec AND d > 7.5dec", vec![3]),
		("c = 2.5dec AND d <= 7.50dec", vec![1, 2]),
		("c = 2.5dec AND d < 7.5dec", vec![]),
	] {
		assert_indexed(&ds, &ses, "pair", cond, &[]).await;
		assert_eq!(
			read_both_ways(&ds, &ses, "pair", cond, &[]).await,
			ids_of("pair", &expected),
			"{cond}"
		);
	}
}

/// An index stamped before decimals were respelled keeps writing them as they
/// are, so what it holds is still found at the keys it was written under, and
/// an update or a delete of an entry leaves nothing behind.
async fn an_older_index_keeps_its_spelling_and_cleans_up_after_itself(unique: bool) {
	let (ds, ses) = datastore().await;
	let (untidy, tidy) = two_spellings(&ds, &ses).await;
	let kind = if unique {
		" UNIQUE"
	} else {
		""
	};
	run(&ds, &ses, &format!("DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS c{kind};"), &[]).await;
	stamp_index_format(&ds, "t", SPELLS_TRAILING_ZEROS).await;
	assert_eq!(index_format(&ds, "t").await, SPELLS_TRAILING_ZEROS);

	// An index of the older format, `UNIQUE` or not, takes both spellings.
	let mut rows = fill(&ds, &ses, "t", vec![untidy.clone(), tidy.clone(), dec("2.6")]).await;
	assert_kernel_bytes("t", "a statement", &entries(&ds, &ses, "t", &["c"], &rows).await);
	let x = [("x", &tidy)];
	let indexed = "SELECT VALUE id FROM t WITH INDEX ix WHERE c = $x;";
	assert_eq!(exact_ids(run(&ds, &ses, indexed, &x).await), ids_of("t", &[1]));
	let scanned = "SELECT VALUE id FROM t WITH NOINDEX WHERE c = $x;";
	assert_eq!(exact_ids(run(&ds, &ses, scanned, &x).await), ids_of("t", &[0, 1]));
	// A range is read from the keys and every record it reaches is judged
	// again, so a bound past one spelling of its value leaves the other out.
	assert_eq!(read_both_ways(&ds, &ses, "t", "c > $x", &x).await, ids_of("t", &[2]));
	assert_eq!(count_above(&ds, &ses, &tidy).await, 1);
	let indexed = "SELECT VALUE id FROM t WITH INDEX ix WHERE c <= $x;";
	assert_eq!(exact_ids(run(&ds, &ses, indexed, &x).await), ids_of("t", &[1]));
	let scanned = "SELECT VALUE id FROM t WITH NOINDEX WHERE c <= $x;";
	assert_eq!(exact_ids(run(&ds, &ses, scanned, &x).await), ids_of("t", &[0, 1]));
	// The computed decimal's entry sits at the key its own spelling gives it.
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let db = tx.expect_db_by_name("test", "test").await.unwrap();
	let table = TableName::from("t");
	let def = tx.expect_tb_index(db.namespace_id, db.database_id, &table, "ix").await.unwrap();
	assert!(!crate::catalog::spells_normalised_decimal_keys(def.format_version));
	let ks = IndexKeyspace::of(db.namespace_id, db.database_id, &def);
	let raw =
		ks.stored_key(unique, std::slice::from_ref(&untidy), &RecordIdKey::Number(0)).unwrap();
	let respelled =
		ks.stored_key(unique, std::slice::from_ref(&tidy), &RecordIdKey::Number(0)).unwrap();
	assert_ne!(raw.key, respelled.key);
	assert!(tx.get_key(&raw, None).await.unwrap().is_some(), "the computed decimal's entry");
	tx.cancel().await.unwrap();

	// Rewriting and deleting entries leaves none of them behind.
	let three = int(3);
	run(&ds, &ses, "UPDATE t:0 SET c = $x;", &[("x", &three)]).await;
	rows[0].values = vec![three];
	assert_kernel_bytes("t", "an update", &entries(&ds, &ses, "t", &["c"], &rows).await);
	run(&ds, &ses, "UPDATE t:2 SET c = 1.25dec * 2;", &[]).await;
	rows[2].values = vec![untidy.clone()];
	assert_kernel_bytes("t", "an update", &entries(&ds, &ses, "t", &["c"], &rows).await);
	run(&ds, &ses, "UPDATE t:2 SET c = 1.7dec * 2;", &[]).await;
	rows[2].values = run(&ds, &ses, "RETURN 1.7dec * 2;", &[]).await;
	assert_kernel_bytes("t", "an update", &entries(&ds, &ses, "t", &["c"], &rows).await);
	run(&ds, &ses, "DELETE t:2;", &[]).await;
	rows.truncate(2);
	assert_kernel_bytes("t", "a delete", &entries(&ds, &ses, "t", &["c"], &rows).await);
	run(&ds, &ses, "DELETE t:1; DELETE t:0;", &[]).await;
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let left = tx.scan(ks.all().unwrap(), 100, 0, None).await.unwrap();
	tx.cancel().await.unwrap();
	assert!(left.is_empty(), "{} entries left behind", left.len());

	// A rebuild respells every entry, and the index then answers as a scan
	// does. A `UNIQUE` index holding both spellings refuses the rebuild.
	let values = if unique {
		vec![untidy, dec("2.6")]
	} else {
		vec![untidy, tidy.clone(), dec("2.6")]
	};
	let last = values.len() as i64 - 1;
	let at_most: Vec<i64> = (0..last).collect();
	fill(&ds, &ses, "t", values).await;
	run(&ds, &ses, "REBUILD INDEX ix ON t;", &[]).await;
	assert!(crate::catalog::spells_normalised_decimal_keys(index_format(&ds, "t").await));
	assert_eq!(read_both_ways(&ds, &ses, "t", "c > $x", &x).await, ids_of("t", &[last]));
	assert_eq!(read_both_ways(&ds, &ses, "t", "c <= $x", &x).await, ids_of("t", &at_most));
	assert_eq!(count_above(&ds, &ses, &tidy).await, 1);
}

/// `count()` of the records of `t` whose `c` lies above `x`.
async fn count_above(ds: &Datastore, ses: &Session, x: &Value) -> i64 {
	let sql = "SELECT count() FROM t WHERE c > $x GROUP ALL;";
	let out = run(ds, ses, sql, &[("x", x)]).await;
	match out.as_slice() {
		[Value::Array(Array(rows))] => match rows.as_slice() {
			[Value::Object(row)] => match row.get("count") {
				Some(Value::Number(Number::Int(n))) => *n,
				v => panic!("count: {v:?}"),
			},
			rows => panic!("rows: {rows:?}"),
		},
		out => panic!("{sql}: {out:?}"),
	}
}

#[tokio::test]
async fn an_older_non_unique_index_keeps_its_spelling_and_cleans_up_after_itself() {
	an_older_index_keeps_its_spelling_and_cleans_up_after_itself(false).await;
}

#[tokio::test]
async fn an_older_unique_index_keeps_its_spelling_and_cleans_up_after_itself() {
	an_older_index_keeps_its_spelling_and_cleans_up_after_itself(true).await;
}

/// `REBUILD INDEX` stamps the current format, the build writes the respelled
/// keys, and every write after it maintains them: no entry in the older
/// spelling survives the rebuild.
async fn a_rebuild_respells_the_entries_of_an_older_index(unique: bool) {
	let (ds, ses) = datastore().await;
	let (untidy, tidy) = two_spellings(&ds, &ses).await;
	let kind = if unique {
		" UNIQUE"
	} else {
		""
	};
	run(&ds, &ses, &format!("DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS c{kind};"), &[]).await;
	stamp_index_format(&ds, "t", SPELLS_TRAILING_ZEROS).await;
	// An older `UNIQUE` index could hold both spellings, but a rebuild over
	// both is refused, so it starts from a table holding one.
	let mut values = vec![untidy.clone()];
	if !unique {
		values.push(tidy.clone());
	}
	values.push(int(7));
	let mut rows = fill(&ds, &ses, "t", values).await;
	assert_kernel_bytes("t", "a statement", &entries(&ds, &ses, "t", &["c"], &rows).await);

	run(&ds, &ses, "REBUILD INDEX ix ON t;", &[]).await;
	assert_eq!(index_format(&ds, "t").await, crate::catalog::INDEX_FORMAT_VERSION);
	assert_kernel_bytes("t", "a rebuild", &entries(&ds, &ses, "t", &["c"], &rows).await);
	let x = [("x", &tidy)];
	let equal: &[i64] = if unique {
		&[0]
	} else {
		&[0, 1]
	};
	assert_eq!(read_both_ways(&ds, &ses, "t", "c = $x", &x).await, ids_of("t", equal));

	// Writes after the rebuild are maintained at the respelled keys.
	run(&ds, &ses, "UPDATE t:0 SET c = 1.7dec * 2;", &[]).await;
	rows[0].values = run(&ds, &ses, "RETURN 1.7dec * 2;", &[]).await;
	assert_kernel_bytes("t", "an update", &entries(&ds, &ses, "t", &["c"], &rows).await);
	run(&ds, &ses, "CREATE t:9 SET c = 1.25dec * 2;", &[]).await;
	rows.push(Row {
		id: RecordIdKey::Number(9),
		values: vec![untidy.clone()],
	});
	assert_kernel_bytes("t", "a create", &entries(&ds, &ses, "t", &["c"], &rows).await);
	let equal: &[i64] = if unique {
		&[9]
	} else {
		&[1, 9]
	};
	assert_eq!(read_both_ways(&ds, &ses, "t", "c = $x", &x).await, ids_of("t", equal));
	if unique {
		let res = try_run(&ds, &ses, "CREATE t:10 SET c = 2.5dec;", &[]).await;
		assert!(matches!(&res[..], [Err(e)] if e.contains("already contains")), "{res:?}");
	}
	run(&ds, &ses, "DELETE t:9;", &[]).await;
	rows.pop();
	assert_kernel_bytes("t", "a delete", &entries(&ds, &ses, "t", &["c"], &rows).await);
}

#[tokio::test]
async fn a_rebuild_respells_the_entries_of_an_older_non_unique_index() {
	a_rebuild_respells_the_entries_of_an_older_index(false).await;
}

#[tokio::test]
async fn a_rebuild_respells_the_entries_of_an_older_unique_index() {
	a_rebuild_respells_the_entries_of_an_older_index(true).await;
}
