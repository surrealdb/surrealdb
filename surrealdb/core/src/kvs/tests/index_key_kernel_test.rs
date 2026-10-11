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
	IndexKeyspace {
		ns: crate::catalog::NamespaceId(1),
		db: crate::catalog::DatabaseId(2),
		tb,
		ix: crate::catalog::IndexId(3),
	}
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
	match v {
		Value::Number(Number::Float(f)) => format!("f64({:#018x})", f.to_bits()),
		v => format!("{v:?}"),
	}
}

/// The ids a query answers, compared with their number kinds.
fn exact_ids(values: Vec<Value>) -> Vec<String> {
	let mut out: Vec<String> = values
		.into_iter()
		.flat_map(|v| match v {
			Value::Array(Array(items)) => items,
			v => vec![v],
		})
		.map(|v| match v {
			Value::RecordId(rid) => exact(&rid),
			v => panic!("not a record id: {v:?}"),
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
	let inf_or_nan = |v: &Value| match v {
		Value::Number(Number::Float(f)) => f.is_nan() || *f == f64::INFINITY,
		_ => false,
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
	let is_zero = |v: &Value| match v {
		Value::Number(Number::Int(i)) => *i == 0,
		Value::Number(Number::Float(f)) => *f == 0.0,
		Value::Number(Number::Decimal(d)) => d.is_zero(),
		_ => false,
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

/// A non-unique entry's key spells the record id under the index format,
/// which erases a nested number's kind: records whose ids differ only there,
/// holding the same indexed value, share one entry. The entry names the
/// record written last, and an indexed read answers that record alone.
#[tokio::test]
async fn records_differing_only_in_id_number_kind_share_a_non_unique_entry() {
	let (ds, ses) = datastore().await;
	run(&ds, &ses, "DEFINE TABLE kd; DEFINE INDEX ix ON kd FIELDS v;", &[]).await;
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
}

/// A float and a decimal that agree to an `f64`'s precision but not exactly
/// are two keys, each spelling its exact value. Value comparison treats such
/// a float and decimal as equal. An unindexed read answers the records whose
/// values the predicate admits; an indexed read answers those of them whose
/// keys the predicate's key range also holds. So an equality, or an inclusive
/// bound, on one of the pair reads both through a scan and one through the
/// index.
///
/// A range is answered by its keys alone only where its bound is a number a
/// float and a decimal both hold exactly; there the key range and the
/// comparison admit the same values. A bound like either of the pair keeps the
/// comparison as a recheck, over a column declared a number kind as over an
/// undeclared one, so an exclusive bound on one of the pair never reads the
/// other through the index.
#[tokio::test]
async fn a_float_and_a_finer_decimal_are_two_keys_read_by_their_exact_values() {
	let (ds, ses) = datastore().await;
	let values = [float(0.1), dec("0.1"), float(1.0), dec("1.0000000000000000000000000001")];
	let tb = TableName::from("x");
	let ks = keyspace(&tb);
	let key = |v: &Value| ks.unique_key(std::slice::from_ref(v)).unwrap().as_slice().to_vec();
	assert!(key(&dec("0.1")) < key(&float(0.1)));
	assert!(key(&float(1.0)) < key(&dec("1.0000000000000000000000000001")));
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
				let sql = format!("SELECT VALUE id FROM {tb} WITH INDEX ix WHERE v {op} $x;");
				let indexed = exact_ids(run(&ds, &ses, &sql, &[("x", x)]).await);
				let expected = ids(&|v| admits(key(v).cmp(&key(x))) && admits(v.cmp(x)));
				assert_eq!(indexed, expected, "{tb}: indexed v {op} {x:?}");
				let sql = format!("SELECT VALUE id FROM {tb} WITH NOINDEX WHERE v {op} $x;");
				let scanned = exact_ids(run(&ds, &ses, &sql, &[("x", x)]).await);
				assert_eq!(scanned, ids(&|v| admits(v.cmp(x))), "{tb}: scanned v {op} {x:?}");
			}
		}
		let sql = |hint: &str| format!("SELECT VALUE id FROM {tb} WITH {hint} WHERE v = $x;");
		let x = float(0.1);
		let indexed = exact_ids(run(&ds, &ses, &sql("INDEX ix"), &[("x", &x)]).await);
		let scanned = exact_ids(run(&ds, &ses, &sql("NOINDEX"), &[("x", &x)]).await);
		assert_eq!((indexed.len(), scanned.len()), (1, 2), "{tb}: v = 0.1f");
	}
}
