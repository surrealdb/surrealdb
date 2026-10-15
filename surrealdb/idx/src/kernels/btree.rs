//! B-tree and `UNIQUE` index key kernels.
//!
//! An index entry is stored under the index's keyspace — namespace, database,
//! table, index — at a key spelled from the entry's indexed values (`fd`):
//!
//! - a non-unique entry at `EntryKey`: the values, then the record's id, so any number of records
//!   share the same values;
//! - a unique entry at `UniqueKey`: the values alone, so a second record with the same values
//!   collides;
//! - a unique entry any of whose values is `NONE` or `NULL` at `EntryKey`, as a non-unique entry
//!   is, so such entries coexist (`NULL` equals no other `NULL` for uniqueness).
//!
//! The values are encoded under the index format, which orders numbers of
//! every kind as one: `0`, `0f` and `0dec` share key bytes, and a range over
//! numbers reads them in numeric order. Both entry shapes store an
//! [`IndexEntryValue`].
//!
//! Each index holds one spelling of a `Datetime`, chosen by its format-version
//! stamp (see [`DatetimeSpelling`]): an index stamped at
//! [`DATETIME_ORDER_FORMAT_VERSION`] or later spells it so its keys sort
//! chronologically, and one stamped earlier spells it as RFC 3339 text. Every
//! key below is encoded for the stamp of the index it belongs to, so a write, a
//! delete, a unique-slot check and a read of one index all agree.
//!
//! [`IndexKeyspace`] is the only builder of these keys: nothing else names
//! `EntryKey`, `UniqueKey` or the bounds cut from `EntryKey` with indexed
//! values in them, which a test in this module checks over the workspace's
//! sources. [`IndexKeyspace::stored_key`] is the one place a write decides
//! where an entry goes, and [`IndexKeyspace::equality`],
//! [`IndexKeyspace::range`] and [`IndexKeyspace::compound`] are the ranges a
//! read scans for a predicate. The ranges are half-open, `[start, end)`, and
//! contain exactly the keys [`IndexKeyspace::stored_key`] writes for values
//! the predicate admits.

use std::borrow::Cow;
use std::ops::Bound;
use std::slice;

use anyhow::Result;

use crate::catalog::{
	DATETIME_ORDER_FORMAT_VERSION, DatabaseId, IndexDefinition, IndexId, NamespaceId,
};
use crate::entry::IndexEntryValue;
use crate::expr::BinaryOperator;
use crate::key::schema::{EntryFdOpenPrefix, EntryFdPrefix, EntryKey, EntryPrefix, UniqueKey};
use crate::key::{KVKey, KVRange, KVValue, Key, KeyRange, TypedRange};
use crate::val::{IndexFormat, IndexIdFormat, RecordId, RecordIdKey, TableName, Value};

/// Where an entry for one tuple of indexed values is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredKey {
	/// The entry's key.
	pub key: Key<'static>,
	/// Whether the key is a unique slot: one a second record with the same
	/// values collides on, rather than one naming the record.
	pub unique: bool,
}

/// A stored key holds an index entry, so the transaction's typed reads and
/// writes take it as they take a generated entry key.
impl KVKey for StoredKey {
	type Value = IndexEntryValue;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		buffer.extend_from_slice(self.key.as_slice());
		Ok(())
	}

	fn encode_key(&self) -> Result<Key<'static>> {
		Ok(self.key.clone())
	}

	fn value_context(&self) {}
}

/// How the keys of one index spell a `Datetime` among its indexed values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatetimeSpelling {
	/// RFC 3339 text, under `IndexIdFormat`: the spelling of an index stamped
	/// below [`DATETIME_ORDER_FORMAT_VERSION`]. Its bytes do not sort
	/// chronologically.
	Text,
	/// Seconds then nanoseconds since the Unix epoch, under `IndexFormat`: the
	/// spelling of an index stamped at [`DATETIME_ORDER_FORMAT_VERSION`] or
	/// later. Its bytes sort chronologically.
	Chronological,
}

impl DatetimeSpelling {
	/// The spelling an index stamped with `format_version` holds.
	pub fn of(format_version: u16) -> Self {
		if format_version >= DATETIME_ORDER_FORMAT_VERSION {
			Self::Chronological
		} else {
			Self::Text
		}
	}

	/// `key`'s bytes, with its indexed values in this spelling. Every key and
	/// bound in this module is encoded here. A value with no key encoding
	/// fails as [`surrealdb_kvs::key::Error::Unencodable`], the error every
	/// other key encoder raises for it.
	fn encode<K>(self, key: &K) -> Result<Key<'static>>
	where
		K: storekey::Encode<IndexFormat> + storekey::Encode<IndexIdFormat>,
	{
		let bytes = match self {
			Self::Text => storekey::encode_vec_format::<IndexIdFormat, _>(key),
			Self::Chronological => storekey::encode_vec_format::<IndexFormat, _>(key),
		};
		Ok(Key::from(bytes.map_err(|_| surrealdb_kvs::key::Error::Unencodable)?))
	}
}

/// One index's keyspace: every key of its entries starts with these fields,
/// and spells its indexed values for the index's format-version stamp.
#[derive(Debug, Clone, Copy)]
pub struct IndexKeyspace<'a> {
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	ix: IndexId,
	spelling: DatetimeSpelling,
}

impl<'a> IndexKeyspace<'a> {
	/// The keyspace of index `ix`, defined in database `db` of namespace `ns`.
	pub fn of(ns: NamespaceId, db: DatabaseId, ix: &'a IndexDefinition) -> Self {
		Self::new(ns, db, &ix.table_name, ix.index_id, ix.format_version)
	}

	/// The keyspace of index `ix` on table `tb` in database `db` of namespace
	/// `ns`, stamped with `format_version`. [`Self::of`] takes these from the
	/// index's definition, which is where a caller holding one takes them.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		format_version: u16,
	) -> Self {
		Self {
			ns,
			db,
			tb,
			ix,
			spelling: DatetimeSpelling::of(format_version),
		}
	}

	/// How this index's keys spell a `Datetime`.
	pub fn spelling(&self) -> DatetimeSpelling {
		self.spelling
	}

	/// The key of a non-unique entry for values `fd` of the record keyed `id`.
	pub fn entry_key(&self, fd: &[Value], id: &RecordIdKey) -> Result<Key<'static>> {
		self.spelling.encode(&EntryKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: Cow::Borrowed(fd),
			id: Cow::Borrowed(id),
		})
	}

	/// The unique slot for values `fd`.
	pub fn unique_key(&self, fd: &[Value]) -> Result<Key<'static>> {
		self.spelling.encode(&UniqueKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: Cow::Borrowed(fd),
		})
	}

	/// Where the entry for values `fd` of the record keyed `id` is stored, on
	/// an index that is `unique` or not.
	pub fn stored_key(&self, unique: bool, fd: &[Value], id: &RecordIdKey) -> Result<StoredKey> {
		if unique && !fd.iter().any(Value::is_nullish) {
			Ok(StoredKey {
				key: self.unique_key(fd)?,
				unique: true,
			})
		} else {
			Ok(StoredKey {
				key: self.entry_key(fd, id)?,
				unique: false,
			})
		}
	}

	/// Every entry of the index.
	pub fn all(&self) -> Result<TypedRange<IndexEntryValue>> {
		let prefix = self.spelling.encode(&self.prefix())?;
		let end = prefix.clone().next_neighbour_expect();
		Ok(self.typed((prefix..end).into()))
	}

	/// The entries whose values are exactly `[value]`.
	pub fn equality(&self, unique: bool, value: &Value) -> Result<TypedRange<IndexEntryValue>> {
		let fd = slice::from_ref(value);
		if unique && !value.is_nullish() {
			// A unique slot is a complete key, so the range holding it alone
			// ends at its immediate successor.
			let key = self.unique_key(fd)?;
			let end = key.as_borrowed().next();
			Ok(self.typed((key..end).into()))
		} else {
			// Every entry for exactly these values, whatever record it names.
			let bound = self.spelling.encode(&EntryFdPrefix {
				ns: self.ns,
				db: self.db,
				tb: Cow::Borrowed(self.tb),
				ix: self.ix,
				fd: Cow::Borrowed(fd),
			})?;
			Ok(self.typed(bound.prefix_expect()))
		}
	}

	/// The entries whose first value lies between `from` and `to`, whatever
	/// values follow it on a compound index.
	///
	/// A bound on a value admits or excludes every key that begins with that
	/// value: a single-column unique slot, the unique slots of a compound
	/// index, which go on to the further values, and plain entries, which go
	/// on to the record id. The range is therefore the same for a unique index
	/// as for a non-unique one.
	pub fn range(
		&self,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<TypedRange<IndexEntryValue>> {
		let start = match from {
			Bound::Included(x) => self.first_under(x)?,
			Bound::Excluded(x) => self.past(x)?,
			Bound::Unbounded => self.spelling.encode(&self.prefix())?,
		};
		let end = match to {
			Bound::Included(x) => self.past(x)?,
			Bound::Excluded(x) => self.first_under(x)?,
			Bound::Unbounded => self.spelling.encode(&self.prefix())?.next_neighbour_expect(),
		};
		Ok(self.typed((start..end).into()))
	}

	/// The entries of a compound index whose leading values equal `prefix`,
	/// and, with `range`, whose next value compares to the range's value as
	/// its operator states. The operator is a comparison (`=`, `==`, `<`,
	/// `<=`, `>`, `>=`); any other is refused, since no key range answers it.
	pub fn compound(
		&self,
		prefix: &[Value],
		range: Option<(&BinaryOperator, &Value)>,
	) -> Result<TypedRange<IndexEntryValue>> {
		let without = || self.open(prefix);
		let Some((op, value)) = range else {
			return Ok(self.typed(without()?.prefix_expect()));
		};
		let with = || {
			let mut fd = Vec::with_capacity(prefix.len() + 1);
			fd.extend_from_slice(prefix);
			fd.push(value.clone());
			self.open(&fd)
		};
		let range: KeyRange<'static> = match op {
			BinaryOperator::Equal | BinaryOperator::ExactEqual => with()?.prefix_expect(),
			BinaryOperator::MoreThan => {
				(with()?.next_neighbour_expect()..without()?.next_neighbour_expect()).into()
			}
			BinaryOperator::MoreThanEqual => (with()?..without()?.next_neighbour_expect()).into(),
			BinaryOperator::LessThan => (without()?..with()?).into(),
			BinaryOperator::LessThanEqual => (without()?..with()?.next_neighbour_expect()).into(),
			op => anyhow::bail!("`{op:?}` is no comparison a compound index range answers"),
		};
		Ok(self.typed(range))
	}

	/// The first key an entry whose first value is `x` may sit at.
	fn first_under(&self, x: &Value) -> Result<Key<'static>> {
		self.open(slice::from_ref(x))
	}

	/// The first key past every entry whose first value is `x`.
	fn past(&self, x: &Value) -> Result<Key<'static>> {
		Ok(self.open(slice::from_ref(x))?.next_neighbour_expect())
	}

	fn prefix(&self) -> EntryPrefix<'a> {
		EntryPrefix {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
		}
	}

	/// The bound over every entry whose values start with `fd`: the first
	/// key such an entry may sit at, and, past its last byte that is not
	/// `0xFF`, the first key after all of them.
	fn open(&self, fd: &[Value]) -> Result<Key<'static>> {
		self.spelling.encode(&EntryFdOpenPrefix {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: Cow::Borrowed(fd),
		})
	}

	/// A range within the index's entries, which all store an
	/// [`IndexEntryValue`].
	fn typed(&self, range: KeyRange<'static>) -> TypedRange<IndexEntryValue> {
		self.prefix().typed(range)
	}
}

/// The entry an index entry's stored bytes hold: the record id, and the
/// record's doc-id for an index whose format carries it.
pub fn decode_entry(bytes: &[u8]) -> Result<IndexEntryValue> {
	IndexEntryValue::kv_decode_value(bytes, ())
}

/// The record id an index entry's stored bytes name, whichever format the
/// entry was written in.
pub fn decode_record_id(bytes: &[u8]) -> Result<RecordId> {
	IndexEntryValue::decode_record_id(bytes)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, INDEX_FORMAT_VERSION, IndexId, NamespaceId};
	use crate::val::{Array, Number, Object};

	fn space(tb: &TableName) -> IndexKeyspace<'_> {
		IndexKeyspace::new(NamespaceId(1), DatabaseId(2), tb, IndexId(3), INDEX_FORMAT_VERSION)
	}

	fn contains(range: &TypedRange<IndexEntryValue>, key: &Key<'_>) -> bool {
		range.start().as_slice() <= key.as_slice() && key.as_slice() < range.end().as_slice()
	}

	fn kinds() -> Vec<Value> {
		vec![
			Value::None,
			Value::Null,
			Value::Bool(false),
			Value::Bool(true),
			Value::Number(Number::Int(-7)),
			Value::Number(Number::Int(0)),
			Value::Number(Number::Float(0.5)),
			Value::Number(Number::Int(42)),
			Value::from("a"),
			Value::from("a\0b"),
			Value::from("zz"),
			Value::Array(Array::from(vec![Value::from(1), Value::from("x")])),
			Value::Object(Object::default()),
		]
	}

	#[test]
	fn an_entry_lies_in_the_equality_range_of_its_value_and_no_other() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		let kinds = kinds();
		for unique in [false, true] {
			for v in &kinds {
				let stored = ks.stored_key(unique, slice::from_ref(v), &id).unwrap();
				for w in &kinds {
					let range = ks.equality(unique, w).unwrap();
					assert_eq!(
						contains(&range, &stored.key),
						v == w,
						"unique={unique}: entry for {v:?} vs equality on {w:?}"
					);
				}
			}
		}
	}

	#[test]
	fn a_nullish_tuple_on_a_unique_index_is_stored_as_a_non_unique_entry() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		for v in [Value::None, Value::Null] {
			let stored = ks.stored_key(true, &[Value::from(1), v.clone()], &id).unwrap();
			assert!(!stored.unique);
			assert_eq!(stored.key, ks.entry_key(&[Value::from(1), v], &id).unwrap());
		}
		let stored = ks.stored_key(true, &[Value::from(1)], &id).unwrap();
		assert!(stored.unique);
	}

	#[test]
	fn numbers_of_every_kind_share_key_bytes() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let int = ks.unique_key(&[Value::from(1)]).unwrap();
		let float = ks.unique_key(&[Value::from(1.0f64)]).unwrap();
		let dec = ks.unique_key(&[Value::Number(Number::Decimal(1.into()))]).unwrap();
		assert_eq!(int, float);
		assert_eq!(int, dec);
	}

	#[test]
	fn a_range_admits_the_entries_between_its_bounds() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		let values: Vec<Value> = (0..6).map(Value::from).collect();
		for unique in [false, true] {
			let key = |v: &Value| ks.stored_key(unique, slice::from_ref(v), &id).unwrap().key;
			let cases = [
				(Bound::Included(&values[1]), Bound::Included(&values[3]), 1..=3),
				(Bound::Excluded(&values[1]), Bound::Excluded(&values[3]), 2..=2),
				(Bound::Unbounded, Bound::Excluded(&values[2]), 0..=1),
				(Bound::Excluded(&values[3]), Bound::Unbounded, 4..=5),
			];
			for (from, to, admitted) in cases {
				let range = ks.range(from, to).unwrap();
				for (i, v) in values.iter().enumerate() {
					assert_eq!(
						contains(&range, &key(v)),
						admitted.contains(&i),
						"unique={unique} {from:?}..{to:?} on {v:?}"
					);
				}
			}
		}
	}

	#[test]
	fn a_range_on_the_leading_value_admits_every_tuple_under_its_bounds() {
		// The entries of a compound index for one leading value all begin with
		// it, whatever follows: the further values, then the end of a unique
		// slot or, for a plain entry (a unique tuple holding NONE or NULL is
		// one), the record id. A bound on the leading value takes all of them
		// or none.
		let tb = TableName::from("t");
		let ks = space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		let one = Value::from(1);
		let rest = [Value::None, Value::Null, Value::from(0), Value::from(5), Value::from("z")];
		type Case<'a> = (Bound<&'a Value>, Bound<&'a Value>, fn(i64) -> bool);
		let cases: [Case<'_>; 5] = [
			(Bound::Excluded(&one), Bound::Unbounded, |a| a > 1),
			(Bound::Included(&one), Bound::Unbounded, |a| a >= 1),
			(Bound::Unbounded, Bound::Excluded(&one), |a| a < 1),
			(Bound::Unbounded, Bound::Included(&one), |a| a <= 1),
			(Bound::Included(&one), Bound::Included(&one), |a| a == 1),
		];
		for unique in [false, true] {
			for (from, to, admits) in cases {
				let range = ks.range(from, to).unwrap();
				for a in 0..3 {
					for b in &rest {
						let fd = [Value::from(a), b.clone()];
						let key = ks.stored_key(unique, &fd, &id).unwrap().key;
						assert_eq!(
							contains(&range, &key),
							admits(a),
							"unique={unique} {from:?}..{to:?} on ({a}, {b:?})"
						);
					}
				}
			}
		}
	}

	#[test]
	fn a_compound_range_admits_the_entries_under_its_prefix() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		let three = Value::from(3);
		type Case<'a> = (Option<(&'a BinaryOperator, &'a Value)>, fn(i64) -> bool);
		let cases: [Case<'_>; 6] = [
			(None, |_| true),
			(Some((&BinaryOperator::Equal, &three)), |b| b == 3),
			(Some((&BinaryOperator::MoreThan, &three)), |b| b > 3),
			(Some((&BinaryOperator::MoreThanEqual, &three)), |b| b >= 3),
			(Some((&BinaryOperator::LessThan, &three)), |b| b < 3),
			(Some((&BinaryOperator::LessThanEqual, &three)), |b| b <= 3),
		];
		// A unique index stores a tuple at its unique slot, which ends after
		// the values where a non-unique entry goes on to the record id.
		for unique in [false, true] {
			let key = |a: i64, b: i64| {
				ks.stored_key(unique, &[Value::from(a), Value::from(b)], &id).unwrap().key
			};
			for (range, admits) in cases {
				let r = ks.compound(&[Value::from(1)], range).unwrap();
				for a in 0..3 {
					for b in 0..6 {
						assert_eq!(
							contains(&r, &key(a, b)),
							a == 1 && admits(b),
							"unique={unique} {range:?} on ({a}, {b})"
						);
					}
				}
			}
		}
	}

	#[test]
	fn an_operator_that_is_no_comparison_is_refused() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let three = Value::from(3);
		for op in [BinaryOperator::NotEqual, BinaryOperator::Contain, BinaryOperator::Inside] {
			assert!(
				ks.compound(&[Value::from(1)], Some((&op, &three))).is_err(),
				"{op:?} must not answer the whole prefix"
			);
		}
	}

	#[test]
	fn a_prefix_scan_admits_a_unique_tuple_stored_as_an_entry() {
		// A unique tuple holding NONE or NULL is stored at the non-unique entry
		// key, so such tuples coexist; a scan of its prefix still reaches it,
		// and a scan of another prefix does not.
		let tb = TableName::from("t");
		let ks = space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		for nullish in [Value::None, Value::Null] {
			let stored = ks.stored_key(true, &[Value::from(1), nullish.clone()], &id).unwrap();
			assert!(!stored.unique, "{nullish:?} is stored as an entry");
			let under = ks.compound(&[Value::from(1)], None).unwrap();
			assert!(contains(&under, &stored.key), "the prefix (1) misses (1, {nullish:?})");
			let other = ks.compound(&[Value::from(2)], None).unwrap();
			assert!(!contains(&other, &stored.key), "the prefix (2) admits (1, {nullish:?})");
			let three = Value::from(3);
			let equal = ks.compound(&[Value::from(1)], Some((&BinaryOperator::Equal, &three)));
			assert!(!contains(&equal.unwrap(), &stored.key), "(1, 3) admits (1, {nullish:?})");
		}
	}

	#[test]
	fn every_range_lies_within_the_index() {
		let tb = TableName::from("t");
		let ks = space(&tb);
		let all = ks.all().unwrap();
		let one = Value::from(1);
		for unique in [false, true] {
			let r = ks.range(Bound::Unbounded, Bound::Unbounded).unwrap();
			assert_eq!((r.start(), r.end()), (all.start(), all.end()));
			let r = ks.equality(unique, &one).unwrap();
			assert!(all.start() <= r.start() && r.end() <= all.end());
		}
	}

	fn at_version(tb: &TableName, format_version: u16) -> IndexKeyspace<'_> {
		IndexKeyspace::new(NamespaceId(1), DatabaseId(2), tb, IndexId(3), format_version)
	}

	fn datetime(text: &str) -> Value {
		Value::Datetime(text.parse().unwrap())
	}

	/// Whether `key` holds `text` as a storekey string.
	fn holds_text(key: &Key<'_>, text: &str) -> bool {
		let mut needle = text.as_bytes().to_vec();
		needle.push(0);
		key.as_slice().windows(needle.len()).any(|w| w == needle)
	}

	#[test]
	fn the_stamp_selects_the_datetime_spelling() {
		let tb = TableName::from("t");
		let text = at_version(&tb, DATETIME_ORDER_FORMAT_VERSION - 1);
		let chrono = at_version(&tb, DATETIME_ORDER_FORMAT_VERSION);
		assert_eq!(text.spelling(), DatetimeSpelling::Text);
		assert_eq!(chrono.spelling(), DatetimeSpelling::Chronological);
		assert_eq!(at_version(&tb, 0).spelling(), DatetimeSpelling::Text);
		assert_eq!(
			at_version(&tb, INDEX_FORMAT_VERSION).spelling(),
			DatetimeSpelling::Chronological
		);

		let id = RecordIdKey::from("r".to_owned());
		// As the text spelling writes them: UTC, with the fraction in groups
		// of three digits.
		for spelled in ["2026-08-28T09:00:00Z", "2026-08-28T09:00:00.500Z"] {
			let v = datetime(spelled);
			let fd = slice::from_ref(&v);
			let nested = [Value::Array(Array::from(vec![Value::from(1), v.clone()]))];
			for unique in [false, true] {
				let old = text.stored_key(unique, fd, &id).unwrap().key;
				let new = chrono.stored_key(unique, fd, &id).unwrap().key;
				assert!(holds_text(&old, spelled), "{spelled}: {old:?}");
				assert!(!holds_text(&new, spelled), "{spelled}: {new:?}");
				// Each spelling's entry lies in its own index's equality range
				// and in no range of the other, so an index read, written,
				// deleted and checked for uniqueness in one spelling agrees with
				// itself.
				assert!(contains(&text.equality(unique, &v).unwrap(), &old));
				assert!(contains(&chrono.equality(unique, &v).unwrap(), &new));
				assert!(!contains(&text.equality(unique, &v).unwrap(), &new));
				assert!(!contains(&chrono.equality(unique, &v).unwrap(), &old));
				let old = text.stored_key(unique, &nested, &id).unwrap().key;
				let new = chrono.stored_key(unique, &nested, &id).unwrap().key;
				assert!(holds_text(&old, spelled) && !holds_text(&new, spelled), "{spelled}");
			}
		}
		// Every other value, and a record id holding a Datetime, is spelled
		// alike under both stamps.
		let dated = RecordIdKey::Array(Array::from(vec![datetime("2026-08-28T09:00:00Z")]));
		for v in kinds() {
			for unique in [false, true] {
				let fd = slice::from_ref(&v);
				assert_eq!(
					text.stored_key(unique, fd, &dated).unwrap(),
					chrono.stored_key(unique, fd, &dated).unwrap(),
					"{v:?}"
				);
			}
			let (a, b) = (
				text.compound(slice::from_ref(&v), None).unwrap(),
				chrono.compound(slice::from_ref(&v), None).unwrap(),
			);
			assert_eq!((a.start(), a.end()), (b.start(), b.end()), "{v:?}");
		}
		assert_eq!(text.all().unwrap().start(), chrono.all().unwrap().start());
		assert_eq!(text.all().unwrap().end(), chrono.all().unwrap().end());
	}

	/// The stamp chooses the order a range over Datetimes reads: chronological
	/// from the Datetime-order format, and the text's order below it.
	#[test]
	fn a_datetime_range_reads_in_the_order_of_the_stamp() {
		let tb = TableName::from("t");
		let id = RecordIdKey::from("r".to_owned());
		let whole = datetime("2026-08-28T09:00:00Z");
		let fraction = datetime("2026-08-28T09:00:00.5Z");
		let later = datetime("2026-08-28T09:00:01Z");
		for unique in [false, true] {
			let chrono = at_version(&tb, DATETIME_ORDER_FORMAT_VERSION);
			let key = |v: &Value| chrono.stored_key(unique, slice::from_ref(v), &id).unwrap().key;
			let r = chrono.range(Bound::Included(&whole), Bound::Excluded(&later)).unwrap();
			assert!(contains(&r, &key(&whole)) && contains(&r, &key(&fraction)));
			assert!(!contains(&r, &key(&later)));

			// `...:00Z` sorts after `...:00.5Z` as text, so a range from the
			// whole second misses the fraction within it.
			let text = at_version(&tb, DATETIME_ORDER_FORMAT_VERSION - 1);
			let key = |v: &Value| text.stored_key(unique, slice::from_ref(v), &id).unwrap().key;
			let r = text.range(Bound::Included(&whole), Bound::Excluded(&later)).unwrap();
			assert!(contains(&r, &key(&whole)) && !contains(&r, &key(&fraction)));
		}
	}

	/// The keys of index entries and unique slots, and the bounds over their
	/// values, are built in this module alone, so every one of them is spelled
	/// for its index's stamp. Each generated type, and each builder that makes
	/// one from a level root, encodes under one format of its own accord, so a
	/// key built anywhere else would be spelled for the current stamp whatever
	/// the index's is.
	#[test]
	fn entry_and_unique_keys_are_built_only_here() {
		let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
		// The schema declares the types and checks their layout, and the macro
		// crate names them in its own tests of the schema language. The planner's
		// `PhysFactReq` has a variant of the same name as a unique key type, a
		// requirement on the physical plan that builds no key: its declaration
		// is allowed here and its uses are skipped below.
		let allowed = [
			std::path::Path::new("idx/src/kernels/btree.rs"),
			std::path::Path::new("datastore/src/key/schema.rs"),
			std::path::Path::new("core/src/exec/ir/iface/egraph.rs"),
		];
		let skipped = std::path::Path::new("keyspace-macro");
		const NAMES: [&str; 4] = ["EntryKey", "UniqueKey", "EntryFdPrefix", "EntryFdOpenPrefix"];
		const BUILDERS: [&str; 2] = [".entry_fd_prefix(", ".entry_fd_open_prefix("];
		let word = |line: &str, name: &str| {
			line.match_indices(name).any(|(at, _)| {
				let ident = |c: char| c.is_alphanumeric() || c == '_';
				!line[..at].ends_with(ident) && !line[at + name.len()..].starts_with(ident)
			})
		};
		let mut stack = vec![workspace.clone()];
		let mut scanned = 0usize;
		let mut found = Vec::new();
		while let Some(dir) = stack.pop() {
			for entry in std::fs::read_dir(&dir).unwrap() {
				let path = entry.unwrap().path();
				let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
				if path.is_dir() {
					let macro_crate = path.strip_prefix(&workspace).is_ok_and(|r| r == skipped);
					if name != "target" && !name.starts_with('.') && !macro_crate {
						stack.push(path);
					}
					continue;
				}
				if path.extension().is_none_or(|e| e != "rs") {
					continue;
				}
				let relative = path.strip_prefix(&workspace).unwrap();
				if allowed.contains(&relative) {
					continue;
				}
				scanned += 1;
				let source = std::fs::read_to_string(&path).unwrap();
				for (n, line) in source.lines().enumerate() {
					if line.trim_start().starts_with("//") || line.contains("PhysFactReq::") {
						continue;
					}
					let named = NAMES.iter().any(|name| word(line, name));
					let built = BUILDERS.iter().any(|b| line.contains(b));
					if named || built {
						found.push(format!("{}:{}: {}", relative.display(), n + 1, line.trim()));
					}
				}
				// A level root's `entry` and `unique` builders.
				for (at, _) in source.match_indices("IdxRoot") {
					let statement = source[at..].split(';').next().unwrap_or_default();
					let statement: String =
						statement.chars().filter(|c| !c.is_whitespace()).collect();
					if statement.contains(".entry(") || statement.contains(".unique(") {
						found.push(format!("{}: an `IdxRoot` builds an entry", relative.display()));
					}
				}
			}
		}
		assert!(scanned > 100, "scanned only {scanned} files under {}", workspace.display());
		assert!(
			found.is_empty(),
			"index entry keys built outside `IndexKeyspace`:\n{}",
			found.join("\n")
		);
	}

	#[test]
	fn an_entry_decodes_to_the_record_it_names() {
		let rid = RecordId::new(TableName::from("t"), RecordIdKey::from(7));
		for doc_id in [None, Some(9)] {
			let entry = IndexEntryValue {
				rid: rid.clone(),
				doc_id,
			};
			let bytes = entry.kv_encode_value().unwrap();
			assert_eq!(decode_entry(&bytes).unwrap(), entry);
			assert_eq!(decode_record_id(&bytes).unwrap(), rid);
		}
	}
}
