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
//! The stamp also decides how a decimal is spelled. An index stamped at
//! [`crate::catalog::BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION`] or later respells every
//! decimal in its values without trailing zeros before encoding them, in the
//! entries it writes and in the probe values and bounds it reads with, so `2.5dec`
//! and a computed `2.50dec` share one key as they compare equal. One stamped
//! earlier spells them as written: its entries are found, replaced and deleted at
//! the keys they were written at, until the index is rebuilt. Only the indexed
//! values are respelled; the record's own id in an entry key never is.
//!
//! The stamp also decides how a non-unique entry spells the record id that
//! ends its key. An index stamped at
//! [`crate::catalog::BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION`] or later spells it as a
//! `RecordIdentity`, which keeps a number's kind, so `c:[1]` and `c:[1dec]` have an
//! entry each. One stamped earlier spells it as the indexed values are spelled,
//! dropping the kind, so such a pair shares one entry until the index is rebuilt.
//! The two spellings differ only for an id holding a number, where the identity
//! adds the number's kind after it: entries of one value still list their records
//! in record id order.
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
	spells_identity_entry_ids, spells_normalised_decimal_keys,
};
use crate::entry::IndexEntryValue;
use crate::expr::BinaryOperator;
use crate::key::schema::{EntryFdOpenPrefix, EntryFdPrefix, EntryPrefix, UniqueKey};
use crate::key::{KVKey, KVRange, KVValue, Key, KeyRange, TypedRange};
use crate::val::{
	Array, IndexFormat, IndexIdFormat, Number, Object, Range, RecordId, RecordIdKey,
	RecordIdKeyRange, TableName, Value,
};

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

/// The byte that follows a non-unique entry's values and precedes its record
/// id (`raw 0x03` in the `entry` route of the key schema).
const ENTRY_RECORD_ID: u8 = 0x03;

/// One index's keyspace: every key of its entries starts with these fields,
/// and spells its indexed values for the index's format-version stamp.
#[derive(Debug, Clone, Copy)]
pub struct IndexKeyspace<'a> {
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	ix: IndexId,
	spelling: DatetimeSpelling,
	/// Whether the stamp spells decimals without trailing zeros.
	normalised_decimals: bool,
	/// Whether the stamp spells a non-unique entry's record id as a
	/// `RecordIdentity`.
	identity_ids: bool,
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
			normalised_decimals: spells_normalised_decimal_keys(format_version),
			identity_ids: spells_identity_entry_ids(format_version),
		}
	}

	/// `fd` as this index spells it in a key.
	fn spelled_all<'v>(&self, fd: &'v [Value]) -> Cow<'v, [Value]> {
		if self.normalised_decimals && fd.iter().any(holds_untidy_decimal) {
			let mut owned = fd.to_vec();
			owned.iter_mut().for_each(normalise_decimals);
			Cow::Owned(owned)
		} else {
			Cow::Borrowed(fd)
		}
	}

	/// How this index's keys spell a `Datetime`.
	pub fn spelling(&self) -> DatetimeSpelling {
		self.spelling
	}

	/// The key of a non-unique entry for values `fd` of the record keyed `id`:
	/// the closed prefix of the values, the `0x03` that marks a record id, then
	/// the id, spelled for the stamp. An `EntryKey` is that layout with the id
	/// as a `RecordIdentity`; the key is assembled here so the id is encoded
	/// where it is borrowed, in either spelling.
	pub fn entry_key(&self, fd: &[Value], id: &RecordIdKey) -> Result<Key<'static>> {
		let mut key = self
			.spelling
			.encode(&EntryFdPrefix {
				ns: self.ns,
				db: self.db,
				tb: Cow::Borrowed(self.tb),
				ix: self.ix,
				fd: self.spelled_all(fd),
			})?
			.into_vec();
		key.push(ENTRY_RECORD_ID);
		let id = if self.identity_ids {
			storekey::encode_vec(id)
		} else {
			storekey::encode_vec_format::<IndexIdFormat, _>(id)
		};
		key.extend(id.map_err(|_| surrealdb_kvs::key::Error::Unencodable)?);
		Ok(Key::from(key))
	}

	/// The unique slot for values `fd`.
	pub fn unique_key(&self, fd: &[Value]) -> Result<Key<'static>> {
		self.spelling.encode(&UniqueKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: self.spelled_all(fd),
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
				fd: self.spelled_all(fd),
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
			fd: self.spelled_all(fd),
		})
	}

	/// A range within the index's entries, which all store an
	/// [`IndexEntryValue`].
	fn typed(&self, range: KeyRange<'static>) -> TypedRange<IndexEntryValue> {
		self.prefix().typed(range)
	}
}

/// Whether `value` holds a decimal that [`normalise_decimals`] respells: one
/// with trailing zeros after its point, anywhere in the value, including inside
/// the id of a record id and the bounds of a range.
fn holds_untidy_decimal(value: &Value) -> bool {
	fn bound(bound: &Bound<Value>) -> bool {
		matches!(bound, Bound::Included(v) | Bound::Excluded(v) if holds_untidy_decimal(v))
	}
	fn key_bound(bound: &Bound<RecordIdKey>) -> bool {
		matches!(bound, Bound::Included(k) | Bound::Excluded(k) if key_holds_untidy_decimal(k))
	}
	fn key_holds_untidy_decimal(key: &RecordIdKey) -> bool {
		match key {
			RecordIdKey::Array(a) => a.iter().any(holds_untidy_decimal),
			RecordIdKey::Object(o) => o.values().any(holds_untidy_decimal),
			RecordIdKey::Range(r) => key_bound(&r.start) || key_bound(&r.end),
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => false,
		}
	}
	match value {
		Value::Number(Number::Decimal(d)) => d.scale() > 0 && d.mantissa() % 10 == 0,
		Value::Array(a) => a.iter().any(holds_untidy_decimal),
		Value::Set(s) => s.iter().any(holds_untidy_decimal),
		Value::Object(o) => o.values().any(holds_untidy_decimal),
		Value::RecordId(r) => key_holds_untidy_decimal(&r.key),
		Value::Range(r) => bound(&r.start) || bound(&r.end),
		_ => false,
	}
}

/// Respell every decimal in `value` without trailing zeros, through arrays,
/// sets, objects, the ids of record ids and the bounds of ranges. Values that
/// compare equal before compare equal after, and the order of a set or object
/// is unchanged.
fn normalise_decimals(value: &mut Value) {
	fn bound(bound: &mut Bound<Value>) {
		if let Bound::Included(v) | Bound::Excluded(v) = bound {
			normalise_decimals(v);
		}
	}
	fn key_bound(bound: &mut Bound<RecordIdKey>) {
		if let Bound::Included(k) | Bound::Excluded(k) = bound {
			normalise_key(k);
		}
	}
	fn normalise_key(key: &mut RecordIdKey) {
		match key {
			RecordIdKey::Array(Array(a)) => a.iter_mut().for_each(normalise_decimals),
			RecordIdKey::Object(Object(o)) => o.values_mut().for_each(normalise_decimals),
			RecordIdKey::Range(r) => {
				let RecordIdKeyRange {
					start,
					end,
				} = &mut **r;
				key_bound(start);
				key_bound(end);
			}
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => {}
		}
	}
	match value {
		Value::Number(Number::Decimal(d)) => *d = d.normalize(),
		Value::Array(Array(a)) => a.iter_mut().for_each(normalise_decimals),
		Value::Set(set) if set.iter().any(holds_untidy_decimal) => {
			*set = std::mem::take(set)
				.into_iter()
				.map(|mut v| {
					normalise_decimals(&mut v);
					v
				})
				.collect();
		}
		Value::Object(Object(o)) => o.values_mut().for_each(normalise_decimals),
		Value::RecordId(r) => normalise_key(&mut r.key),
		Value::Range(r) => {
			let Range {
				start,
				end,
			} = &mut **r;
			bound(start);
			bound(end);
		}
		_ => {}
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
	use crate::catalog::{
		BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION, BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION,
		DatabaseId, INDEX_FORMAT_VERSION, IndexId, NamespaceId,
	};
	use crate::key::schema::EntryKey;
	use crate::val::{RecordIdentity, Set, Strand};

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

	fn current_space(tb: &TableName) -> IndexKeyspace<'_> {
		at_version(tb, BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION)
	}

	/// The stamp before decimals are spelled without trailing zeros.
	fn earlier_space(tb: &TableName) -> IndexKeyspace<'_> {
		at_version(tb, BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION - 1)
	}

	#[test]
	fn the_stamp_selects_the_decimal_spelling() {
		let tb = TableName::from("t");
		let current = BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION;
		assert!(spells_normalised_decimal_keys(current));
		assert!(!spells_normalised_decimal_keys(current - 1));
		assert!(!spells_normalised_decimal_keys(0));
		assert!(spells_normalised_decimal_keys(INDEX_FORMAT_VERSION));
		for format_version in [0, current - 1, current, INDEX_FORMAT_VERSION] {
			assert_eq!(
				at_version(&tb, format_version).normalised_decimals,
				spells_normalised_decimal_keys(format_version),
				"format version {format_version}"
			);
		}
	}

	fn dec(text: &str) -> Value {
		Value::Number(Number::Decimal(text.parse().unwrap()))
	}

	fn object_of(key: &str, value: Value) -> Object {
		Object(std::iter::once((Strand::from(key), value)).collect())
	}

	/// Values holding the decimal `text` in every place a key spells one.
	fn holding(text: &str) -> Vec<Value> {
		let d = || dec(text);
		let id = |key: RecordIdKey| Value::RecordId(RecordId::new(TableName::from("r"), key));
		vec![
			d(),
			Value::Array(Array::from(vec![Value::from(1), d()])),
			Value::Set(Set(std::iter::once(d()).collect())),
			Value::Object(object_of("k", d())),
			id(RecordIdKey::Array(Array::from(vec![d()]))),
			id(RecordIdKey::Object(object_of("k", d()))),
			id(RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Included(RecordIdKey::Array(Array::from(vec![d()]))),
				end: Bound::Unbounded,
			}))),
			Value::Range(Box::new(Range {
				start: Bound::Included(d()),
				end: Bound::Excluded(Value::Array(Array::from(vec![d()]))),
			})),
		]
	}

	#[test]
	fn equal_decimals_share_a_key_on_an_index_that_spells_them_without_trailing_zeros() {
		let tb = TableName::from("t");
		let id = RecordIdKey::from("r".to_owned());
		for (untidy, tidy) in holding("2.50").into_iter().zip(holding("2.5")) {
			assert_eq!(untidy, tidy, "the two spellings compare equal");
			for unique in [false, true] {
				let (a, b) = (slice::from_ref(&untidy), slice::from_ref(&tidy));
				let ks = current_space(&tb);
				assert_eq!(
					ks.stored_key(unique, a, &id).unwrap(),
					ks.stored_key(unique, b, &id).unwrap(),
					"unique={unique}: {untidy:?}"
				);
				// An index that spells them as written keeps two keys.
				let old = earlier_space(&tb);
				assert_ne!(
					old.stored_key(unique, a, &id).unwrap(),
					old.stored_key(unique, b, &id).unwrap(),
					"unique={unique}: {untidy:?} on an index that does not normalise"
				);
			}
		}
		// An integer and a decimal with a zero after its point compare equal too.
		let (decimal, integer) = (dec("2.0"), Value::from(2));
		assert_eq!(decimal, integer);
		let (a, b) = (slice::from_ref(&decimal), slice::from_ref(&integer));
		let (ks, old) = (current_space(&tb), earlier_space(&tb));
		assert_eq!(ks.stored_key(false, a, &id).unwrap(), ks.stored_key(false, b, &id).unwrap());
		assert_ne!(old.stored_key(false, a, &id).unwrap(), old.stored_key(false, b, &id).unwrap());
	}

	#[test]
	fn a_read_bound_spelled_either_way_reaches_an_entry_spelled_either_way() {
		let tb = TableName::from("t");
		let ks = current_space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		let (untidy, tidy) = (dec("2.50"), dec("2.5"));
		let (below, above) = (dec("2.49"), dec("2.51"));
		for unique in [false, true] {
			for stored in [&untidy, &tidy] {
				let key = ks.stored_key(unique, slice::from_ref(stored), &id).unwrap().key;
				for probe in [&untidy, &tidy] {
					let eq = ks.equality(unique, probe).unwrap();
					assert!(contains(&eq, &key), "unique={unique}: = {probe:?} misses {stored:?}");
					let inclusive = [
						ks.range(Bound::Included(probe), Bound::Unbounded).unwrap(),
						ks.range(Bound::Unbounded, Bound::Included(probe)).unwrap(),
						ks.range(Bound::Included(probe), Bound::Included(probe)).unwrap(),
					];
					for range in &inclusive {
						assert!(contains(range, &key), "unique={unique}: inclusive on {probe:?}");
					}
					let exclusive = [
						ks.range(Bound::Excluded(probe), Bound::Unbounded).unwrap(),
						ks.range(Bound::Unbounded, Bound::Excluded(probe)).unwrap(),
					];
					for range in &exclusive {
						assert!(!contains(range, &key), "unique={unique}: exclusive on {probe:?}");
					}
					let between =
						ks.range(Bound::Excluded(&below), Bound::Excluded(&above)).unwrap();
					assert!(contains(&between, &key));
				}
			}
		}
	}

	#[test]
	fn a_compound_prefix_and_range_value_are_spelled_without_trailing_zeros() {
		let tb = TableName::from("t");
		let ks = current_space(&tb);
		let id = RecordIdKey::from("r".to_owned());
		let ops = [
			(BinaryOperator::Equal, true),
			(BinaryOperator::MoreThanEqual, true),
			(BinaryOperator::LessThanEqual, true),
			(BinaryOperator::MoreThan, false),
			(BinaryOperator::LessThan, false),
		];
		for unique in [false, true] {
			for (lead, last) in [(dec("2.50"), dec("7.50")), (dec("2.5"), dec("7.5"))] {
				let key = ks.stored_key(unique, &[lead.clone(), last.clone()], &id).unwrap().key;
				for (probe_lead, probe_last) in
					[(dec("2.50"), dec("7.5")), (dec("2.5"), dec("7.50"))]
				{
					let prefix = slice::from_ref(&probe_lead);
					let under = ks.compound(prefix, None).unwrap();
					assert!(contains(&under, &key), "prefix {probe_lead:?} misses ({lead:?}, ..)");
					for (op, admits) in &ops {
						let range = ks.compound(prefix, Some((op, &probe_last))).unwrap();
						assert_eq!(
							contains(&range, &key),
							*admits,
							"{op:?} {probe_last:?} on ({lead:?}, {last:?})"
						);
					}
				}
			}
		}
	}

	/// Ids of one value whose numbers differ only in kind, and others around them.
	fn kinded_ids() -> Vec<RecordIdKey> {
		let arr = |n: Number| RecordIdKey::Array(Array::from(vec![Value::Number(n)]));
		vec![
			arr(Number::Int(1)),
			arr(Number::Float(1.0)),
			arr(Number::Decimal(1.into())),
			arr(Number::Int(2)),
			RecordIdKey::Number(1),
			RecordIdKey::from("a".to_owned()),
			RecordIdKey::Array(Array::from(vec![
				Value::from("a"),
				datetime("2026-08-28T09:00:00Z"),
			])),
		]
	}

	/// From the stamp that spells it, an entry's id is a `RecordIdentity`: the key
	/// [`IndexKeyspace::entry_key`] assembles is the `entry` route's, under both
	/// datetime spellings, values respelled as the stamp asks.
	#[test]
	fn an_entry_key_is_the_entry_route_with_the_id_as_an_identity() {
		let tb = TableName::from("t");
		let ks = at_version(&tb, BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION);
		for fd in [vec![Value::from(1)], vec![dec("2.50"), datetime("2026-08-28T09:00:00.5Z")]] {
			for id in kinded_ids() {
				let route = ks
					.spelling
					.encode(&EntryKey {
						ns: NamespaceId(1),
						db: DatabaseId(2),
						tb: Cow::Borrowed(&tb),
						ix: IndexId(3),
						fd: ks.spelled_all(&fd),
						id: RecordIdentity(id.clone()),
					})
					.unwrap();
				assert_eq!(ks.entry_key(&fd, &id).unwrap(), route, "{fd:?} {id:?}");
			}
		}
	}

	/// Records whose ids differ only in a number's kind have an entry each from
	/// the stamp that spells the id as an identity, and share one before it. The
	/// two spellings are one exactly for an id that holds no number.
	#[test]
	fn ids_differing_in_a_numbers_kind_are_two_entries_from_the_identity_stamp() {
		let tb = TableName::from("t");
		let fd = [Value::from(1)];
		let (current, earlier) = (
			at_version(&tb, BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION),
			at_version(&tb, BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION - 1),
		);
		let ids = kinded_ids();
		let (int, float, decimal) = (&ids[0], &ids[1], &ids[2]);
		for (a, b) in [(int, float), (int, decimal), (float, decimal)] {
			assert_ne!(current.entry_key(&fd, a).unwrap(), current.entry_key(&fd, b).unwrap());
			assert_eq!(earlier.entry_key(&fd, a).unwrap(), earlier.entry_key(&fd, b).unwrap());
		}
		for id in &ids {
			let alike = current.entry_key(&fd, id).unwrap() == earlier.entry_key(&fd, id).unwrap();
			assert_eq!(alike, id.index_format_spells_alike(), "{id:?}");
		}
		// Both entries of a unique index's nullish tuple are kept apart too.
		for unique in [false, true] {
			let none = [Value::None];
			assert_ne!(
				current.stored_key(unique, &none, int).unwrap(),
				current.stored_key(unique, &none, decimal).unwrap()
			);
		}
	}

	/// The identity adds a number's kind after the number, so the entries of one
	/// value list their records in record id order: by value, then by kind among
	/// equal numbers, as a table scan lists them.
	#[test]
	fn entries_of_one_value_list_their_records_in_record_id_order() {
		let tb = TableName::from("t");
		let ks = at_version(&tb, BTREE_IDENTITY_ENTRY_IDS_FORMAT_VERSION);
		let fd = [Value::from(1)];
		let arr = |n: Number| RecordIdKey::Array(Array::from(vec![Value::Number(n)]));
		let ordered = [
			arr(Number::Int(0)),
			arr(Number::Int(1)),
			arr(Number::Float(1.0)),
			arr(Number::Decimal(1.into())),
			arr(Number::Float(1.5)),
			arr(Number::Int(2)),
		];
		let keys: Vec<_> = ordered.iter().map(|id| ks.entry_key(&fd, id).unwrap()).collect();
		for (pair, ids) in keys.windows(2).zip(ordered.windows(2)) {
			assert!(pair[0] < pair[1], "{:?} before {:?}", ids[0], ids[1]);
			let record = |id: &RecordIdKey| storekey::encode_vec(id).unwrap();
			assert!(record(&ids[0]) < record(&ids[1]), "record key order agrees");
		}
	}

	#[test]
	fn the_records_own_id_is_never_respelled() {
		let tb = TableName::from("t");
		let ks = current_space(&tb);
		let fd = [Value::from(1)];
		let id = |text: &str| RecordIdKey::Array(Array::from(vec![dec(text)]));
		assert_ne!(ks.entry_key(&fd, &id("2.50")).unwrap(), ks.entry_key(&fd, &id("2.5")).unwrap());
	}

	#[test]
	fn a_value_with_nothing_to_respell_is_borrowed() {
		let tb = TableName::from("t");
		let ks = current_space(&tb);
		let tidy = holding("2.5");
		for v in &tidy {
			let one = slice::from_ref(v);
			assert!(matches!(ks.spelled_all(one), Cow::Borrowed(_)), "{v:?} was cloned");
		}
		assert!(matches!(ks.spelled_all(&tidy), Cow::Borrowed(_)));
		let untidy = holding("2.50");
		for v in &untidy {
			let one = slice::from_ref(v);
			assert!(matches!(ks.spelled_all(one), Cow::Owned(_)), "{v:?} was not respelled");
		}
		// An index that does not normalise never inspects, let alone clones.
		let old = earlier_space(&tb);
		assert!(matches!(old.spelled_all(&untidy), Cow::Borrowed(_)));
	}

	/// A decimal is untidy exactly when respelling it changes its scale: a
	/// zero after the point, a zero value included.
	#[test]
	fn a_decimal_is_untidy_when_its_point_is_followed_by_a_trailing_zero() {
		for (text, untidy) in [
			("2.50", true),
			("-2.50", true),
			("0.00", true),
			("-0.0", true),
			("100.0", true),
			("2.5", false),
			("-2.5", false),
			("10", false),
			("0", false),
			("0.0000000000000000000000000001", false),
		] {
			let Value::Number(Number::Decimal(d)) = dec(text) else {
				unreachable!("a decimal")
			};
			assert_eq!(holds_untidy_decimal(&dec(text)), untidy, "{text}");
			assert_eq!(d.normalize().scale() != d.scale(), untidy, "{text}");
		}
	}

	#[test]
	fn respelling_changes_only_the_scale_of_decimals() {
		let tb = TableName::from("t");
		let ks = current_space(&tb);
		for (untidy, tidy) in holding("2.50").iter().zip(holding("2.5")) {
			let spelled = ks.spelled_all(slice::from_ref(untidy));
			let [spelled] = &*spelled else {
				panic!("{untidy:?} was respelled into {} values", spelled.len());
			};
			assert_eq!(*spelled, tidy);
			assert_eq!(format!("{spelled:?}"), format!("{tidy:?}"));
		}
		for v in [Value::from(3), Value::from(2.50f64), dec("0.000"), dec("100"), dec("-1.10")] {
			let spelled = ks.spelled_all(slice::from_ref(&v));
			let [spelled] = &*spelled else {
				panic!("{v:?} was respelled into {} values", spelled.len());
			};
			assert_eq!(*spelled, v);
			if let Value::Number(Number::Decimal(d)) = spelled {
				assert_eq!(d.scale(), d.normalize().scale());
			}
		}
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
