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
//! [`IndexKeyspace::stored_key`] is the one place a write decides where an
//! entry goes, and [`IndexKeyspace::equality`], [`IndexKeyspace::range`] and
//! [`IndexKeyspace::compound`] are the ranges a read scans for a predicate.
//! The ranges are half-open, `[start, end)`, and contain exactly the keys
//! [`IndexKeyspace::stored_key`] writes for values the predicate admits.

use std::borrow::Cow;
use std::ops::Bound;
use std::slice;

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, IndexId, NamespaceId};
use crate::entry::IndexEntryValue;
use crate::expr::BinaryOperator;
use crate::key::schema::{EntryFdOpenPrefix, EntryFdPrefix, EntryKey, EntryPrefix, UniqueKey};
use crate::key::{KVKey, KVRange, KVSubspace, KVValue, Key, KeyRange, TypedRange};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

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

/// One index's keyspace: every key of its entries starts with these fields.
#[derive(Debug, Clone, Copy)]
pub struct IndexKeyspace<'a> {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: &'a TableName,
	pub ix: IndexId,
}

impl<'a> IndexKeyspace<'a> {
	/// The keyspace of index `ix`, defined in database `db` of namespace `ns`.
	pub fn of(ns: NamespaceId, db: DatabaseId, ix: &'a IndexDefinition) -> Self {
		Self {
			ns,
			db,
			tb: &ix.table_name,
			ix: ix.index_id,
		}
	}

	/// The key of a non-unique entry for values `fd` of the record keyed `id`.
	pub fn entry_key(&self, fd: &[Value], id: &RecordIdKey) -> Result<Key<'static>> {
		EntryKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: Cow::Borrowed(fd),
			id: Cow::Borrowed(id),
		}
		.encode_key()
	}

	/// The unique slot for values `fd`.
	pub fn unique_key(&self, fd: &[Value]) -> Result<Key<'static>> {
		UniqueKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: Cow::Borrowed(fd),
		}
		.encode_key()
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
		let prefix = self.prefix();
		Ok(self.typed((prefix.encode_bound()?..prefix.skip_extensions()?).into()))
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
			EntryFdPrefix {
				ns: self.ns,
				db: self.db,
				tb: Cow::Borrowed(self.tb),
				ix: self.ix,
				fd: Cow::Borrowed(fd),
			}
			.range()
		}
	}

	/// The entries of a single-column index whose value lies between `from`
	/// and `to`.
	pub fn range(
		&self,
		unique: bool,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<TypedRange<IndexEntryValue>> {
		let start = match from {
			Bound::Included(x) => self.lower_inclusive(unique, x)?,
			Bound::Excluded(x) => self.upper_inclusive(unique, x)?,
			Bound::Unbounded => self.prefix().encode_bound()?,
		};
		let end = match to {
			Bound::Included(x) => self.upper_inclusive(unique, x)?,
			Bound::Excluded(x) => self.lower_inclusive(unique, x)?,
			Bound::Unbounded => self.prefix().skip_extensions()?,
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
		let without = || self.open(prefix).encode_bound();
		let Some((op, value)) = range else {
			return Ok(self.typed(without()?.prefix_expect()));
		};
		let with = || {
			let mut fd = Vec::with_capacity(prefix.len() + 1);
			fd.extend_from_slice(prefix);
			fd.push(value.clone());
			self.open(&fd).encode_bound()
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

	/// The first key an entry for `[x]` may sit at.
	fn lower_inclusive(&self, unique: bool, x: &Value) -> Result<Key<'static>> {
		if unique && !x.is_nullish() {
			self.unique_key(slice::from_ref(x))
		} else {
			self.open(slice::from_ref(x)).encode_bound()
		}
	}

	/// The first key past every entry for `[x]`.
	fn upper_inclusive(&self, unique: bool, x: &Value) -> Result<Key<'static>> {
		if unique && !x.is_nullish() {
			Ok(self.unique_key(slice::from_ref(x))?.next_neighbour_expect())
		} else {
			self.open(slice::from_ref(x)).skip_extensions()
		}
	}

	fn prefix(&self) -> EntryPrefix<'a> {
		EntryPrefix {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
		}
	}

	/// The bound over every entry whose values start with `fd`.
	fn open<'b>(&self, fd: &'b [Value]) -> EntryFdOpenPrefix<'b>
	where
		'a: 'b,
	{
		EntryFdOpenPrefix {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(self.tb),
			ix: self.ix,
			fd: Cow::Borrowed(fd),
		}
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
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::val::{Array, Number, Object};

	fn space(tb: &TableName) -> IndexKeyspace<'_> {
		IndexKeyspace {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb,
			ix: IndexId(3),
		}
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
				let range = ks.range(unique, from, to).unwrap();
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
			let r = ks.range(unique, Bound::Unbounded, Bound::Unbounded).unwrap();
			assert_eq!((r.start(), r.end()), (all.start(), all.end()));
			let r = ks.equality(unique, &one).unwrap();
			assert!(all.start() <= r.start() && r.end() <= all.end());
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
