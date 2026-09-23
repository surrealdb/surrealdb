use std::cmp::Ordering;
use std::ops::Bound;

use common::fmt::{EscapeIdent, EscapeRidKey};
use rand::seq::IndexedRandom;
use revision::revisioned;
use storekey::{BorrowDecode, Encode};
use surrealdb_cnf::ID_CHARS;
use surrealdb_types::{SqlFormat, ToSql, write_sql};
use ulid::Ulid;

use crate::expr::{self};
use crate::val::table_name_public::{IntoPublicTable, IntoTableName};
use crate::val::{Array, IndexFormat, Number, Object, Range, Strand, TableName, Uuid, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Encode, BorrowDecode)]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub struct RecordIdKeyRange {
	pub start: Bound<RecordIdKey>,
	pub end: Bound<RecordIdKey>,
}

impl PartialOrd for RecordIdKeyRange {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for RecordIdKeyRange {
	fn cmp(&self, other: &Self) -> Ordering {
		fn compare_bounds(a: &Bound<RecordIdKey>, b: &Bound<RecordIdKey>) -> Ordering {
			match a {
				Bound::Unbounded => match b {
					Bound::Unbounded => Ordering::Equal,
					_ => Ordering::Less,
				},
				Bound::Included(a) => match b {
					Bound::Unbounded => Ordering::Greater,
					Bound::Included(b) => a.cmp(b),
					Bound::Excluded(_) => Ordering::Less,
				},
				Bound::Excluded(a) => match b {
					Bound::Excluded(b) => a.cmp(b),
					_ => Ordering::Greater,
				},
			}
		}
		match compare_bounds(&self.start, &other.end) {
			Ordering::Equal => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl ToSql for RecordIdKeyRange {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self.start {
			Bound::Unbounded => {}
			Bound::Included(ref x) => write_sql!(f, sql_fmt, "{x}"),
			Bound::Excluded(ref x) => write_sql!(f, sql_fmt, "{x}>"),
		}
		write_sql!(f, sql_fmt, "..");
		match self.end {
			Bound::Unbounded => {}
			Bound::Included(ref x) => write_sql!(f, sql_fmt, "={x}"),
			Bound::Excluded(ref x) => write_sql!(f, sql_fmt, "{x}"),
		}
	}
}

impl TryFrom<RecordIdKeyRange> for crate::types::PublicRecordIdKeyRange {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKeyRange) -> Result<Self, Self::Error> {
		Ok(crate::types::PublicRecordIdKeyRange {
			start: match value.start {
				Bound::Included(x) => Bound::Included(x.try_into()?),
				Bound::Excluded(x) => Bound::Excluded(x.try_into()?),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match value.end {
				Bound::Included(x) => Bound::Included(x.try_into()?),
				Bound::Excluded(x) => Bound::Excluded(x.try_into()?),
				Bound::Unbounded => Bound::Unbounded,
			},
		})
	}
}

impl From<crate::types::PublicRecordIdKeyRange> for RecordIdKeyRange {
	fn from(value: crate::types::PublicRecordIdKeyRange) -> Self {
		RecordIdKeyRange {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl RecordIdKeyRange {
	pub fn into_literal(self) -> expr::RecordIdKeyRangeLit {
		let start = self.start.map(|x| x.into_literal());
		let end = self.end.map(|x| x.into_literal());
		expr::RecordIdKeyRangeLit {
			start,
			end,
		}
	}

	/// Convertes a record id key range into the range from a normal value.
	pub fn into_value_range(self) -> Range {
		Range {
			start: self.start.map(|x| x.into_value()),
			end: self.end.map(|x| x.into_value()),
		}
	}

	/// Convertes a record id key range into the range from a normal value.
	pub fn from_value_range(range: Range) -> Option<Self> {
		let start = match range.start {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_value(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_value(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match range.end {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_value(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_value(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Some(RecordIdKeyRange {
			start,
			end,
		})
	}
}

impl PartialEq<Range> for RecordIdKeyRange {
	fn eq(&self, other: &Range) -> bool {
		(match self.start {
			Bound::Included(ref a) => {
				if let Bound::Included(ref b) = other.start {
					a == b
				} else {
					false
				}
			}
			Bound::Excluded(ref a) => {
				if let Bound::Excluded(ref b) = other.start {
					a == b
				} else {
					false
				}
			}
			Bound::Unbounded => matches!(other.start, Bound::Unbounded),
		}) && (match self.end {
			Bound::Included(ref a) => {
				if let Bound::Included(ref b) = other.end {
					a == b
				} else {
					false
				}
			}
			Bound::Excluded(ref a) => {
				if let Bound::Excluded(ref b) = other.end {
					a == b
				} else {
					false
				}
			}
			Bound::Unbounded => matches!(other.end, Bound::Unbounded),
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, BorrowDecode)]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub enum RecordIdKey {
	Number(i64),
	String(Strand),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Range(Box<RecordIdKeyRange>),
}

/// A record id carried inside a key as an *identity*, encoded losslessly
/// whatever format the enclosing key pins.
///
/// [`IndexFormat`] erases a number's kind deliberately: an index key stands for
/// its whole equality class, so a lookup for `1` has to find a row stored as
/// `1f`. A record id in a key is not matched that way — it addresses a record —
/// so erasing its type is precisely what breaks it. Two distinct records
/// collapse onto one key, and an id decoded back out names a record that was
/// never stored under it, because the record itself is addressed by the default
/// codec, which carries the number's kind.
///
/// Encoding and decoding therefore go through the default codec under every
/// format, so an identity cannot pick up the value encoding by sitting in a key
/// that pins one. Key layouts that carry a record id use this rather than
/// [`RecordIdKey`] directly; the bare type stays available for the value side,
/// where the erasure is what is wanted.
#[derive(Clone, Debug)]
pub struct RecordIdentity(pub RecordIdKey);

/// The bytes a record is addressed by.
///
/// A key that will not encode cannot be stored under one either, but the
/// comparisons built on this have to stay total, so that case falls back to the
/// debug spelling — which distinguishes the numeric variants too, so the
/// fallback upholds the same property.
fn addressed_by(key: &RecordIdKey) -> Vec<u8> {
	storekey::encode_vec(key).unwrap_or_else(|_| format!("{key:?}").into_bytes())
}

impl RecordIdentity {
	/// The bytes this identity is addressed by.
	///
	/// Equality, ordering and hashing all go through the encoding rather than
	/// through [`RecordIdKey`]'s own comparison, which is cross-variant: `[1]`
	/// and `[1dec]` compare equal there while addressing two different records,
	/// and keeping those apart is what this type exists for. Deriving the
	/// comparisons would reintroduce, between identities, the collapse the type
	/// is here to prevent.
	fn addressed_by(&self) -> Vec<u8> {
		addressed_by(&self.0)
	}
}

impl PartialEq for RecordIdentity {
	fn eq(&self, other: &Self) -> bool {
		self.addressed_by() == other.addressed_by()
	}
}

impl Eq for RecordIdentity {}

impl PartialOrd for RecordIdentity {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for RecordIdentity {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.addressed_by().cmp(&other.addressed_by())
	}
}

impl std::hash::Hash for RecordIdentity {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.addressed_by().hash(state);
	}
}

impl RecordIdentity {
	pub fn into_inner(self) -> RecordIdKey {
		self.0
	}
}

impl AsRef<RecordIdKey> for RecordIdentity {
	fn as_ref(&self) -> &RecordIdKey {
		&self.0
	}
}

impl From<RecordIdKey> for RecordIdentity {
	fn from(id: RecordIdKey) -> Self {
		Self(id)
	}
}

impl From<RecordIdentity> for RecordIdKey {
	fn from(id: RecordIdentity) -> Self {
		id.0
	}
}

impl<F> Encode<F> for RecordIdentity {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		Encode::<()>::encode(&self.0, w)
	}
}

impl<'de, F> BorrowDecode<'de, F> for RecordIdentity {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		Ok(Self(BorrowDecode::<'de, ()>::borrow_decode(r)?))
	}
}

impl RecordIdKey {
	/// Whether `self` and `other` address the same record.
	///
	/// **Not `==`.** [`Number`]'s equality is cross-variant, so `[1] == [1dec]`
	/// while the two are stored, fetched and indexed under different keys — an
	/// equality check therefore accepts exactly the collision a caller asking
	/// this question is trying to rule out. The comparison is on the encoding,
	/// which is what addresses the record.
	///
	/// This is the comparison behind [`RecordIdentity`]'s own equality; it is
	/// exposed separately for the callers that hold bare keys, chiefly the
	/// doc-ID mapping, where a forward hit is adopted only once the reverse
	/// mapping agrees it names the record asked for.
	pub fn addresses_same_record(&self, other: &Self) -> bool {
		addressed_by(self) == addressed_by(other)
	}

	/// [`Value::hash_agrees_with_eq`] for a record id's key.
	///
	/// [`RecordIdKey::Number`] is an `i64`, not a [`crate::val::Number`], so plain
	/// record ids are always decisive; only a compound id can hold one.
	pub fn hash_agrees_with_eq(&self) -> bool {
		match self {
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => true,
			RecordIdKey::Array(a) => a.iter().all(Value::hash_agrees_with_eq),
			RecordIdKey::Object(o) => o.values().all(Value::hash_agrees_with_eq),
			RecordIdKey::Range(_) => false,
		}
	}

	/// Generate a new random ID
	pub fn rand() -> Self {
		let id: String = crate::rnd::with_rng(|rng| {
			(0..20).map(|_| *ID_CHARS[..].choose(&mut *rng).unwrap_or(&'0')).collect()
		});
		Self::String(id.into())
	}
	/// Generate a new random ULID
	pub fn ulid() -> Self {
		Self::String(Ulid::new().to_string().into())
	}
	/// Generate a new random UUID
	pub fn uuid() -> Self {
		Self::Uuid(Uuid::new_v7())
	}

	/// Returns if this key is a range.
	pub fn is_range(&self) -> bool {
		matches!(self, RecordIdKey::Range(_))
	}

	/// Returns surrealql value of this key.
	pub fn into_value(self) -> Value {
		match self {
			RecordIdKey::Number(n) => Value::Number(Number::Int(n)),
			RecordIdKey::String(s) => Value::String(s),
			RecordIdKey::Uuid(u) => Value::Uuid(u),
			RecordIdKey::Object(object) => Value::Object(object),
			RecordIdKey::Array(array) => Value::Array(array),
			RecordIdKey::Range(range) => Value::Range(Box::new(Range {
				start: range.start.map(RecordIdKey::into_value),
				end: range.end.map(RecordIdKey::into_value),
			})),
		}
	}

	/// Tries to convert a value into a record id key,
	///
	/// Returns None if the value cannot be converted.
	pub fn from_value(value: Value) -> Option<Self> {
		// NOTE: This method dictates how coversion between values and record id keys
		// behave. This method is reimplementing previous (before expr inversion pr)
		// behavior but I am not sure if it is the right one, float and decimal
		// generaly implicitly convert to other number types but here they are
		// rejected.
		match value {
			Value::Number(Number::Int(i)) => Some(RecordIdKey::Number(i)),
			Value::String(strand) => Some(RecordIdKey::String(strand)),
			// NOTE: This was previously (before expr inversion pr) also rejected in this
			// conversion, a bug I assume.
			Value::Uuid(uuid) => Some(RecordIdKey::Uuid(uuid)),
			Value::Array(array) => Some(RecordIdKey::Array(array)),
			Value::Object(object) => Some(RecordIdKey::Object(object)),
			Value::Range(range) => {
				RecordIdKeyRange::from_value_range(*range).map(|x| RecordIdKey::Range(Box::new(x)))
			}
			_ => None,
		}
	}

	/// Returns the expression which evaluates to the same value
	pub fn into_literal(self) -> expr::RecordIdKeyLit {
		match self {
			RecordIdKey::Number(n) => expr::RecordIdKeyLit::Number(n),
			RecordIdKey::String(s) => expr::RecordIdKeyLit::String(s),
			RecordIdKey::Uuid(uuid) => expr::RecordIdKeyLit::Uuid(uuid),
			RecordIdKey::Object(object) => expr::RecordIdKeyLit::Object(object.into_literal()),
			RecordIdKey::Array(array) => expr::RecordIdKeyLit::Array(array.into_literal()),
			RecordIdKey::Range(range) => {
				expr::RecordIdKeyLit::Range(Box::new(range.into_literal()))
			}
		}
	}
}

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		RecordIdKey::Number(value)
	}
}

impl From<String> for RecordIdKey {
	fn from(value: String) -> Self {
		RecordIdKey::String(value.into())
	}
}

impl From<Strand> for RecordIdKey {
	fn from(value: Strand) -> Self {
		RecordIdKey::String(value)
	}
}

impl From<Uuid> for RecordIdKey {
	fn from(value: Uuid) -> Self {
		RecordIdKey::Uuid(value)
	}
}
impl From<Object> for RecordIdKey {
	fn from(value: Object) -> Self {
		RecordIdKey::Object(value)
	}
}
impl From<Array> for RecordIdKey {
	fn from(value: Array) -> Self {
		RecordIdKey::Array(value)
	}
}
impl From<Box<RecordIdKeyRange>> for RecordIdKey {
	fn from(value: Box<RecordIdKeyRange>) -> Self {
		RecordIdKey::Range(value)
	}
}

impl From<crate::types::PublicRecordIdKey> for RecordIdKey {
	fn from(value: crate::types::PublicRecordIdKey) -> Self {
		match value {
			crate::types::PublicRecordIdKey::Number(x) => Self::Number(x),
			crate::types::PublicRecordIdKey::String(x) => Self::String(x.into()),
			crate::types::PublicRecordIdKey::Uuid(x) => Self::Uuid(x.into()),
			crate::types::PublicRecordIdKey::Array(x) => Self::Array(x.into()),
			crate::types::PublicRecordIdKey::Object(x) => Self::Object(x.into()),
			crate::types::PublicRecordIdKey::Range(x) => Self::Range(Box::new((*x).into())),
		}
	}
}

impl TryFrom<RecordIdKey> for crate::types::PublicRecordIdKey {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self, Self::Error> {
		Ok(match value {
			RecordIdKey::Number(x) => Self::Number(x),
			RecordIdKey::String(x) => Self::String(x.into()),
			RecordIdKey::Uuid(x) => Self::Uuid(x.into()),
			RecordIdKey::Array(x) => Self::Array(x.try_into()?),
			RecordIdKey::Object(x) => Self::Object(x.try_into()?),
			RecordIdKey::Range(x) => Self::Range(Box::new((*x).try_into()?)),
		})
	}
}

impl PartialEq<Value> for RecordIdKey {
	fn eq(&self, other: &Value) -> bool {
		match self {
			RecordIdKey::Number(a) => Value::Number(Number::Int(*a)) == *other,
			RecordIdKey::String(a) => {
				if let Value::String(b) = other {
					a.as_str() == b.as_str()
				} else {
					false
				}
			}
			RecordIdKey::Uuid(a) => {
				if let Value::Uuid(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Object(a) => {
				if let Value::Object(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Array(a) => {
				if let Value::Array(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Range(a) => {
				if let Value::Range(b) = other {
					**a == **b
				} else {
					false
				}
			}
		}
	}
}

impl ToSql for RecordIdKey {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			RecordIdKey::Number(n) => write_sql!(f, sql_fmt, "{n}"),
			RecordIdKey::String(v) => write_sql!(f, sql_fmt, "{}", EscapeRidKey(v.as_str())),
			RecordIdKey::Uuid(uuid) => write_sql!(f, sql_fmt, "{}", uuid),
			RecordIdKey::Object(object) => write_sql!(f, sql_fmt, "{}", object),
			RecordIdKey::Array(array) => write_sql!(f, sql_fmt, "{}", array),
			RecordIdKey::Range(rid) => write_sql!(f, sql_fmt, "{}", rid),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, BorrowDecode)]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub struct RecordId {
	pub table: TableName,
	pub key: RecordIdKey,
}

impl RecordId {
	/// Creates a new record id from the given table and key
	pub fn new<K>(table: TableName, key: K) -> Self
	where
		RecordIdKey: From<K>,
	{
		RecordId {
			table,
			key: key.into(),
		}
	}

	/// Turns the record id into a literal which resolves to the same value.
	pub fn into_literal(self) -> expr::RecordIdLit {
		expr::RecordIdLit {
			table: self.table,
			key: self.key.into_literal(),
		}
	}

	pub fn is_table_type(&self, tables: &[TableName]) -> bool {
		tables.is_empty() || tables.contains(&self.table)
	}
}

impl TryFrom<RecordId> for crate::types::PublicRecordId {
	type Error = anyhow::Error;

	fn try_from(value: RecordId) -> Result<Self, Self::Error> {
		Ok(crate::types::PublicRecordId {
			table: value.table.into_public_table(),
			key: value.key.try_into()?,
		})
	}
}

impl From<crate::types::PublicRecordId> for RecordId {
	fn from(value: crate::types::PublicRecordId) -> Self {
		RecordId {
			table: value.table.into_table_name(),
			key: RecordIdKey::from(value.key),
		}
	}
}

impl ToSql for RecordId {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		// The table half is an identifier, so it escapes like one: `EscapeRidKey`
		// is for the key half, where an all-digit name must be quoted to keep it
		// from reading as an integer, and where a reserved word or a leading
		// digit is harmless. In table position both of those must be quoted or
		// the record id cannot be read back.
		write_sql!(f, sql_fmt, "{}:{}", EscapeIdent(self.table.as_str()), self.key)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A record id must render so that it reads back as the same record id.
	///
	/// The table half was rendered with `EscapeRidKey`, which is the escaper
	/// for the *key* half: it backticks an all-digit key so it cannot be
	/// mistaken for an integer, but it checks neither reserved words nor a
	/// leading digit, which is what a table name needs. So `select:1` and
	/// `1a:1` were emitted, and neither parses back.
	///
	/// This is not only a display concern. Export writes `record.data.to_sql()`
	/// into `INSERT` statements, so a dump of such a table could not be
	/// re-imported.
	#[test]
	fn record_ids_render_so_they_can_be_read_back() {
		for name in ["select", "1a", "9lives", "table", "person", "_123", "a-b"] {
			let rid = RecordId {
				table: TableName::from(name),
				key: RecordIdKey::Number(1),
			};
			let rendered = rid.to_sql();
			let reparsed = crate::syn::expr(&rendered)
				.unwrap_or_else(|e| panic!("{rendered} does not parse back: {e}"));
			let crate::sql::Expr::Literal(crate::sql::Literal::RecordId(lit)) = &reparsed else {
				panic!("{rendered} parsed as {reparsed:?}, not a record id");
			};
			assert_eq!(lit.table.as_str(), name, "{rendered} round-tripped to a different table");
		}
	}

	/// `IndexFormat` erases a number's [`NumberKind`], so a compound record id
	/// does not survive a round trip through it: every nested `Int`/`Float`
	/// decodes back as a `Decimal`, and two ids differing only in numeric
	/// variant encode to the same bytes.
	///
	/// That is intended for an ordinary index key, which is only ever compared
	/// and ordered. It is not safe for a key whose id component is decoded back
	/// into a record identity and used to fetch the record: the fetch re-encodes
	/// the id in the default format, which *does* carry the kind, so the
	/// decimalised id addresses a key that no record was stored under.
	///
	/// The assertions are on encoded bytes rather than on `==`, because
	/// [`Number`]'s equality is approximate across variants — `Float(1.5)`
	/// compares equal to `Decimal(1.5)` while their record keys differ.
	#[test]
	fn index_format_does_not_round_trip_a_compound_record_id() {
		fn arr(n: Number) -> RecordIdKey {
			RecordIdKey::Array(Array::from(vec![Value::Number(n)]))
		}
		/// The bytes a record is actually stored and fetched under.
		fn record_key(key: &RecordIdKey) -> Vec<u8> {
			storekey::encode_vec(key).unwrap()
		}
		/// The id as it comes back out of an `IndexFormat` key.
		fn through_index_format(key: &RecordIdKey) -> RecordIdKey {
			let enc = storekey::encode_vec_format::<IndexFormat, _>(key).unwrap();
			storekey::decode_borrow_format::<IndexFormat, _>(&enc).unwrap()
		}

		let int = arr(Number::Int(1));
		let float = arr(Number::Float(1.5));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));

		// An Int comes back as a Decimal, and so addresses a different record key.
		assert!(matches!(through_index_format(&int), RecordIdKey::Array(ref a)
			if matches!(a.first(), Some(Value::Number(Number::Decimal(_))))));
		assert_ne!(
			record_key(&through_index_format(&int)),
			record_key(&int),
			"`[1]` does not address its own record after an IndexFormat round trip"
		);
		assert_ne!(
			record_key(&through_index_format(&float)),
			record_key(&float),
			"`[1.5f]` does not address its own record after an IndexFormat round trip"
		);
		// A Decimal is the fixed point, which is why only decimal ids survive today.
		assert_eq!(record_key(&through_index_format(&dec)), record_key(&dec));

		// The encoding is also not injective: `[1]` and `[1dec]` are distinct
		// records that collide on one key.
		assert_ne!(record_key(&int), record_key(&dec), "they are distinct records");
		assert_eq!(
			storekey::encode_vec_format::<IndexFormat, _>(&int).unwrap(),
			storekey::encode_vec_format::<IndexFormat, _>(&dec).unwrap(),
			"`[1]` and `[1dec]` encode to the same IndexFormat key"
		);

		// The default format carries the kind, so it does round trip.
		let back: RecordIdKey = storekey::decode_borrow(&record_key(&int)).unwrap();
		assert_eq!(record_key(&back), record_key(&int));
	}

	/// [`RecordIdentity`] is what a key uses instead, and it holds the property
	/// the bare type does not: it round trips and stays injective even when the
	/// enclosing key pins [`IndexFormat`].
	#[test]
	fn record_identity_round_trips_under_index_format() {
		fn arr(n: Number) -> RecordIdKey {
			RecordIdKey::Array(Array::from(vec![Value::Number(n)]))
		}
		fn through_index_format(key: &RecordIdKey) -> RecordIdKey {
			let id = RecordIdentity(key.clone());
			let enc = storekey::encode_vec_format::<IndexFormat, _>(&id).unwrap();
			storekey::decode_borrow_format::<IndexFormat, RecordIdentity>(&enc)
				.unwrap()
				.into_inner()
		}

		let int = arr(Number::Int(1));
		let float = arr(Number::Float(1.5));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));
		let nested = RecordIdKey::Object(
			[("k".to_owned(), Value::Number(Number::Int(2)))].into_iter().collect(),
		);

		for id in [&int, &float, &dec, &nested] {
			assert_eq!(
				storekey::encode_vec(&through_index_format(id)).unwrap(),
				storekey::encode_vec(id).unwrap(),
				"{id:?} must address its own record after the round trip"
			);
		}

		// And distinct records stay on distinct keys.
		assert_ne!(
			storekey::encode_vec_format::<IndexFormat, _>(&RecordIdentity(int)).unwrap(),
			storekey::encode_vec_format::<IndexFormat, _>(&RecordIdentity(dec)).unwrap(),
			"`[1]` and `[1dec]` must not share a key"
		);
	}
}

surrealdb_kvs::impl_kv_value_revisioned!(RecordId);
surrealdb_kvs::impl_kv_value_revisioned!(RecordIdKey);

#[cfg(test)]
mod record_identity_comparison_tests {
	use std::collections::HashSet;

	use super::*;

	fn identity(n: Number) -> RecordIdentity {
		RecordIdentity(RecordIdKey::Array(vec![Value::Number(n)].into()))
	}

	/// The type exists to keep two ids apart that [`RecordIdKey`] itself calls
	/// equal, so its own comparison must not inherit that. Deriving it does,
	/// which would put both records in one slot of any set or map keyed by it.
	#[test]
	fn identities_differing_only_in_numeric_variant_are_not_equal() {
		let int = identity(Number::Int(1));
		let dec = identity(Number::Decimal(1.into()));

		assert_ne!(int, dec, "the identities must not collapse");
		assert_ne!(
			storekey::encode_vec(&int.0).unwrap(),
			storekey::encode_vec(&dec.0).unwrap(),
			"...because they are stored under different keys"
		);
		assert_eq!(
			HashSet::from([int.clone(), dec.clone()]).len(),
			2,
			"and a set keyed by identity must hold both"
		);
		assert_ne!(int.cmp(&dec), std::cmp::Ordering::Equal, "ordering must agree");
		assert_eq!(int, int.clone(), "an identity still equals itself");
	}
}
