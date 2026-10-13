use std::hash::{Hash, Hasher};

use rust_decimal::Decimal;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, RecordIdLit};
use crate::val::{Bytes, Datetime, Duration, File, Geometry, Regex, Uuid};

// Note: Literal uses `Strand` (not `String`) for the inner value of
// `Literal::String` so that converting between `Value` and `Expr` via
// `into_literal()` is a zero-cost move rather than a heap allocation.
// `Literal` is not revisioned, so there is no on-disk compatibility
// concern with changing the inner type.

/// A literal value, should be computed to get an actual value.
///
/// # Note regarding equality.
/// A literal is equal to an other literal if it is the exact same byte
/// representation, so normal float rules regarding equality do not apply, i.e.
/// if `a != b` then `Literal::Float(a)` could still be equal
/// to `Literal::Float(b)` in the case of `NaN` floats for example. Also
/// surrealql rules regarding number equality are not observed, 1f != 1dec.

#[derive(Clone, Debug)]
pub enum Literal {
	None,
	Null,
	// An unbounded range, i.e. `..` without any start or end bound.
	UnboundedRange,
	Bool(bool),
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
	String(Strand),
	Bytes(Bytes),
	Regex(Regex),
	RecordId(RecordIdLit),
	Array(Vec<Expr>),
	Set(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Geometry(Geometry),
	File(File),
}

impl Literal {
	pub fn is_static(&self) -> bool {
		match self {
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.is_static(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Set(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Object(items) => items.iter().all(|x| x.value.is_static()),
		}
	}

	/// Whether evaluating this literal can modify data. Composite literals
	/// (arrays, sets, objects, record ids) evaluate their element expressions
	/// in place, so those must be inspected; scalars cannot write.
	pub fn read_only(&self) -> bool {
		match self {
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.read_only(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.read_only()),
			Literal::Set(exprs) => exprs.iter().all(|x| x.read_only()),
			Literal::Object(items) => items.iter().all(|x| x.value.read_only()),
		}
	}

	/// The value this literal denotes, when it denotes one on its own.
	///
	/// `Some` when no part of the literal needs the evaluator: the scalars, and
	/// the collections whose every element is itself directly convertible. The
	/// result is required to equal what evaluating the literal produces, so a
	/// caller may use either interchangeably. In particular an object's entries
	/// are ordered and a repeated key resolves to its last occurrence, matching
	/// evaluation.
	///
	/// `None` for anything whose value is not a property of the literal alone:
	/// a record id with a generated key (the key is allocated when the literal
	/// is evaluated), and any collection containing such an element. `None` is
	/// always a safe answer, so a caller must treat it as "ask the evaluator"
	/// rather than as "not a value".
	///
	/// This is the shape a `.surql` import consists of almost entirely, and
	/// answering it without the evaluator is what keeps import off the async
	/// stack: see the callers in the legacy literal evaluator and in the
	/// planner.
	pub fn as_static_value(&self) -> Option<crate::val::Value> {
		use crate::val::Value;

		let value = match self {
			Literal::None => Value::None,
			Literal::Null => Value::Null,
			Literal::UnboundedRange => Value::Range(Box::new(crate::val::Range::unbounded())),
			Literal::Bool(x) => Value::Bool(*x),
			Literal::Float(x) => Value::Number(crate::val::Number::Float(*x)),
			Literal::Integer(x) => Value::Number(crate::val::Number::Int(*x)),
			Literal::Decimal(x) => Value::Number(crate::val::Number::Decimal(*x)),
			Literal::String(x) => Value::String(x.clone()),
			Literal::Bytes(x) => Value::Bytes(x.clone()),
			Literal::Regex(x) => Value::Regex(x.clone()),
			Literal::Duration(x) => Value::Duration(*x),
			Literal::Datetime(x) => Value::Datetime(*x),
			Literal::Uuid(x) => Value::Uuid(*x),
			Literal::Geometry(x) => Value::Geometry(x.clone()),
			Literal::File(x) => Value::File(x.clone()),
			// Only the key forms that are values in their own right. A
			// generated key is allocated during evaluation, and the compound
			// keys hold expressions.
			Literal::RecordId(rid) => {
				use crate::expr::RecordIdKeyLit;
				let key = match &rid.key {
					RecordIdKeyLit::Number(x) => crate::val::RecordIdKey::Number(*x),
					RecordIdKeyLit::String(x) => crate::val::RecordIdKey::String(x.clone()),
					RecordIdKeyLit::Uuid(x) => crate::val::RecordIdKey::Uuid(*x),
					_ => return None,
				};
				Value::RecordId(crate::val::RecordId::new(rid.table.clone(), key))
			}
			Literal::Array(exprs) => {
				let mut values = Vec::with_capacity(exprs.len());
				for e in exprs {
					values.push(e.as_static_value()?);
				}
				Value::Array(crate::val::Array(values))
			}
			Literal::Set(exprs) => {
				let mut set = crate::val::Set::new();
				for e in exprs {
					set.insert(e.as_static_value()?);
				}
				Value::Set(set)
			}
			// Built through a sorted map, exactly as evaluation builds it, so
			// key ordering and duplicate-key resolution cannot drift apart.
			Literal::Object(items) => {
				let mut map = std::collections::BTreeMap::new();
				for i in items {
					map.insert(i.key.clone(), i.value.as_static_value()?);
				}
				Value::Object(crate::val::Object::from(map))
			}
		};
		Some(value)
	}
}

impl PartialEq for Literal {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Literal::None, Literal::None) => true,
			(Literal::Null, Literal::Null) => true,
			(Literal::UnboundedRange, Literal::UnboundedRange) => true,
			(Literal::Bool(a), Literal::Bool(b)) => a == b,
			(Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
			(Literal::Integer(a), Literal::Integer(b)) => a == b,
			(Literal::Decimal(a), Literal::Decimal(b)) => a == b,
			(Literal::String(a), Literal::String(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			(Literal::Regex(a), Literal::Regex(b)) => a == b,
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Set(a), Literal::Set(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
			// Every variant must appear above. This arm is for mismatched
			// variants only: a variant missing from the list falls through to
			// it and compares unequal to itself, which breaks the `Eq` asserted
			// below and silently defeats every structural comparison of an
			// expression tree containing it.
			_ => false,
		}
	}
}
impl Eq for Literal {}

impl Hash for Literal {
	fn hash<H: Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			Literal::None => {}
			Literal::Null => {}
			Literal::UnboundedRange => {}
			Literal::Bool(x) => x.hash(state),
			Literal::Float(x) => x.to_bits().hash(state),
			Literal::Integer(x) => x.hash(state),
			Literal::Decimal(x) => x.hash(state),
			Literal::String(x) => x.hash(state),
			Literal::Bytes(x) => x.hash(state),
			Literal::Regex(x) => x.hash(state),
			Literal::RecordId(x) => x.hash(state),
			Literal::Array(x) => x.hash(state),
			Literal::Set(x) => x.hash(state),
			Literal::Object(x) => x.hash(state),
			Literal::Duration(x) => x.hash(state),
			Literal::Datetime(x) => x.hash(state),
			Literal::Uuid(x) => x.hash(state),
			Literal::Geometry(x) => x.hash(state),
			Literal::File(x) => x.hash(state),
		}
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let lit: crate::sql::Literal = self.clone().into();
		lit.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ObjectEntry {
	pub key: Strand,
	pub value: Expr,
}

impl ToSql for ObjectEntry {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let entry: crate::sql::literal::ObjectEntry = self.clone().into();
		entry.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod equality_tests {
	use rust_decimal::Decimal;

	use super::Literal;
	use crate::val::Strand;

	/// `Eq` is asserted for `Literal`, so every variant must equal itself.
	///
	/// The impl is hand-written with a `_ => false` fallthrough, so a variant
	/// omitted from it compares unequal to itself rather than failing to
	/// compile. That breaks any structural comparison of an expression
	/// containing it: a change-detector sees a spurious difference, and an
	/// index-guard match sees none.
	#[test]
	fn every_variant_equals_itself() {
		// The exhaustive match is the point: it makes adding a variant a
		// compile error here, so the sample list cannot fall behind the enum
		// the way a hand-maintained subset does.
		fn _forces_this_list_to_be_updated(l: &Literal) {
			match l {
				Literal::None
				| Literal::Null
				| Literal::UnboundedRange
				| Literal::Bool(_)
				| Literal::Float(_)
				| Literal::Integer(_)
				| Literal::Decimal(_)
				| Literal::String(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::RecordId(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Duration(_)
				| Literal::Datetime(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_) => {}
			}
		}
		let samples = [
			Literal::None,
			Literal::Null,
			Literal::UnboundedRange,
			Literal::Bool(true),
			Literal::Float(1.5),
			Literal::Float(-0.0),
			Literal::Integer(7),
			Literal::Decimal(Decimal::new(150, 2)),
			Literal::String(Strand::new("s")),
			Literal::Bytes(crate::val::Bytes::from(vec![1u8])),
			Literal::Regex("a".parse::<crate::val::Regex>().unwrap()),
			Literal::RecordId(crate::expr::RecordIdLit {
				table: "t".into(),
				key: crate::expr::RecordIdKeyLit::Number(1),
			}),
			Literal::Array(vec![]),
			Literal::Set(vec![]),
			Literal::Object(vec![]),
			Literal::Duration(crate::val::Duration::from_secs(1)),
			Literal::Datetime(crate::val::Datetime::MIN_UTC),
			Literal::Uuid(crate::val::Uuid::nil()),
			Literal::Geometry(crate::val::Geometry::Point(geo::Point::new(1.0, 2.0))),
			Literal::File(crate::val::File::new("b".to_owned(), "p".to_owned())),
		];
		for s in samples {
			assert_eq!(s, s.clone(), "{s:?} must equal itself");
		}
	}

	/// `Float` compares by bit pattern, so the two zeroes are distinct — they
	/// render differently (`-0f` vs `0f`), and treating them as equal would let
	/// a rewrite that changes one into the other be dropped as a no-op.
	#[test]
	fn signed_zero_is_not_equal_to_zero() {
		assert_ne!(Literal::Float(-0.0), Literal::Float(0.0));
	}
}

#[cfg(test)]
mod static_value_tests {
	use surrealdb_strand::Strand;

	use super::{Literal, ObjectEntry};
	use crate::expr::record_id::RecordIdKeyGen;
	use crate::expr::{Expr, RecordIdKeyLit, RecordIdLit};
	use crate::val::Value;

	fn entry(key: &str, value: Literal) -> ObjectEntry {
		ObjectEntry {
			key: Strand::new(key),
			value: Expr::Literal(value),
		}
	}

	/// An object's entries come out ordered by key regardless of source order,
	/// because the value has to be indistinguishable from the evaluated one.
	#[test]
	fn object_entries_are_ordered_by_key() {
		let lit =
			Literal::Object(vec![entry("z", Literal::Integer(1)), entry("a", Literal::Integer(2))]);
		let Some(Value::Object(obj)) = lit.as_static_value() else {
			panic!("a literal object of literals is a value");
		};
		let keys: Vec<&str> = obj.iter().map(|(k, _)| k.as_str()).collect();
		assert_eq!(keys, ["a", "z"]);
	}

	/// A repeated key resolves to its last occurrence, matching what inserting
	/// each entry in source order produces.
	#[test]
	fn repeated_object_key_keeps_the_last() {
		let lit =
			Literal::Object(vec![entry("a", Literal::Integer(1)), entry("a", Literal::Integer(3))]);
		let Some(Value::Object(obj)) = lit.as_static_value() else {
			panic!("a literal object of literals is a value");
		};
		assert_eq!(obj.len(), 1);
		assert_eq!(obj.get("a"), Some(&Value::Number(crate::val::Number::Int(3))));
	}

	/// A generated record-id key is allocated during evaluation, so a literal
	/// carrying one is not a value on its own — and neither is any collection
	/// holding it, or the fast path would hand out ids the evaluator never
	/// allocated.
	#[test]
	fn generated_record_id_key_is_not_static() {
		let lit = Literal::RecordId(RecordIdLit {
			table: "t".into(),
			key: RecordIdKeyLit::Generate(RecordIdKeyGen::Ulid),
		});
		assert!(lit.as_static_value().is_none());
		let nested = Literal::Object(vec![entry("id", lit.clone())]);
		assert!(nested.as_static_value().is_none());
		let in_array = Literal::Array(vec![Expr::Literal(lit)]);
		assert!(in_array.as_static_value().is_none());
	}

	/// A collection is a value only if every element is, so one non-literal
	/// element withholds the whole collection from the fast path.
	#[test]
	fn non_literal_element_withholds_the_collection() {
		let lit = Literal::Array(vec![
			Expr::Literal(Literal::Integer(1)),
			Expr::Idiom(crate::expr::Idiom::field("f".to_owned())),
		]);
		assert!(lit.as_static_value().is_none());
	}

	/// Simple record-id keys are values, which is what makes an exported
	/// record's `id` field take the fast path.
	#[test]
	fn simple_record_id_keys_are_static() {
		for key in [
			RecordIdKeyLit::Number(1),
			RecordIdKeyLit::String(Strand::new("a")),
			RecordIdKeyLit::Uuid(crate::val::Uuid::nil()),
		] {
			let lit = Literal::RecordId(RecordIdLit {
				table: "t".into(),
				key,
			});
			assert!(matches!(lit.as_static_value(), Some(Value::RecordId(_))));
		}
	}
}
