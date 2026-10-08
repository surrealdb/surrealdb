//! A value the VM holds in a register without owning heap memory.
//!
//! [`Scalar`] is either a scalar kind held inline — numbers, booleans,
//! durations, datetimes, uuids, and strings or bytes borrowed from the row —
//! or any other value, borrowed or owned, in [`Scalar::Value`]. Comparing and
//! testing scalars answers exactly what the same operation answers on the
//! [`Value`]s they denote: each scalar kind is compared by the type the
//! `Value` variant holds, and kinds are ranked against each other in the
//! declaration order of [`Value`]'s variants, which is what its derived
//! ordering ranks them by.

use std::borrow::Cow;
use std::cmp::Ordering;

use crate::val::{Datetime, Duration, Number, Uuid, Value};

/// A value in a register.
///
/// [`Scalar::of`] holds a value of a scalar kind as that kind. A
/// [`Scalar::Value`] may still hold one, decoded where it could not be
/// borrowed; every comparison reads it through [`Scalar::view`].
#[derive(Debug, Clone)]
pub(crate) enum Scalar<'a> {
	None,
	Null,
	Bool(bool),
	Number(Number),
	Str(&'a str),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Bytes(&'a [u8]),
	/// A value of any other kind: an array, set, object, record id, geometry
	/// and so on.
	Value(Cow<'a, Value>),
}

impl<'a> Scalar<'a> {
	/// The scalar that denotes `value`, borrowing its heap parts.
	pub(crate) fn of(value: &'a Value) -> Self {
		match value {
			Value::None => Scalar::None,
			Value::Null => Scalar::Null,
			Value::Bool(v) => Scalar::Bool(*v),
			Value::Number(v) => Scalar::Number(*v),
			Value::String(v) => Scalar::Str(v.as_str()),
			Value::Duration(v) => Scalar::Duration(*v),
			Value::Datetime(v) => Scalar::Datetime(*v),
			Value::Uuid(v) => Scalar::Uuid(*v),
			Value::Bytes(v) => Scalar::Bytes(v.0.as_ref()),
			other => Scalar::Value(Cow::Borrowed(other)),
		}
	}

	/// The [`Value`] this scalar denotes. Allocates for a string, bytes and a
	/// borrowed composite.
	pub(crate) fn to_value(&self) -> Value {
		match self {
			Scalar::None => Value::None,
			Scalar::Null => Value::Null,
			Scalar::Bool(v) => Value::Bool(*v),
			Scalar::Number(v) => Value::Number(*v),
			Scalar::Str(v) => Value::String((*v).into()),
			Scalar::Duration(v) => Value::Duration(*v),
			Scalar::Datetime(v) => Value::Datetime(*v),
			Scalar::Uuid(v) => Value::Uuid(*v),
			Scalar::Bytes(v) => Value::Bytes(crate::val::Bytes(::bytes::Bytes::copy_from_slice(v))),
			Scalar::Value(v) => v.as_ref().clone(),
		}
	}

	/// The [`Value`] this scalar denotes, borrowed where the scalar holds one.
	/// Allocates only for a string and bytes.
	pub(crate) fn as_value(&self) -> Cow<'_, Value> {
		match self {
			Scalar::Value(v) => Cow::Borrowed(v.as_ref()),
			other => Cow::Owned(other.to_value()),
		}
	}

	/// The same value, with a scalar kind held in [`Scalar::Value`] held as
	/// its kind.
	fn view(&self) -> Scalar<'_> {
		match self {
			Scalar::Value(v) => Scalar::of(v.as_ref()),
			Scalar::None => Scalar::None,
			Scalar::Null => Scalar::Null,
			Scalar::Bool(v) => Scalar::Bool(*v),
			Scalar::Number(v) => Scalar::Number(*v),
			Scalar::Str(v) => Scalar::Str(v),
			Scalar::Duration(v) => Scalar::Duration(*v),
			Scalar::Datetime(v) => Scalar::Datetime(*v),
			Scalar::Uuid(v) => Scalar::Uuid(*v),
			Scalar::Bytes(v) => Scalar::Bytes(v),
		}
	}

	/// Whether this is an array or a set, which `*=` and `?=` compare element
	/// by element.
	pub(crate) fn is_collection(&self) -> bool {
		matches!(self.view(), Scalar::Value(v) if matches!(v.as_ref(), Value::Array(_) | Value::Set(_)))
	}

	/// Whether this is `NONE`.
	pub(crate) fn is_none(&self) -> bool {
		matches!(self.view(), Scalar::None)
	}

	/// [`Value::is_truthy`].
	pub(crate) fn is_truthy(&self) -> bool {
		match &self.view() {
			Scalar::None | Scalar::Null => false,
			Scalar::Bool(v) => *v,
			Scalar::Number(v) => v.is_truthy(),
			Scalar::Str(v) => !v.is_empty(),
			Scalar::Duration(v) => v.as_nanos() > 0,
			Scalar::Datetime(_) | Scalar::Uuid(_) => true,
			Scalar::Bytes(v) => !v.is_empty(),
			Scalar::Value(v) => v.is_truthy(),
		}
	}

	/// [`Value::equal`], the `=` operator.
	pub(crate) fn equal(&self, other: &Scalar<'_>) -> bool {
		let (this, other) = (self.view(), other.view());
		match (&this, &other) {
			(Scalar::Value(a), Scalar::Value(b)) => a.equal(b),
			// A string equals a regex that matches it, either way round.
			(Scalar::Str(s), Scalar::Value(v)) | (Scalar::Value(v), Scalar::Str(s)) => {
				matches!(v.as_ref(), Value::Regex(r) if r.inner().is_match(s))
			}
			_ => this.same_kind_eq(&other),
		}
	}

	/// The derived [`PartialEq`] of [`Value`], the `==` operator.
	pub(crate) fn exact(&self, other: &Scalar<'_>) -> bool {
		let (this, other) = (self.view(), other.view());
		match (&this, &other) {
			(Scalar::Value(a), Scalar::Value(b)) => a == b,
			_ => this.same_kind_eq(&other),
		}
	}

	/// Equality of two values of the same scalar kind; values of different
	/// kinds are unequal.
	fn same_kind_eq(&self, other: &Scalar<'_>) -> bool {
		match (self, other) {
			(Scalar::None, Scalar::None) | (Scalar::Null, Scalar::Null) => true,
			(Scalar::Bool(a), Scalar::Bool(b)) => a == b,
			(Scalar::Number(a), Scalar::Number(b)) => a == b,
			(Scalar::Str(a), Scalar::Str(b)) => a == b,
			(Scalar::Duration(a), Scalar::Duration(b)) => a == b,
			(Scalar::Datetime(a), Scalar::Datetime(b)) => a == b,
			(Scalar::Uuid(a), Scalar::Uuid(b)) => a == b,
			(Scalar::Bytes(a), Scalar::Bytes(b)) => a == b,
			_ => false,
		}
	}

	/// The derived [`PartialOrd`] of [`Value`], which `<`, `<=`, `>` and `>=`
	/// answer from.
	pub(crate) fn partial_cmp(&self, other: &Scalar<'_>) -> Option<Ordering> {
		let (this, other) = (self.view(), other.view());
		match (&this, &other) {
			(Scalar::Value(a), Scalar::Value(b)) => a.as_ref().partial_cmp(b.as_ref()),
			(Scalar::None, Scalar::None) | (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
			(Scalar::Bool(a), Scalar::Bool(b)) => a.partial_cmp(b),
			(Scalar::Number(a), Scalar::Number(b)) => a.partial_cmp(b),
			(Scalar::Str(a), Scalar::Str(b)) => a.partial_cmp(b),
			(Scalar::Duration(a), Scalar::Duration(b)) => a.partial_cmp(b),
			(Scalar::Datetime(a), Scalar::Datetime(b)) => a.partial_cmp(b),
			(Scalar::Uuid(a), Scalar::Uuid(b)) => a.partial_cmp(b),
			(Scalar::Bytes(a), Scalar::Bytes(b)) => a.partial_cmp(b),
			_ => this.rank().partial_cmp(&other.rank()),
		}
	}

	/// The position of this value's kind among [`Value`]'s variants.
	fn rank(&self) -> u8 {
		match self {
			Scalar::None => 0,
			Scalar::Null => 1,
			Scalar::Bool(_) => 2,
			Scalar::Number(_) => 3,
			Scalar::Str(_) => 4,
			Scalar::Duration(_) => 5,
			Scalar::Datetime(_) => 6,
			Scalar::Uuid(_) => 7,
			Scalar::Bytes(_) => 12,
			Scalar::Value(v) => value_rank(v),
		}
	}
}

/// The position of `value`'s variant in the declaration of [`Value`].
fn value_rank(value: &Value) -> u8 {
	match value {
		Value::None => 0,
		Value::Null => 1,
		Value::Bool(_) => 2,
		Value::Number(_) => 3,
		Value::String(_) => 4,
		Value::Duration(_) => 5,
		Value::Datetime(_) => 6,
		Value::Uuid(_) => 7,
		Value::Array(_) => 8,
		Value::Set(_) => 9,
		Value::Object(_) => 10,
		Value::Geometry(_) => 11,
		Value::Bytes(_) => 12,
		Value::Table(_) => 13,
		Value::RecordId(_) => 14,
		Value::File(_) => 15,
		Value::Regex(_) => 16,
		Value::Range(_) => 17,
		Value::Closure(_) => 18,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::val;
	use crate::fnc::operate;

	/// Values of every kind, with the edges each kind's comparison has: the
	/// three number representations of one value, signed zeros and NaN, empty
	/// and non-empty strings and bytes, and a regex a string matches.
	async fn samples() -> Vec<Value> {
		let mut out = Vec::new();
		for src in [
			"NONE",
			"NULL",
			"true",
			"false",
			"0",
			"1",
			"-1",
			"1f",
			"1.0f",
			"1dec",
			"1.5f",
			"0f",
			"-0f",
			"9223372036854775807",
			"'1'",
			"''",
			"'a'",
			"'ab'",
			"'b'",
			"'é'",
			"1s",
			"0ns",
			"2s",
			"d'2024-01-01T00:00:00Z'",
			"d'2025-01-01T00:00:00Z'",
			"u'018f9b8e-0000-7000-8000-000000000000'",
			"u'018f9b8e-0000-7000-8000-000000000001'",
			"<bytes> ''",
			"<bytes> 'ab'",
			"[]",
			"[1]",
			"{}",
			"{ a: 1 }",
			"t:1",
			"/a.*/",
			"(1, 2)",
		] {
			out.push(val(src).await);
		}
		out.push(Value::Number(Number::Float(f64::NAN)));
		out
	}

	#[tokio::test]
	async fn scalars_compare_as_the_values_they_denote() {
		let values = samples().await;
		// Every scalar kind, and the composites a scalar is compared against,
		// is among the samples.
		let kinds: std::collections::HashSet<u8> = values.iter().map(value_rank).collect();
		for rank in [0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 14, 16] {
			assert!(kinds.contains(&rank), "no sample of the kind ranked {rank}");
		}
		for a in &values {
			let sa = Scalar::of(a);
			assert_eq!(sa.is_truthy(), a.is_truthy(), "truthiness of {a:?}");
			assert_eq!(sa.to_value(), *a, "round trip of {a:?}");
			for b in &values {
				// A value decoded whole compares as the value borrowed as its kind.
				let held = Scalar::Value(Cow::Borrowed(b));
				assert_eq!(sa.equal(&held), sa.equal(&Scalar::of(b)), "{a:?} = held {b:?}");
				assert_eq!(
					sa.partial_cmp(&held),
					sa.partial_cmp(&Scalar::of(b)),
					"{a:?} cmp held {b:?}"
				);
				let sb = Scalar::of(b);
				let expected = |r: anyhow::Result<Value>| r.expect("operator answers").is_truthy();
				let cases = [
					("=", sa.equal(&sb), expected(operate::equal(a, b))),
					("!=", !sa.equal(&sb), expected(operate::not_equal(a, b))),
					("==", sa.exact(&sb), expected(operate::exact(a, b))),
					(
						"<",
						sa.partial_cmp(&sb) == Some(Ordering::Less),
						expected(operate::less_than(a, b)),
					),
					(
						"<=",
						matches!(sa.partial_cmp(&sb), Some(Ordering::Less | Ordering::Equal)),
						expected(operate::less_than_or_equal(a, b)),
					),
					(
						">",
						sa.partial_cmp(&sb) == Some(Ordering::Greater),
						expected(operate::more_than(a, b)),
					),
					(
						">=",
						matches!(sa.partial_cmp(&sb), Some(Ordering::Greater | Ordering::Equal)),
						expected(operate::more_than_or_equal(a, b)),
					),
				];
				for (op, got, want) in cases {
					assert_eq!(got, want, "{a:?} {op} {b:?}");
				}
			}
		}
	}

	/// The borrowed forms hold no heap memory of their own.
	#[tokio::test]
	async fn a_scalar_borrows_its_string_and_bytes() {
		let s = val("'hello'").await;
		let Scalar::Str(borrowed) = Scalar::of(&s) else {
			panic!("a string is a Str");
		};
		let Value::String(owned) = &s else {
			unreachable!()
		};
		assert!(std::ptr::eq(borrowed.as_ptr(), owned.as_str().as_ptr()));
	}
}
