use std::cmp::Ordering;
use std::ops::Bound;

pub(crate) use common::range::{IntegerRangeIter, TypedRange};
use revision::revisioned;
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::value::CoerceErrorExt;
use crate::expr;
use crate::expr::kind::HasKind;
use crate::val::value::{Coerce, CoerceError};
use crate::val::{Array, IndexFormat, Number, Value};

/// A range of surrealql values,
///
/// Can be any kind of values, "a"..1 is allowed.
#[revisioned(revision = 1)]
#[derive(Debug, Eq, PartialEq, Clone, Hash, Encode, BorrowDecode)]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) struct Range {
	pub start: Bound<Value>,
	pub end: Bound<Value>,
}

impl Range {
	/// returns a range with no bounds.
	pub const fn unbounded() -> Self {
		Range {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}

	/// Returns if the range cannot contain a value.
	pub fn is_empty(&self) -> bool {
		match &self.start {
			Bound::Included(a) => match &self.end {
				Bound::Included(b) => a > b,
				Bound::Excluded(b) => a >= b,
				Bound::Unbounded => false,
			},
			Bound::Excluded(a) => match &self.end {
				Bound::Included(b) | Bound::Excluded(b) => a >= b,
				Bound::Unbounded => false,
			},
			Bound::Unbounded => false,
		}
	}

	/// Returns the intersection of two ranges.
	pub fn intersect(self, other: Self) -> Self {
		let start = match self.start {
			Bound::Included(a) => match other.start {
				Bound::Included(b) => Bound::Included(a.max(b)),
				Bound::Excluded(b) => {
					if a <= b {
						Bound::Excluded(b)
					} else {
						Bound::Included(a)
					}
				}
				Bound::Unbounded => Bound::Included(a),
			},
			Bound::Excluded(a) => match other.start {
				Bound::Excluded(b) => Bound::Excluded(a.max(b)),
				Bound::Included(b) => {
					if a < b {
						Bound::Included(b)
					} else {
						Bound::Excluded(a)
					}
				}
				Bound::Unbounded => Bound::Excluded(a),
			},
			Bound::Unbounded => other.start,
		};

		let end = match self.end {
			Bound::Included(a) => match other.end {
				Bound::Included(b) => Bound::Included(a.min(b)),
				Bound::Excluded(b) => {
					if a >= b {
						Bound::Excluded(b)
					} else {
						Bound::Included(a)
					}
				}
				Bound::Unbounded => Bound::Included(a),
			},
			Bound::Excluded(a) => match other.end {
				Bound::Excluded(b) => Bound::Excluded(a.min(b)),
				Bound::Included(b) => {
					if a > b {
						Bound::Included(b)
					} else {
						Bound::Excluded(a)
					}
				}
				Bound::Unbounded => Bound::Excluded(a),
			},
			Bound::Unbounded => other.end,
		};

		Range {
			start,
			end,
		}
	}
}

impl PartialOrd for Range {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Range {
	fn cmp(&self, other: &Self) -> Ordering {
		fn compare_bounds(a: &Bound<Value>, b: &Bound<Value>) -> Ordering {
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
		match compare_bounds(&self.start, &other.start) {
			Ordering::Equal => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl ToSql for Range {
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

impl Range {
	pub fn can_coerce_to_typed<T: Coerce>(&self) -> bool {
		match self.start {
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				if !x.can_coerce_to::<T>() {
					return false;
				}
			}
			Bound::Unbounded => {}
		}

		match self.end {
			Bound::Included(ref x) | Bound::Excluded(ref x) => x.can_coerce_to::<T>(),
			Bound::Unbounded => true,
		}
	}

	pub fn coerce_to_typed<T: Coerce + HasKind>(self) -> Result<TypedRange<T>, CoerceError> {
		let start = match self.start {
			Bound::Included(x) => Bound::Included(
				T::coerce(x).with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Excluded(x) => Bound::Excluded(
				T::coerce(x).with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match self.end {
			Bound::Included(x) => Bound::Included(
				T::coerce(x).with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Excluded(x) => Bound::Excluded(
				T::coerce(x).with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Unbounded => Bound::Unbounded,
		};
		Ok(TypedRange {
			start,
			end,
		})
	}

	pub(crate) fn into_literal(self) -> expr::Expr {
		match (self.start, self.end) {
			(Bound::Unbounded, Bound::Unbounded) => {
				expr::Expr::Literal(expr::Literal::UnboundedRange)
			}
			(Bound::Included(x), Bound::Unbounded) => expr::Expr::Postfix {
				op: expr::PostfixOperator::Range,
				expr: Box::new(x.into_literal()),
			},
			(Bound::Excluded(x), Bound::Unbounded) => expr::Expr::Postfix {
				op: expr::PostfixOperator::RangeSkip,
				expr: Box::new(x.into_literal()),
			},

			(Bound::Unbounded, Bound::Included(y)) => expr::Expr::Prefix {
				op: expr::PrefixOperator::RangeInclusive,
				expr: Box::new(y.into_literal()),
			},
			(Bound::Included(x), Bound::Included(y)) => expr::Expr::Binary {
				left: Box::new(x.into_literal()),
				op: expr::BinaryOperator::RangeInclusive,
				right: Box::new(y.into_literal()),
			},
			(Bound::Excluded(x), Bound::Included(y)) => expr::Expr::Binary {
				left: Box::new(x.into_literal()),
				op: expr::BinaryOperator::RangeSkipInclusive,
				right: Box::new(y.into_literal()),
			},
			(Bound::Unbounded, Bound::Excluded(y)) => expr::Expr::Prefix {
				op: expr::PrefixOperator::Range,
				expr: Box::new(y.into_literal()),
			},
			(Bound::Included(x), Bound::Excluded(y)) => expr::Expr::Binary {
				left: Box::new(x.into_literal()),
				op: expr::BinaryOperator::Range,
				right: Box::new(y.into_literal()),
			},
			(Bound::Excluded(x), Bound::Excluded(y)) => expr::Expr::Binary {
				left: Box::new(x.into_literal()),
				op: expr::BinaryOperator::RangeSkip,
				right: Box::new(y.into_literal()),
			},
		}
	}
}

/// `TypedRange<i64>` lives in `surrealdb-common`, below the value layer, so the
/// conversion into an [`Array`] cannot be an inherent method on it.
pub(crate) trait IntegerRangeExt {
	fn cast_to_array(self) -> Array;
}

impl IntegerRangeExt for TypedRange<i64> {
	fn cast_to_array(self) -> Array {
		let iter = self.iter();
		Array(iter.map(|i| Value::Number(Number::Int(i))).collect())
	}
}

impl<T> From<TypedRange<T>> for Range
where
	Value: From<T>,
{
	fn from(value: TypedRange<T>) -> Self {
		Range {
			start: value.start.map(From::from),
			end: value.end.map(From::from),
		}
	}
}

#[cfg(test)]
mod test {
	use super::Range;
	use crate::syn;
	use crate::val::Value;
	use crate::val::convert_public::convert_public_value_to_internal;

	fn r(r: &str) -> Range {
		let Value::Range(r) = convert_public_value_to_internal(syn::value(r).unwrap()) else {
			panic!()
		};
		*r
	}

	fn round_trip(r: &Range) {
		let enc = storekey::encode_vec(r).unwrap();
		let dec = storekey::decode_borrow(&enc).unwrap();
		assert_eq!(r, &dec)
	}

	fn ensure_order(a: &Range, b: &Range) {
		let a_enc = storekey::encode_vec(a).unwrap();
		let b_enc = storekey::encode_vec(b).unwrap();

		assert_eq!(
			a.cmp(b),
			a_enc.cmp(&b_enc),
			"ordering of {a:?} {b:?} is not correct after encoding"
		);
	}

	#[test]
	fn encode_decode() {
		round_trip(&r("1..2"));
		round_trip(&r(".."));
		round_trip(&r("1>.."));
		round_trip(&r("1>..=3"));
		round_trip(&r("..3"));
		round_trip(&r("'a'..'b'"));
	}

	#[test]
	fn encoding_ordering() {
		ensure_order(&r(".."), &r(".."));
		ensure_order(&r(".."), &r("1.."));
		ensure_order(&r("1.."), &r("1>.."));
		ensure_order(&r(".."), &r("..1"));
		ensure_order(&r(".."), &r("..=1"));
		ensure_order(&r("1.."), &r("2.."));
		ensure_order(&r("'a'.."), &r("'b'.."));
	}
}
