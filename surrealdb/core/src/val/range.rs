use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};

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

/// A range of a specific type, can be converted back into a general range and
/// coerced from a general range.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TypedRange<T> {
	pub start: Bound<T>,
	pub end: Bound<T>,
}

impl<T: PartialOrd> PartialOrd for TypedRange<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		fn compare_bounds<T: PartialOrd>(a: &Bound<T>, b: &Bound<T>) -> Option<Ordering> {
			match a {
				Bound::Unbounded => match b {
					Bound::Unbounded => Some(Ordering::Equal),
					_ => Some(Ordering::Less),
				},
				Bound::Included(a) => match b {
					Bound::Unbounded => Some(Ordering::Greater),
					Bound::Included(b) => a.partial_cmp(b),
					Bound::Excluded(_) => Some(Ordering::Less),
				},
				Bound::Excluded(a) => match b {
					Bound::Excluded(b) => a.partial_cmp(b),
					_ => Some(Ordering::Greater),
				},
			}
		}

		match compare_bounds(&self.start, &other.start) {
			Some(Ordering::Equal) => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl<T: Clone> TypedRange<T> {
	pub fn from_range<R: RangeBounds<T>>(r: R) -> Self {
		TypedRange {
			start: r.start_bound().map(|x| x.clone()),
			end: r.end_bound().map(|x| x.clone()),
		}
	}
}

impl TypedRange<i64> {
	/// Returns an iterator over this range.
	pub fn iter(self) -> IntegerRangeIter {
		let cur = match self.start {
			Bound::Included(x) => x,
			Bound::Excluded(x) => match x.checked_add(1) {
				Some(x) => x,
				// i64::MAX is excluded so the iterator will never return anything.
				None => {
					return IntegerRangeIter {
						cur: i64::MAX,
						end: Some(i64::MIN),
					};
				}
			},
			Bound::Unbounded => i64::MIN,
		};

		match self.end {
			Bound::Included(x) => IntegerRangeIter {
				cur,
				end: x.checked_add(1),
			},
			Bound::Excluded(x) => IntegerRangeIter {
				cur,
				end: Some(x),
			},
			Bound::Unbounded => IntegerRangeIter {
				cur,
				end: None,
			},
		}
	}

	pub fn slice<'a, T>(&self, s: &'a [T]) -> Option<&'a [T]> {
		let r = match self.end {
			Bound::Included(x) => s.get(..=(x as usize))?,
			Bound::Excluded(x) => s.get(..(x as usize))?,
			Bound::Unbounded => s,
		};
		match self.start {
			Bound::Included(x) => r.get((x as usize)..),
			Bound::Excluded(x) => {
				let x = (x as usize).checked_add(1)?;
				r.get(x..)
			}
			Bound::Unbounded => Some(r),
		}
	}

	pub fn slice_mut<'a, T>(&self, s: &'a mut [T]) -> Option<&'a mut [T]> {
		let r = match self.end {
			Bound::Included(x) => s.get_mut(..=(x as usize))?,
			Bound::Excluded(x) => s.get_mut(..(x as usize))?,
			Bound::Unbounded => s,
		};
		match self.start {
			Bound::Included(x) => r.get_mut((x as usize)..),
			Bound::Excluded(x) => {
				let x = (x as usize).checked_add(1)?;
				r.get_mut(x..)
			}
			Bound::Unbounded => Some(r),
		}
	}

	// TODO: Change this to return an option.
	#[allow(clippy::len_without_is_empty)]
	pub fn len(&self) -> usize {
		let end = match self.end {
			Bound::Unbounded => i64::MAX,
			Bound::Included(x) => x,
			Bound::Excluded(x) => match x.checked_sub(1) {
				Some(x) => x,
				None => return 0,
			},
		};

		let start = match self.start {
			Bound::Unbounded => i64::MIN,
			Bound::Included(x) => x,
			Bound::Excluded(x) => match x.checked_add(1) {
				Some(x) => x,
				None => return 0,
			},
		};

		if start > end {
			return 0;
		}

		usize::try_from(start.abs_diff(end)).unwrap_or(usize::MAX)
	}

	pub(crate) fn cast_to_array(self) -> Array {
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

/// Iterator over `TypedRange<i64>`.
pub struct IntegerRangeIter {
	cur: i64,
	// Signifies the end of the iterator.
	// The iterator will stop returning if self.cur >= self.end
	// If end is None then i64::MAX is included.
	end: Option<i64>,
}

impl Iterator for IntegerRangeIter {
	type Item = i64;

	fn next(&mut self) -> Option<i64> {
		let cur = self.cur;
		if let Some(end) = self.end
			&& cur >= end
		{
			return None;
		}
		if let Some(x) = cur.checked_add(1) {
			self.cur = x
		} else {
			// we have reached i64::MAX so after this we need to avoid returning anything.
			self.end = Some(i64::MIN)
		}

		Some(cur)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let len = if let Some(x) = self.end {
			if self.cur >= x {
				return (0, Some(0));
			}
			self.cur.abs_diff(x) - 1
		} else {
			self.cur.abs_diff(i64::MAX)
		};
		// handling if u64::MAX > usize::MAX
		let upper: Option<usize> = len.try_into().ok();
		(upper.unwrap_or(usize::MAX), upper)
	}
}

#[cfg(test)]
mod test {
	use super::Range;
	use crate::sql::expression::convert_public_value_to_internal;
	use crate::syn;
	use crate::val::Value;

	fn r(r: &str) -> Range {
		let Value::Range(r) = convert_public_value_to_internal(syn::value(r).unwrap()) else {
			panic!()
		};
		*r
	}

	fn round_trip(r: Range) {
		let enc = storekey::encode_vec(&r).unwrap();
		let dec = storekey::decode_borrow(&enc).unwrap();
		assert_eq!(r, dec)
	}

	fn ensure_order(a: Range, b: Range) {
		let a_enc = storekey::encode_vec(&a).unwrap();
		let b_enc = storekey::encode_vec(&b).unwrap();

		assert_eq!(
			a.cmp(&b),
			a_enc.cmp(&b_enc),
			"ordering of {a:?} {b:?} is not correct after encoding"
		);
	}

	#[test]
	fn encode_decode() {
		round_trip(r("1..2"));
		round_trip(r(".."));
		round_trip(r("1>.."));
		round_trip(r("1>..=3"));
		round_trip(r("..3"));
		round_trip(r("'a'..'b'"));
	}

	#[test]
	fn encoding_ordering() {
		ensure_order(r(".."), r(".."));
		ensure_order(r(".."), r("1.."));
		ensure_order(r("1.."), r("1>.."));
		ensure_order(r(".."), r("..1"));
		ensure_order(r(".."), r("..=1"));
		ensure_order(r("1.."), r("2.."));
		ensure_order(r("'a'.."), r("'b'.."));
	}
}
