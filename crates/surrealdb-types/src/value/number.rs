use std::cmp::Ordering;
use std::hash;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::Kind;

/// Represents a numeric value in SurrealDB
///
/// Numbers in SurrealDB can be integers, floating-point numbers, or decimal numbers.
/// This enum provides type-safe representation for all numeric types.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Number {
	/// A 64-bit signed integer
	Int(i64),
	/// A 64-bit floating-point number
	Float(f64),
	/// A decimal number with arbitrary precision
	Decimal(Decimal),
}

macro_rules! impl_number {
	($($variant:ident($type:ty) => ($is:ident, $into:ident, $from:ident),)+) => {
		impl Number {
			/// Get the kind of number
			pub fn kind(&self) -> Kind {
				match self {
					$(
						Self::$variant(_) => Kind::$variant,
					)+
				}
			}

			$(
				/// Check if this is a of the given type
				pub fn $is(&self) -> bool {
					matches!(self, Self::$variant(_))
				}

				/// Convert this number into the given type
				pub fn $into(self) -> anyhow::Result<$type> {
					if let Self::$variant(v) = self {
						Ok(v)
					} else {
						Err(anyhow::anyhow!("Expected a {} but got a {}", Kind::$variant, self.kind()))
					}
				}

				/// Create a new number from the given type
				pub fn $from(v: $type) -> Self {
					Self::$variant(v)
				}
			)+
		}
	}
}

impl_number! (
	Int(i64) => (is_int, into_int, from_int),
	Float(f64) => (is_float, into_float, from_float),
	Decimal(Decimal) => (is_decimal, into_decimal, from_decimal),
);

impl Default for Number {
	fn default() -> Self {
		Self::Int(0)
	}
}

impl Eq for Number {}

impl Ord for Number {
	fn cmp(&self, other: &Self) -> Ordering {
		fn total_cmp_f64(a: f64, b: f64) -> Ordering {
			if a == 0.0 && b == 0.0 {
				// -0.0 = 0.0
				Ordering::Equal
			} else {
				// Handles NaN's
				a.total_cmp(&b)
			}
		}

		// Pick the greater number depending on whether it's positive.
		macro_rules! greater {
			($f:ident) => {
				if $f.is_sign_positive() {
					Ordering::Greater
				} else {
					Ordering::Less
				}
			};
		}

		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.cmp(w),
			(Number::Float(v), Number::Float(w)) => total_cmp_f64(*v, *w),
			(Number::Decimal(v), Number::Decimal(w)) => v.cmp(w),
			// ------------------------------
			(Number::Int(v), Number::Float(w)) => {
				// If the float is not finite, we don't need to compare it to the integer.
				if !w.is_finite() {
					return greater!(w).reverse();
				}
				// Cast int to i128 to avoid saturating.
				let l = *v as i128;
				// Cast the integer-part of the float to i128 to avoid saturating.
				let r = *w as i128;
				// Compare both integer parts.
				match l.cmp(&r) {
					// If the integer parts are equal then we need to compare the mantissa.
					Ordering::Equal => total_cmp_f64(0.0, w.fract()),
					// If the integer parts are not equal then we already know the correct ordering.
					ordering => ordering,
				}
			}
			(v @ Number::Float(_), w @ Number::Int(_)) => w.cmp(v).reverse(),
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).cmp(w),
			(Number::Decimal(v), Number::Int(w)) => v.cmp(&Decimal::from(*w)),
			// ------------------------------
			(Number::Float(v), Number::Decimal(w)) => {
				// Compare fractional parts of the float and decimal.
				macro_rules! compare_fractions {
					($l:ident, $r:ident) => {
						match ($l == 0.0, $r == Decimal::ZERO) {
							// If both numbers are zero, these are equal.
							(true, true) => {
								return Ordering::Equal;
							}
							// If only the float is zero, check the decimal's sign.
							(true, false) => {
								return greater!($r).reverse();
							}
							// If only the decimal is zero, check the float's sign.
							(false, true) => {
								return greater!($l);
							}
							// If neither is zero, continue checking the rest of the digits.
							(false, false) => {
								continue;
							}
						}
					};
				}
				// If the float is not finite, we don't need to compare it to the decimal
				if !v.is_finite() {
					return greater!(v);
				}
				// Cast int to i128 to avoid saturating.
				let l = *v as i128;
				// Cast the integer-part of the decimal to i128.
				let Ok(r) = i128::try_from(*w) else {
					return greater!(w).reverse();
				};
				// Compare both integer parts.
				match l.cmp(&r) {
					// If the integer parts are equal then we need to compare the fractional parts.
					Ordering::Equal => {
						// We can't compare the fractional parts of floats with decimals reliably.
						// Instead, we need to compare them as integers. To do this, we need to
						// multiply the fraction with a number large enough to move some digits
						// to the integer part of the float or decimal. The number should fit in
						// 52 bits and be able to multiply f64 fractions between -1 and 1 without
						// losing precision. Since we may need to do this repeatedly it helps if
						// the number is as big as possible to reduce the number of
						// iterations needed.
						//
						// This number is roughly 2 ^ 53 with the last digits truncated in order
						// to make sure the fraction converges to 0 every time we multiply it.
						// This is a magic number I found through my experiments so don't ask me
						// the logic behind it :) Before changing this number, please make sure
						// that the relevant tests aren't flaky after changing it.
						const SAFE_MULTIPLIER: i64 = 9_007_199_254_740_000;
						// Get the fractional part of the float.
						let mut l = v.fract();
						// Get the fractional part of the decimal.
						let mut r = w.fract();
						// Move the digits and compare them.
						// This is very generous. For example, for our tests to pass we only need
						// 3 iterations. This should be at least 6 to make sure we cover all
						// possible decimals and floats.
						for _ in 0..12 {
							l *= SAFE_MULTIPLIER as f64;
							r *= Decimal::new(SAFE_MULTIPLIER, 0);
							// Cast the integer part of the decimal to i64. The fractions are always
							// less than 1 so we know this will always be less than SAFE_MULTIPLIER.
							match r.to_i64() {
								Some(ref right) => match (l as i64).cmp(right) {
									// If the integer parts are equal, we need to check the
									// remaining fractional parts.
									Ordering::Equal => {
										// Drop the integer parts we already compared.
										l = l.fract();
										r = r.fract();
										// Compare the fractional parts and decide whether to return
										// or continue checking the next digits.
										compare_fractions!(l, r);
									}
									ordering => {
										// If the integer parts are not equal then we already know
										// the correct ordering.
										return ordering;
									}
								},
								// This is technically unreachable. Reaching this part likely
								// indicates a bug in `rust-decimal`'s `to_f64`'s
								// implementation.
								None => {
									// We will assume the decimal is bigger or smaller depending on
									// its sign.
									return greater!(w).reverse();
								}
							}
						}
						// After our iterations, if we still haven't exhausted both fractions we
						// will just treat them as equal. It should be impossible to reach
						// this point after at least 6 iterations. We could use an infinite
						// loop instead but this way we make sure the loop always exits.
						Ordering::Equal
					}
					// If the integer parts are not equal then we already know the correct ordering.
					ordering => ordering,
				}
			}
			(v @ Number::Decimal(..), w @ Number::Float(..)) => w.cmp(v).reverse(),
		}
	}
}

// Warning: Equal numbers may have different hashes, which violates
// the invariants of certain collections!
impl hash::Hash for Number {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		match self {
			Number::Int(v) => v.hash(state),
			Number::Float(v) => v.to_bits().hash(state),
			Number::Decimal(v) => v.hash(state),
		}
	}
}

impl PartialEq for Number {
	fn eq(&self, other: &Self) -> bool {
		fn total_eq_f64(a: f64, b: f64) -> bool {
			a.to_bits().eq(&b.to_bits()) || (a == 0.0 && b == 0.0)
		}

		match (self, other) {
			(Number::Int(v), Number::Int(w)) => v.eq(w),
			(Number::Float(v), Number::Float(w)) => total_eq_f64(*v, *w),
			(Number::Decimal(v), Number::Decimal(w)) => v.eq(w),
			// ------------------------------
			(v @ Number::Int(_), w @ Number::Float(_)) => v.cmp(w) == Ordering::Equal,
			(v @ Number::Float(_), w @ Number::Int(_)) => v.cmp(w) == Ordering::Equal,
			// ------------------------------
			(Number::Int(v), Number::Decimal(w)) => Decimal::from(*v).eq(w),
			(Number::Decimal(v), Number::Int(w)) => v.eq(&Decimal::from(*w)),
			// ------------------------------
			(v @ Number::Float(_), w @ Number::Decimal(_)) => v.cmp(w) == Ordering::Equal,
			(v @ Number::Decimal(_), w @ Number::Float(_)) => v.cmp(w) == Ordering::Equal,
		}
	}
}

impl PartialOrd for Number {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

// From implementations for common numeric types
impl From<i32> for Number {
	fn from(value: i32) -> Self {
		Number::Int(value as i64)
	}
}

impl From<i64> for Number {
	fn from(value: i64) -> Self {
		Number::Int(value)
	}
}

impl From<f32> for Number {
	fn from(value: f32) -> Self {
		Number::Float(value as f64)
	}
}

impl From<f64> for Number {
	fn from(value: f64) -> Self {
		Number::Float(value)
	}
}

impl From<Decimal> for Number {
	fn from(value: Decimal) -> Self {
		Number::Decimal(value)
	}
}
