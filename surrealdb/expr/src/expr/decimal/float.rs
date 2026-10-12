//! Conversions between `f64` and the key encoding's decimal form.
//!
//! [`from_f64`] produces the coefficient that is persisted in index keys, so
//! it must be reproduced exactly, not merely closely. It is the result of a
//! specific decimal algorithm rather than the correctly rounded expansion:
//!
//! - A float `m · 2^b` becomes `m · 2^b` (multiplication) or `m / 2^-b` (long division) in the
//!   guarded arithmetic of `arith.rs`, rounded half-up to as many digits as fit in a `u128`.
//! - For `|b| >= 128` the power `2^|b|` does not fit the coefficient. It is truncated to 38 or 39
//!   digits plus its guard, and the product or quotient is then corrected for the dropped guard by
//!   further guarded arithmetic. The corrected value can differ from the exact one in its guard
//!   digits, which occasionally flips the final rounding relative to the exactly rounded result.
//!
//! [`to_f64`] is the conversion back that decoding uses when a key does not
//! parse as a `Decimal`. It is correctly rounded except where its 19-digit
//! fast paths cannot decide, in which case it scales the guarded decimal by
//! powers of two until the mantissa can be read off; that fallback is not
//! always correctly rounded, and decoded values depend on it.
//!
//! Every step mirrors those algorithms operation for operation. Do not
//! simplify any of it towards the mathematically exact answer.

use std::cmp::Ordering;
use std::sync::OnceLock;

use super::arith::{
	D, DIGIT_POWER_10, GUARD_CARRY, POW10_U64, decimal_digits, div, mul, pow10,
	remaining_decimal_digits, rescale, round,
};

/// Converts a finite, non-zero `f64` into `(negative, coefficient, exponent)`
/// such that the value is `±coefficient · 10^exponent`.
pub(super) fn from_f64(x: f64) -> (bool, u128, i32) {
	debug_assert!(x.is_finite() && x != 0.0);
	let bits = x.to_bits();
	let neg = bits >> 63 != 0;
	let frac = bits & 0x000F_FFFF_FFFF_FFFF;
	let exp = (bits >> 52) & 0x7FF;
	let d = if exp == 0 {
		// Subnormal: the coefficient is used as stored, without reduction.
		f2dec(frac, -1074, neg)
	} else {
		let frac = frac | 0x0010_0000_0000_0000;
		let pow = exp as i32 - 1075;
		match pow.cmp(&0) {
			Ordering::Equal => D::new(frac as u128, 0, neg),
			Ordering::Less => {
				let tz = frac.trailing_zeros().min((-pow) as u32);
				f2dec(frac >> tz, pow + tz as i32, neg)
			}
			Ordering::Greater => f2dec(frac, pow, neg),
		}
	};
	(neg, d.digits, d.exponent())
}

fn f2dec(mant: u64, b_exp: i32, neg: bool) -> D {
	let d = D::new(mant as u128, 0, neg);
	match b_exp.cmp(&0) {
		Ordering::Equal => d,
		Ordering::Less => round(div(d, power_of_two(-b_exp as u32))),
		Ordering::Greater => round(mul(d, power_of_two(b_exp as u32))),
	}
}

// ---------------------------------------------------------------------------
// Powers of two
// ---------------------------------------------------------------------------

/// `2^b` as a decimal operand: exact for `b < 128`, and truncated to the
/// coefficient (with its guard digits) above that.
fn power_of_two(b: u32) -> D {
	debug_assert!(b <= 1074);
	if b < 128 {
		return D::new(1u128 << b, 0, false);
	}
	let (digits, power, guard) = wide_powers_of_two()[(b - 128) as usize];
	D {
		digits,
		scale: -(power as i16),
		neg: false,
		guard,
	}
}

/// Truncated `2^b` for `b ∈ [128, 1074]`, as `(coefficient, power, guard)`
/// with `2^b ≈ (coefficient + guard / 10^7) · 10^power`.
///
/// The coefficient keeps 39 digits, or 38 when 39 would not fit a `u128`; the
/// guard holds the following 7 digits, truncated.
fn wide_powers_of_two() -> &'static [(u128, u16, u32)] {
	static TABLE: OnceLock<Vec<(u128, u16, u32)>> = OnceLock::new();
	TABLE.get_or_init(|| (128..=1074).map(truncated_power_of_two).collect())
}

fn truncated_power_of_two(b: u32) -> (u128, u16, u32) {
	// Little-endian limbs of 2^b · 10^7, so that dividing by 10^power leaves
	// the coefficient followed by its seven guard digits.
	let mut n = vec![0u64; (b as usize + 24) / 64 + 1];
	n[b as usize / 64] = 1u64 << (b % 64);
	let mut carry = 0u128;
	for limb in n.iter_mut() {
		let v = *limb as u128 * GUARD_CARRY as u128 + carry;
		*limb = v as u64;
		carry = v >> 64;
	}
	debug_assert_eq!(carry, 0);
	// Decimal digit count of 2^b, from floor(b · log10 2); exact for b <= 1650.
	let digit_count = ((b as u64 * 78913) >> 18) as u32 + 1;
	let mut power = digit_count - 39;
	let (mut coefficient, mut guard) = split_guard(&div_pow10_big(&n, power));
	if coefficient.1 != 0 {
		power += 1;
		(coefficient, guard) = split_guard(&div_pow10_big(&n, power));
	}
	debug_assert_eq!(coefficient.1, 0);
	(coefficient.0, power as u16, guard)
}

/// Splits a value of at most 256 bits into `(value / 10^7, value % 10^7)`,
/// the quotient as `(low, high)` 128-bit halves.
fn split_guard(q: &[u64]) -> ((u128, u128), u32) {
	debug_assert!(q.iter().skip(4).all(|&x| x == 0));
	let w = |i: usize| q.get(i).copied().unwrap_or(0);
	let mut out = [0u64; 4];
	let mut rem: u128 = 0;
	for i in (0..4).rev() {
		let cur = (rem << 64) | w(i) as u128;
		out[i] = (cur / GUARD_CARRY as u128) as u64;
		rem = cur % GUARD_CARRY as u128;
	}
	(
		(out[0] as u128 | ((out[1] as u128) << 64), out[2] as u128 | ((out[3] as u128) << 64)),
		rem as u32,
	)
}

/// `floor(n / 10^power)` for a little-endian limb vector.
fn div_pow10_big(n: &[u64], mut power: u32) -> Vec<u64> {
	let mut q = n.to_vec();
	while power > 0 {
		let p = power.min(DIGIT_POWER_10);
		let d = POW10_U64[p as usize] as u128;
		let mut rem = 0u128;
		for limb in q.iter_mut().rev() {
			let cur = (rem << 64) | *limb as u128;
			*limb = (cur / d) as u64;
			rem = cur % d;
		}
		power -= p;
	}
	while q.len() > 1 && q.last() == Some(&0) {
		q.pop();
	}
	q
}

// ---------------------------------------------------------------------------
// Decimal to binary
// ---------------------------------------------------------------------------

/// Smallest decimal exponent of a 64-bit coefficient that can round to a
/// non-zero `f64`.
const MIN_10_EXP_REAL: i32 = -342;
/// Biased exponent of infinity.
const INFINITE_POWER: i32 = 0x7FF;
/// Smallest unbiased exponent of a normal `f64`.
const EXP_MIN: i32 = -1022;
/// Explicit mantissa bits of an `f64`.
const SIG_BITS: u32 = 52;
/// Largest power of two that fits the coefficient.
const MAX_POWER_OF_TWO: u32 = 127;
/// Largest power of five that fits the coefficient.
const MAX_POWER_OF_FIVE: u32 = 55;

/// Converts `±coefficient · 10^exponent` to an `f64`.
pub(super) fn to_f64(negative: bool, coefficient: u128, exponent: i32) -> f64 {
	let v = to_f64_abs(coefficient, exponent);
	if negative {
		-v
	} else {
		v
	}
}

fn to_f64_abs(coefficient: u128, exponent: i32) -> f64 {
	if coefficient == 0 {
		return 0.0;
	}
	if let Ok(w) = u64::try_from(coefficient) {
		return u64_to_f64(w, exponent);
	}
	// Keep the leading 19-20 digits; the rest only matter if they straddle a
	// rounding boundary.
	let mut w = coefficient;
	let mut exp = exponent;
	let mut inexact = false;
	while w > u64::MAX as u128 {
		let p = (decimal_digits(w) - DIGIT_POWER_10).min(DIGIT_POWER_10);
		let d = POW10_U64[p as usize] as u128;
		inexact |= !w.is_multiple_of(d);
		w /= d;
		exp += p as i32;
	}
	let w = w as u64;
	if exp < MIN_10_EXP_REAL {
		return 0.0;
	} else if exp > f64::MAX_10_EXP {
		return f64::INFINITY;
	}
	if !inexact {
		return u64_to_f64(w, exp);
	}
	if w < u64::MAX {
		let lo = nearest(w, exp);
		if lo == nearest(w + 1, exp) {
			return lo;
		}
	}
	slow_to_f64(D::new(coefficient, (-exponent) as i16, false))
}

fn u64_to_f64(w: u64, exp: i32) -> f64 {
	if exp < MIN_10_EXP_REAL {
		0.0
	} else if exp > f64::MAX_10_EXP {
		f64::INFINITY
	} else {
		nearest(w, exp)
	}
}

/// The correctly rounded `f64` nearest to `w · 10^exp`.
fn nearest(w: u64, exp: i32) -> f64 {
	use std::io::Write as _;
	// u64 digits, 'e', and an i32 exponent fit comfortably.
	let mut buf = [0u8; 40];
	let mut cursor = &mut buf[..];
	// Writing to a slice of sufficient size cannot fail.
	let _ = write!(cursor, "{w}e{exp}");
	let len = 40 - cursor.len();
	std::str::from_utf8(&buf[..len]).ok().and_then(|s| s.parse().ok()).unwrap_or(f64::NAN)
}

/// Scales the decimal by powers of two into `[1/2, 1)`, then multiplies by
/// `2^53` and rounds half-even on the guard to read off the mantissa.
fn slow_to_f64(mut d: D) -> f64 {
	let mut exp2 = 0_i32;
	loop {
		let point = decimal_point(&d);
		if point > INFINITE_POWER {
			return f64::INFINITY;
		} else if point < -INFINITE_POWER {
			return 0.0;
		}
		if point <= 0 {
			break;
		}
		div_power_of_two(&mut d, point as u32);
		exp2 += point;
	}
	while lt_half(&d) {
		let point = decimal_point(&d);
		if point > INFINITE_POWER {
			return f64::INFINITY;
		} else if point < -INFINITE_POWER {
			return 0.0;
		}
		let shift = if point == 0 {
			1
		} else {
			(-point) as u32
		};
		mul_power_of_two(&mut d, shift);
		exp2 -= shift as i32;
	}
	// [1/2, 1) to the [1, 2) of an f64 mantissa.
	exp2 -= 1;
	while EXP_MIN > exp2 {
		div_power_of_two(&mut d, 1);
		exp2 += 1;
	}
	if exp2 - EXP_MIN + 1 >= INFINITE_POWER {
		return f64::INFINITY;
	}
	mul_power_of_two(&mut d, SIG_BITS + 1);
	let mut mantissa = round_to_u64(d);
	// Rounding may have carried into a further bit.
	if mantissa >> (SIG_BITS + 1) != 0 {
		mantissa >>= 1;
		exp2 += 1;
		if exp2 - EXP_MIN + 1 >= INFINITE_POWER {
			return f64::INFINITY;
		}
	}
	let mut power2 = exp2 - EXP_MIN + 1;
	if mantissa < (1_u64 << SIG_BITS) {
		power2 -= 1;
	}
	let mantissa = mantissa & ((1_u64 << SIG_BITS) - 1);
	f64::from_bits(mantissa | ((power2 as u64) << SIG_BITS))
}

/// Position of the decimal point relative to the first digit.
fn decimal_point(d: &D) -> i32 {
	d.digits.checked_ilog10().unwrap_or(0) as i32 + 1 - d.scale as i32
}

fn mul_power_of_two(d: &mut D, mut power: u32) {
	while power > 0 {
		let shift = power.min(MAX_POWER_OF_TWO);
		*d = mul(*d, D::new(1u128 << shift, 0, false));
		power -= shift;
	}
}

/// Divides by `2^power` as a multiplication by `5^power · 10^-power`.
fn div_power_of_two(d: &mut D, mut power: u32) {
	while power > 0 {
		let shift = power.min(MAX_POWER_OF_FIVE);
		*d = mul(*d, D::new(5u128.pow(shift), shift as i16, false));
		power -= shift;
	}
}

/// Whether a positive `d` is below one half. A guard counts only when the
/// coefficients compare equal, as a value just above.
fn lt_half(d: &D) -> bool {
	let power = decimal_digits(d.digits) as i32 - d.scale as i32;
	if power != 0 {
		return power < 0;
	}
	// Strip trailing zeros unless a guard pins the last digit.
	let mut r = *d;
	if r.guard == 0 {
		while r.digits != 0 && r.digits.is_multiple_of(10) {
			r.digits /= 10;
			r.scale -= 1;
		}
	}
	let ord = match r.scale.cmp(&1) {
		Ordering::Equal => r.digits.cmp(&5),
		Ordering::Greater => {
			let p = (r.scale - 1) as u32;
			if p > remaining_decimal_digits(5) {
				Ordering::Less
			} else {
				r.digits.cmp(&(5 * pow10(p)))
			}
		}
		Ordering::Less => {
			let p = (1 - r.scale) as u32;
			if p > remaining_decimal_digits(r.digits) {
				Ordering::Greater
			} else {
				(r.digits * pow10(p)).cmp(&5)
			}
		}
	};
	match ord {
		Ordering::Equal => false,
		o => o == Ordering::Less,
	}
}

/// Rounds to an integer half-even on the first guard digit alone, so a guard
/// of `5000001` still counts as a tie, then narrows to 64 bits the same way.
/// The existing guard is rounded into the coefficient first, so the integer
/// rounding sees a digit that may already have been carried into.
fn round_to_u64(mut d: D) -> u64 {
	round_half_even(&mut d);
	rescale(&mut d, 0);
	round_half_even(&mut d);
	let power = decimal_digits(d.digits).saturating_sub(20);
	if power > 0 {
		let scale = d.scale - power as i16;
		rescale(&mut d, scale);
	}
	if d.digits > u64::MAX as u128 {
		let scale = d.scale - 1;
		rescale(&mut d, scale);
	}
	round_half_even(&mut d);
	if d.digits >= u64::MAX as u128 {
		u64::MAX
	} else {
		d.digits as u64
	}
}

fn round_half_even(d: &mut D) {
	let digit = d.take_guard().first_digit();
	if digit > 5 || (digit == 5 && d.digits & 1 == 1) {
		if d.digits == u128::MAX {
			d.digits /= 10;
			d.scale -= 1;
		}
		d.digits += 1;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Every truncated power of two keeps 39 digits, or 38 where 39 would not
	/// fit, and reassembles to within one guard unit below the exact power.
	#[test]
	fn wide_powers_of_two_are_truncations() {
		for (i, &(digits, power, guard)) in wide_powers_of_two().iter().enumerate() {
			let b = i as u32 + 128;
			let dd = decimal_digits(digits);
			assert!(dd == 39 || (dd == 38 && digits >= (u128::MAX - 9) / 10), "2^{b}: {dd} digits");
			assert!(guard < GUARD_CARRY as u32, "2^{b}: guard {guard}");
			// The digit count of 2^b is the coefficient's plus the dropped power.
			let expected = ((b as f64) * std::f64::consts::LOG10_2).floor() as u32 + 1;
			assert_eq!(dd + power as u32, expected, "2^{b}");
		}
	}

	#[test]
	fn small_powers_of_two_are_exact() {
		assert_eq!(power_of_two(127).digits, 1u128 << 127);
		assert_eq!(power_of_two(0).digits, 1);
	}
}
