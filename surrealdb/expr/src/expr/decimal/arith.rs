//! Guarded decimal arithmetic over a 128-bit coefficient.
//!
//! Values are `±digits · 10^-scale` with a 7-digit *guard* fraction below the
//! last coefficient digit. The guard is truncated, never rounded, and only its
//! first digit takes part in a final rounding. Operations keep the guard
//! through rescaling, addition, multiplication and long division, and correct
//! a product or quotient for the guards of its operands with further guarded
//! operations.
//!
//! This is the arithmetic behind the float conversions in `float.rs`, whose
//! output is persisted in index keys, so it is specified by its exact
//! results: every operation, including its asymmetries between positive and
//! negative intermediates, is part of the format. Do not simplify it towards
//! mathematically exact results.

use std::cmp::Ordering;

/// Number of guard digits carried below the coefficient.
pub(super) const GUARD_DIGITS: u32 = 7;
/// `10^GUARD_DIGITS`, one unit of the coefficient expressed in guard units.
pub(super) const GUARD_CARRY: u64 = 10_000_000;
/// Width of the guard field; guard values are stored masked to it.
pub(super) const GUARD_MASK: u32 = 0x00FF_FFFF;
/// Largest `p` such that `10^p` fits a `u64`.
pub(super) const DIGIT_POWER_10: u32 = 19;

pub(super) const POW10_U64: [u64; 20] = {
	let mut t = [1u64; 20];
	let mut i = 1;
	while i < 20 {
		t[i] = t[i - 1] * 10;
		i += 1;
	}
	t
};

pub(super) const POW10_U128: [u128; 39] = {
	let mut t = [1u128; 39];
	let mut i = 1;
	while i < 39 {
		t[i] = t[i - 1] * 10;
		i += 1;
	}
	t
};

// ---------------------------------------------------------------------------
// Guarded decimal value
// ---------------------------------------------------------------------------

/// A decimal `±digits · 10^-scale` with a truncated guard fraction `guard /
/// 10^7` of one coefficient unit.
#[derive(Clone, Copy, Debug)]
pub(super) struct D {
	pub(super) digits: u128,
	pub(super) scale: i16,
	pub(super) neg: bool,
	pub(super) guard: u32,
}

impl D {
	pub(super) const ZERO: D = D {
		digits: 0,
		scale: 0,
		neg: false,
		guard: 0,
	};

	pub(super) fn new(digits: u128, scale: i16, neg: bool) -> Self {
		D {
			digits,
			scale,
			neg,
			guard: 0,
		}
	}

	pub(super) fn construct(digits: u128, exp: i32, neg: bool, guard: Guard) -> Self {
		D {
			digits,
			scale: (-exp) as i16,
			neg,
			guard: guard.digits & GUARD_MASK,
		}
	}

	pub(super) fn exponent(&self) -> i32 {
		-(self.scale as i32)
	}

	pub(super) fn is_zero(&self) -> bool {
		self.digits == 0 && self.guard == 0
	}

	pub(super) fn is_one(&self) -> bool {
		self.digits == 1 && self.scale == 0 && !self.neg && self.guard == 0
	}

	pub(super) fn neg(mut self) -> Self {
		self.neg = !self.neg;
		self
	}

	pub(super) fn without_guard(mut self) -> Self {
		self.guard = 0;
		self
	}

	pub(super) fn guard(&self) -> Guard {
		Guard::from_digits(self.guard as u64, GUARD_DIGITS)
	}

	pub(super) fn take_guard(&mut self) -> Guard {
		let g = self.guard();
		self.guard = 0;
		g
	}

	pub(super) fn set_guard(&mut self, g: Guard) {
		self.guard = g.digits & GUARD_MASK;
	}

	/// Removes the guard and returns it as a decimal value of its own.
	pub(super) fn take_guard_decimal(&mut self) -> D {
		let g = self.take_guard();
		if g.digits != 0 {
			D::construct(
				g.digits as u128,
				self.exponent() - GUARD_DIGITS as i32,
				self.neg,
				Guard::NONE,
			)
		} else {
			D {
				neg: self.neg,
				..D::ZERO
			}
		}
	}
}

/// A guard fraction in transit, with the number of meaningful digits it holds.
#[derive(Clone, Copy, Debug)]
pub(super) struct Guard {
	digits: u32,
	count: u32,
}

impl Guard {
	pub(super) const NONE: Guard = Guard {
		digits: 0,
		count: 0,
	};

	pub(super) fn from_digits(digits: u64, count: u32) -> Self {
		let (digits, count) = normalize_guard(digits, count);
		Guard {
			digits,
			count,
		}
	}

	pub(super) fn first_digit(self) -> u32 {
		self.digits / 1_000_000
	}

	pub(super) fn push_back(&mut self, tail: Guard) {
		match self.count {
			1..=6 => {
				self.digits += tail.digits / POW10_U64[self.count as usize] as u32;
				self.count += tail.count.min(GUARD_DIGITS - self.count);
			}
			0 => *self = tail,
			_ => {}
		}
	}

	pub(super) fn append(&mut self, tail: u64, count: u32) {
		if tail == 0 {
			return;
		}
		let (tail, count) = normalize_guard(tail, count);
		match self.count {
			1..=6 => {
				self.digits += tail / POW10_U64[self.count as usize] as u32;
				self.count += count.min(GUARD_DIGITS - self.count);
			}
			0 => {
				self.digits = tail;
				self.count = count;
			}
			_ => {}
		}
	}

	/// Shifts `power` leading guard digits out as an integer correction.
	pub(super) fn scale_up(&mut self, mut power: u32) -> Option<D> {
		if self.digits == 0 {
			return None;
		}
		let mut res = D::ZERO;
		let ten = D::new(10, 0, false);
		while power > 0 {
			let digit = self.digits / 1_000_000;
			res = round(add(round(mul(res, ten)), D::new(digit as u128, 0, false)));
			self.digits = (self.digits % 1_000_000) * 10;
			power -= 1;
			self.count = self.count.saturating_sub(1);
		}
		if res.is_zero() {
			None
		} else {
			Some(res)
		}
	}

	pub(super) fn overflowing_add(&mut self, other: Guard) -> bool {
		let res = self.digits as u64 + other.digits as u64;
		if res >= GUARD_CARRY {
			let res = res - GUARD_CARRY;
			self.digits = res as u32;
			self.count = count_digits(res);
			true
		} else {
			self.digits = res as u32;
			self.count = count_digits(res);
			false
		}
	}

	pub(super) fn overflowing_sub(&mut self, other: Guard) -> bool {
		if self.digits >= other.digits {
			self.digits -= other.digits;
			self.count = count_digits(self.digits as u64);
			false
		} else {
			let res = GUARD_CARRY - (other.digits - self.digits) as u64;
			self.digits = res as u32;
			self.count = count_digits(res);
			true
		}
	}
}

pub(super) fn count_digits(digits: u64) -> u32 {
	if digits == 0 {
		0
	} else {
		decimal_digits_64(digits).min(GUARD_DIGITS)
	}
}

pub(super) fn normalize_guard(mut digits: u64, mut count: u32) -> (u32, u32) {
	if count > GUARD_DIGITS {
		digits /= POW10_U64[(count - GUARD_DIGITS) as usize];
		count = GUARD_DIGITS;
	} else if count < GUARD_DIGITS {
		digits = digits.wrapping_mul(POW10_U64[(GUARD_DIGITS - count) as usize]);
	}
	(digits as u32, count)
}

// ---------------------------------------------------------------------------
// Integer helpers
// ---------------------------------------------------------------------------

pub(super) fn decimal_digits_64(n: u64) -> u32 {
	if n == 0 {
		0
	} else {
		n.ilog10() + 1
	}
}

pub(super) fn decimal_digits(n: u128) -> u32 {
	if n == 0 {
		0
	} else {
		n.ilog10() + 1
	}
}

/// The largest `k` such that `n · 10^k` fits a `u128` (39 for zero).
pub(super) fn remaining_decimal_digits(n: u128) -> u32 {
	let dd = decimal_digits(n);
	let mut max_digits = 39;
	if dd != 0 && u128::MAX / pow10(max_digits - dd) < n {
		max_digits -= 1;
	}
	max_digits - dd
}

pub(super) fn can_scale_by_power_of_ten(n: u128, power: u32) -> bool {
	n <= u128::MAX / pow10(power)
}

pub(super) fn pow10(p: u32) -> u128 {
	if p < 39 {
		POW10_U128[p as usize]
	} else {
		// 10^39 exceeds u128; only reached through `u128::MAX / 10^p` style
		// bounds where the true quotient is zero.
		u128::MAX
	}
}

/// Full 256-bit product of two `u128`s as `(low, high)`.
pub(super) fn widening_mul(a: u128, b: u128) -> (u128, u128) {
	let (a0, a1) = (a as u64 as u128, a >> 64);
	let (b0, b1) = (b as u64 as u128, b >> 64);
	let p00 = a0 * b0;
	let p01 = a0 * b1;
	let p10 = a1 * b0;
	let p11 = a1 * b1;
	let mid = (p00 >> 64) + (p01 as u64 as u128) + (p10 as u64 as u128);
	let low = (p00 as u64 as u128) | (mid << 64);
	let high = p11 + (p01 >> 64) + (p10 >> 64) + (mid >> 64);
	(low, high)
}

/// Divides the 256-bit value `(low, high)` by `d`, returning the remainder.
pub(super) fn div_rem_256_by_u64(low: &mut u128, high: &mut u128, d: u64) -> u64 {
	let words = [*low as u64, (*low >> 64) as u64, *high as u64, (*high >> 64) as u64];
	let mut out = [0u64; 4];
	let mut rem: u128 = 0;
	for i in (0..4).rev() {
		let cur = (rem << 64) | words[i] as u128;
		let q = cur / d as u128;
		out[i] = q as u64;
		rem = cur - q * d as u128;
	}
	*low = out[0] as u128 | ((out[1] as u128) << 64);
	*high = out[2] as u128 | ((out[3] as u128) << 64);
	rem as u64
}

/// `(a · b) / d` and `(a · b) % d`, for operands where the quotient fits in
/// 64 bits.
pub(super) fn mul_div_rem(a: u128, b: u128, d: u128) -> (u64, u128) {
	let (low, high) = widening_mul(a, b);
	if high == 0 {
		let q = low / d;
		return (q as u64, low - q * d);
	}
	// A 128-bit product never needs the wide path, so `d` has a high word.
	debug_assert!(d >> 64 != 0);
	// Normalise so the divisor's top bit is set; the quotient is unchanged
	// and the remainder is shifted by the same amount.
	let s = d.leading_zeros();
	let d = d << s;
	let (hi, lo) = if s == 0 {
		(high, low)
	} else {
		((high << s) | (low >> (128 - s)), low << s)
	};
	// The numerator is below d · 2^64, so it has three significant words and
	// a single-word quotient estimated from the divisor's top word, at most
	// two too large.
	let top = (hi << 64) | (lo >> 64);
	let d1 = d >> 64;
	let mut q = (top / d1).min(u64::MAX as u128);
	let (mut p_lo, mut p_hi) = widening_mul(q, d);
	while (p_hi, p_lo) > (hi, lo) {
		q -= 1;
		let (l, borrow) = p_lo.overflowing_sub(d);
		p_lo = l;
		p_hi -= borrow as u128;
	}
	let r = lo.wrapping_sub(p_lo);
	(q as u64, r >> s)
}

// ---------------------------------------------------------------------------
// Rescaling
// ---------------------------------------------------------------------------

/// Moves `d` to `new_scale`. Returns whether an upward rescale was clamped
/// short of the target because the coefficient ran out of room.
pub(super) fn rescale(d: &mut D, new_scale: i16) -> bool {
	match new_scale.cmp(&d.scale) {
		Ordering::Greater => rescale_up(d, new_scale),
		Ordering::Less => {
			rescale_down(d, new_scale);
			false
		}
		Ordering::Equal => false,
	}
}

pub(super) fn extend_scale_to(mut d: D, new_scale: i16) -> D {
	if new_scale > d.scale {
		rescale(&mut d, new_scale);
	}
	d
}

pub(super) fn rescale_up(d: &mut D, new_scale: i16) -> bool {
	let power = (new_scale as i32 - d.scale as i32) as u32;
	if d.digits == 0 {
		d.scale = new_scale;
		let mut g = d.guard();
		if let Some(c) = g.scale_up(power) {
			scale_correct(d, c);
		}
		d.set_guard(g);
		return false;
	}
	let remaining = remaining_decimal_digits(d.digits);
	if remaining == 0 {
		true
	} else if remaining < power {
		rescale_up_unchecked(d, remaining);
		true
	} else {
		rescale_up_unchecked(d, power);
		false
	}
}

pub(super) fn rescale_up_unchecked(d: &mut D, gap: u32) {
	d.digits *= pow10(gap);
	d.scale = (d.scale as i32 + gap as i32) as i16;
	let mut g = d.guard();
	if let Some(c) = g.scale_up(gap) {
		scale_correct(d, c);
	}
	d.set_guard(g);
}

pub(super) fn scale_correct(d: &mut D, mut c: D) {
	c.neg = d.neg;
	c.scale = (c.scale as i32 + d.scale as i32) as i16;
	*d = add(*d, c);
}

pub(super) fn rescale_down(d: &mut D, new_scale: i16) {
	let old = d.guard();
	let power = (d.scale as i32 - new_scale as i32) as u32;
	let dd = decimal_digits(d.digits);
	if dd <= power {
		let shift = power - dd;
		if shift >= GUARD_DIGITS {
			d.guard = 0;
		} else {
			let g = make_guard(d.digits, dd, shift);
			d.set_guard(g);
		}
		d.digits = 0;
	} else {
		let divisor = pow10(power);
		let extra = d.digits % divisor;
		d.digits /= divisor;
		if extra != 0 || old.digits != 0 {
			let mut g = make_guard(extra, power, 0);
			g.push_back(old);
			d.set_guard(g);
		}
	}
	d.scale = (d.scale as i32 - power as i32) as i16;
}

pub(super) fn make_guard(digits: u128, dd: u32, shift: u32) -> Guard {
	let power = dd.saturating_sub(GUARD_DIGITS - shift);
	let v = if power > 0 {
		(digits / pow10(power)) as u64
	} else {
		digits as u64
	};
	Guard::from_digits(v, dd - power + shift)
}

// ---------------------------------------------------------------------------
// Addition and subtraction
// ---------------------------------------------------------------------------

pub(super) fn add(lhs: D, rhs: D) -> D {
	match (lhs.neg, rhs.neg) {
		(false, false) => add_abs(lhs, rhs),
		(true, true) => add_abs(rhs.neg(), lhs.neg()).neg(),
		(false, true) => sub_abs(lhs, rhs.neg()),
		(true, false) => sub_abs(rhs, lhs.neg()),
	}
}

pub(super) fn sub(lhs: D, rhs: D) -> D {
	match (lhs.neg, rhs.neg) {
		(false, false) => sub_abs(lhs, rhs),
		(true, true) => sub_abs(rhs.neg(), lhs.neg()),
		(false, true) => add_abs(lhs, rhs.neg()),
		(true, false) => add_abs(lhs.neg(), rhs).neg(),
	}
}

pub(super) fn add_abs(lhs: D, rhs: D) -> D {
	if rhs.is_zero() {
		return extend_scale_to(lhs, rhs.scale);
	}
	if lhs.is_zero() {
		return extend_scale_to(rhs, lhs.scale);
	}
	match lhs.scale.cmp(&rhs.scale) {
		Ordering::Less => add_rescale(lhs, rhs),
		Ordering::Equal => add_aligned(lhs, rhs),
		Ordering::Greater => add_rescale(rhs, lhs),
	}
}

pub(super) fn add_rescale(mut lhs: D, mut rhs: D) -> D {
	if rescale(&mut lhs, rhs.scale) {
		rescale(&mut rhs, lhs.scale);
	}
	add_aligned(lhs, rhs)
}

pub(super) fn add_aligned(mut lhs: D, mut rhs: D) -> D {
	match lhs.digits.overflowing_add(rhs.digits) {
		(digits, false) => {
			lhs.digits = digits;
			let mut g = lhs.take_guard();
			let carry = g.overflowing_add(rhs.guard());
			lhs.set_guard(g);
			if carry {
				magnitude_inc(&mut lhs);
			}
			lhs
		}
		(_, true) => {
			let scale = lhs.scale - 1;
			rescale(&mut lhs, scale);
			rescale(&mut rhs, scale);
			add_aligned(lhs, rhs)
		}
	}
}

pub(super) fn sub_abs(mut lhs: D, mut rhs: D) -> D {
	if rhs.is_zero() {
		return extend_scale_to(lhs, rhs.scale);
	}
	if lhs.is_zero() {
		return extend_scale_to(rhs, lhs.scale).neg();
	}
	match lhs.scale.cmp(&rhs.scale) {
		Ordering::Equal => {}
		Ordering::Less => {
			if rescale(&mut lhs, rhs.scale) {
				rescale(&mut rhs, lhs.scale);
			}
		}
		Ordering::Greater => {
			if rescale(&mut rhs, lhs.scale) {
				rescale(&mut lhs, rhs.scale);
			}
		}
	}
	sub_aligned(lhs, rhs)
}

pub(super) fn sub_aligned(mut lhs: D, mut rhs: D) -> D {
	match lhs.digits.cmp(&rhs.digits) {
		Ordering::Less => {
			rhs.digits -= lhs.digits;
			sub_guard(&mut rhs, &lhs);
			rhs.neg()
		}
		Ordering::Equal => {
			lhs.digits = 0;
			sub_guard(&mut lhs, &rhs);
			lhs
		}
		Ordering::Greater => {
			lhs.digits -= rhs.digits;
			sub_guard(&mut lhs, &rhs);
			lhs
		}
	}
}

pub(super) fn sub_guard(this: &mut D, other: &D) {
	let mut g = this.take_guard();
	let borrow = g.overflowing_sub(other.guard());
	this.set_guard(g);
	if borrow {
		magnitude_dec(this);
	}
}

/// Steps the coefficient by one unit after a guard carry. For a negative
/// value this adds a negative unit through subtraction, which moves the
/// coefficient towards zero; that direction is part of the reproduced
/// algorithm.
pub(super) fn magnitude_inc(d: &mut D) {
	let one = D::new(1, d.scale, d.neg);
	*d = if d.neg {
		sub(*d, one)
	} else {
		add(*d, one)
	};
}

/// Steps the coefficient by one unit after a guard borrow, with the same sign
/// handling as [`magnitude_inc`].
pub(super) fn magnitude_dec(d: &mut D) {
	let one = D::new(1, d.scale, d.neg);
	*d = if d.neg {
		add(*d, one)
	} else {
		sub(*d, one)
	};
}

/// Adds a correction term unless it is zero.
pub(super) fn correct(d: D, correction: D) -> D {
	if correction.is_zero() {
		d
	} else {
		add(d, correction)
	}
}

// ---------------------------------------------------------------------------
// Multiplication
// ---------------------------------------------------------------------------

pub(super) fn mul(mut lhs: D, mut rhs: D) -> D {
	let neg = lhs.neg != rhs.neg;
	let mut exp = lhs.exponent() + rhs.exponent();
	let mut guard = Guard::NONE;
	if lhs.is_zero() || rhs.is_zero() {
		return D::construct(0, exp, neg, guard);
	}
	let correction = if lhs.guard != 0 || rhs.guard != 0 {
		Some(mul_correction(&mut lhs, &mut rhs))
	} else {
		None
	};
	let (mut low, mut high) = widening_mul(lhs.digits, rhs.digits);
	// Drop low digits until the product fits, the dropped digits becoming the
	// guard. Each step removes at most as many digits as the excess bits
	// guarantee are surplus, so the product ends at the widest coefficient
	// that fits, exactly as dropping one digit at a time would.
	while high != 0 {
		let excess = 128 - high.leading_zeros();
		let j = (((excess as u64 * 77) >> 8) as u32).clamp(1, DIGIT_POWER_10);
		let rem = div_rem_256_by_u64(&mut low, &mut high, POW10_U64[j as usize]);
		let mut g = make_guard(rem as u128, j, 0);
		g.push_back(guard);
		guard = g;
		exp += j as i32;
	}
	let result = D::construct(low, exp, neg, guard);
	match correction {
		Some(c) => correct(result, c),
		None => result,
	}
}

pub(super) fn mul_correction(lhs: &mut D, rhs: &mut D) -> D {
	let xi_lhs = lhs.take_guard_decimal();
	let xi_rhs = rhs.take_guard_decimal();
	match (xi_lhs.is_zero(), xi_rhs.is_zero()) {
		(true, true) => D::ZERO,
		(true, false) => mul(*lhs, xi_rhs),
		(false, true) => mul(*rhs, xi_lhs),
		(false, false) => add(mul(*lhs, xi_rhs), add(mul(*rhs, xi_lhs), mul(xi_rhs, xi_lhs))),
	}
}

// ---------------------------------------------------------------------------
// Division
// ---------------------------------------------------------------------------

pub(super) fn div(mut dividend: D, mut divisor: D) -> D {
	let neg = dividend.neg != divisor.neg;
	debug_assert!(!divisor.is_zero());
	if dividend.is_zero() || divisor.is_one() {
		dividend.neg = neg;
		return dividend;
	}
	let correction = if dividend.guard != 0 || divisor.guard != 0 {
		Some(div_correction(&mut dividend, &mut divisor))
	} else {
		None
	};
	let exp = dividend.exponent() - divisor.exponent();
	let quotient = dividend.digits / divisor.digits;
	let result = LongDivision {
		digits: quotient,
		remainder: dividend.digits - quotient * divisor.digits,
		divisor: divisor.digits,
		neg,
		exp,
		guard: Guard::NONE,
	}
	.run();
	match correction {
		Some(c) => correct(result, c),
		None => result,
	}
}

pub(super) fn div_correction(dividend: &mut D, divisor: &mut D) -> D {
	let xi_dividend = dividend.take_guard_decimal();
	let xi_divisor = divisor.take_guard_decimal();
	if xi_dividend.is_zero() && xi_divisor.is_zero() {
		return D::ZERO;
	}
	if xi_divisor.is_zero() {
		return div(xi_dividend, *divisor);
	}
	let x = if xi_dividend.is_zero() {
		mul(*dividend, xi_divisor).neg().without_guard()
	} else {
		sub(mul(*divisor, xi_dividend), mul(*dividend, xi_divisor)).without_guard()
	};
	if x.is_zero() {
		return D::ZERO;
	}
	let y = add(mul(*divisor, *divisor), mul(*divisor, xi_divisor)).without_guard();
	div(x, y)
}

pub(super) struct LongDivision {
	digits: u128,
	remainder: u128,
	divisor: u128,
	neg: bool,
	exp: i32,
	guard: Guard,
}

impl LongDivision {
	pub(super) fn run(mut self) -> D {
		if self.remainder != 0 {
			self.fractional_part();
		}
		D::construct(self.digits, self.exp, self.neg, self.guard)
	}

	/// Appends `quotient` (holding `mul_power + add_power` digits) to the
	/// coefficient, returning the `add_power` digits that did not fit.
	pub(super) fn mul_add(&mut self, quotient: u64, mul_power: u32, add_power: u32) -> u64 {
		self.digits *= POW10_U64[mul_power as usize] as u128;
		self.exp -= mul_power as i32;
		let qd = POW10_U64[add_power as usize];
		let (mut quotient, mut extra) = (quotient / qd, quotient % qd);
		match self.digits.overflowing_add(quotient as u128) {
			(digits, false) => self.digits = digits,
			(_, true) => {
				self.digits /= 10;
				(quotient, extra) = (quotient / 10, quotient % 10);
				self.digits += quotient as u128;
				self.exp += 1;
			}
		}
		extra
	}

	pub(super) fn fractional_part(&mut self) {
		let mut power = 0;
		let mut extra = 0;
		let mut carry = false;
		while self.remainder != 0 || carry {
			let remaining = remaining_decimal_digits(self.digits);
			if carry {
				if remaining != 0 {
					if remaining <= power {
						let p = power - remaining;
						extra = self.mul_add(extra, remaining, p);
						return self.set_guard(extra, p);
					}
					extra = self.mul_add(extra, power, 0);
					carry = false;
					continue;
				}
				return self.set_guard(extra, power);
			}
			let mut q_power = remaining.min(DIGIT_POWER_10);
			let max_power = (q_power + GUARD_DIGITS).min(DIGIT_POWER_10);
			let mut quotient = self.next_digits(max_power);
			if remaining == 0 {
				return self.set_guard(quotient, max_power);
			}
			if quotient == 0 {
				let p = max_power.min(q_power);
				if self.digits != 0 {
					self.digits *= POW10_U64[p as usize] as u128;
				}
				self.exp -= p as i32;
				continue;
			}
			power = decimal_digits_64(quotient);
			if q_power == 0 {
				return self.set_guard(quotient, power);
			}
			let leading_zeros = max_power - power;
			if self.remainder == 0 {
				while quotient >= 10 && quotient.is_multiple_of(10) {
					quotient /= 10;
					power -= 1;
				}
			}
			if power < q_power {
				q_power = power;
			}
			power = power - q_power + leading_zeros;
			extra = self.mul_add(quotient, q_power, power);
			if power != 0 {
				carry = true;
			}
		}
	}

	/// The next `power` quotient digits; advances the remainder.
	pub(super) fn next_digits(&mut self, power: u32) -> u64 {
		let (q, r) = self.peek_digits(power);
		self.remainder = r;
		q
	}

	pub(super) fn peek_digits(&self, power: u32) -> (u64, u128) {
		if can_scale_by_power_of_ten(self.remainder, power) {
			let n = self.remainder * pow10(power);
			let q = n / self.divisor;
			(q as u64, n - q * self.divisor)
		} else {
			mul_div_rem(self.remainder, pow10(power), self.divisor)
		}
	}

	pub(super) fn set_guard(&mut self, quotient: u64, power: u32) {
		let mut g = Guard::from_digits(quotient, power);
		let rest = GUARD_DIGITS.saturating_sub(power);
		if self.remainder != 0 && rest != 0 {
			let (q, _) = self.peek_digits(rest);
			g.append(q, rest);
		}
		if g.digits != 0 || self.remainder != 0 {
			self.guard = g;
		}
	}
}

/// Rounds half-up on the first guard digit, consuming the guard.
pub(super) fn round(mut d: D) -> D {
	let digit = d.take_guard().first_digit();
	if digit >= 5 {
		if d.digits == u128::MAX {
			d.digits /= 10;
			d.scale -= 1;
		}
		d.digits += 1;
	}
	d
}
