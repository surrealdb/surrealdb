//! Decimal type for SQL operations.

use num_traits::{FromPrimitive, Num, One, Signed, ToPrimitive, Zero};
use revision::Revisioned;
use rust_decimal::Decimal as RustDecimal;
use rust_decimal::MathematicalOps;
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Display, Formatter};
use std::ops::Add;
use std::ops::{AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Rem, Sub, SubAssign};
use std::str::FromStr;

/// Decimal type for SQL operations.
///
/// This type is used for representing decimal numbers in SQL queries and
/// operations.
///
/// The `Decimal` type is a wrapper around the `rust_decimal` crate's `Decimal`
/// type. The purpose of this wrapper is to ensure that the `Decimal` type
/// is consistently parsed and serialized correctly.
#[derive(
	Clone, Copy, Default, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[repr(transparent)]
pub struct Decimal(pub RustDecimal);

impl Decimal {
	pub const ZERO: Decimal = Decimal(RustDecimal::ZERO);
	pub const MIN: Decimal = Decimal(RustDecimal::MIN);
	pub const MAX: Decimal = Decimal(RustDecimal::MAX);

	#[inline]
	pub fn new(num: i64, scale: u32) -> Decimal {
		Decimal(RustDecimal::new(num, scale))
	}

	#[inline]
	pub fn try_new(num: i64, scale: u32) -> Result<Decimal, rust_decimal::Error> {
		RustDecimal::try_new(num, scale).map(Decimal)
	}

	#[inline]
	pub fn from_str_exact(num: &str) -> Result<Decimal, rust_decimal::Error> {
		RustDecimal::from_str_exact(num).map(|d| Decimal(d.normalize()))
	}

	#[inline]
	pub fn is_integer(&self) -> bool {
		self.0.is_integer()
	}

	#[inline]
	pub fn abs(self) -> Self {
		Decimal(self.0.abs())
	}

	#[inline]
	pub fn floor(self) -> Self {
		Decimal(self.0.floor())
	}

	#[inline]
	pub fn ceil(self) -> Self {
		Decimal(self.0.ceil())
	}

	#[inline]
	pub fn max(self, other: Self) -> Self {
		Decimal(self.0.max(other.0))
	}

	#[inline]
	pub fn min(self, other: Self) -> Self {
		Decimal(self.0.min(other.0))
	}

	#[inline]
	pub fn round(self) -> Self {
		Decimal(self.0.round())
	}

	#[inline]
	pub fn round_dp(self, dp: u32) -> Self {
		Decimal(self.0.round_dp(dp))
	}

	#[inline]
	pub fn clamp(self, min: Self, max: Self) -> Self {
		Decimal(self.0.clamp(min.0, max.0))
	}

	#[inline]
	pub fn fract(self) -> Self {
		Decimal(self.0.fract())
	}

	#[inline]
	pub fn from_scientific(num: &str) -> Result<Self, rust_decimal::Error> {
		RustDecimal::from_scientific(num).map(Decimal)
	}

	#[inline]
	pub fn sqrt(self) -> Option<Self> {
		self.0.sqrt().map(Decimal)
	}

	#[inline]
	pub fn powi(self, exp: i64) -> Self {
		Decimal(self.0.powi(exp))
	}

	#[inline]
	pub fn is_sign_positive(&self) -> bool {
		self.0.is_sign_positive()
	}

	#[inline]
	pub fn is_sign_negative(&self) -> bool {
		self.0.is_sign_negative()
	}

	#[inline]
	pub fn checked_add(self, other: Self) -> Option<Self> {
		self.0.checked_add(other.0).map(Decimal)
	}

	#[inline]
	pub fn checked_sub(self, other: Self) -> Option<Self> {
		self.0.checked_sub(other.0).map(Decimal)
	}

	#[inline]
	pub fn checked_mul(self, other: Self) -> Option<Self> {
		self.0.checked_mul(other.0).map(Decimal)
	}

	#[inline]
	pub fn checked_div(self, other: Self) -> Option<Self> {
		self.0.checked_div(other.0).map(|v| Decimal(v.normalize()))
	}

	#[inline]
	pub fn checked_rem(self, other: Self) -> Option<Self> {
		self.0.checked_rem(other.0).map(Decimal)
	}

	#[inline]
	pub fn checked_powi(self, exp: i64) -> Option<Self> {
		self.0.checked_powi(exp).map(Decimal)
	}

	#[inline]
	pub fn checked_powf(self, exp: f64) -> Option<Self> {
		self.0.checked_powf(exp).map(Decimal)
	}

	#[inline]
	pub fn checked_powd(self, exp: Decimal) -> Option<Self> {
		self.0.checked_powd(exp.0).map(Decimal)
	}
}

impl Display for Decimal {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

macro_rules! impl_ops {
	($trait:ident, $method:ident) => {
		impl $trait for Decimal {
			type Output = Self;

			fn $method(self, rhs: Self) -> Self::Output {
				Decimal(self.0.$method(rhs.0))
			}
		}

		impl $trait for &Decimal {
			type Output = Decimal;

			fn $method(self, rhs: Self) -> Self::Output {
				Decimal(self.0.$method(rhs.0))
			}
		}
	};
}

impl_ops!(Add, add);
impl_ops!(Sub, sub);
impl_ops!(Mul, mul);
impl_ops!(Div, div);
impl_ops!(Rem, rem);

impl AddAssign for Decimal {
	fn add_assign(&mut self, rhs: Self) {
		self.0.add_assign(rhs.0);
	}
}

impl SubAssign for Decimal {
	fn sub_assign(&mut self, rhs: Self) {
		self.0.sub_assign(rhs.0);
	}
}

impl DivAssign for Decimal {
	fn div_assign(&mut self, rhs: Self) {
		self.0.div_assign(rhs.0);
	}
}

impl MulAssign for Decimal {
	fn mul_assign(&mut self, rhs: Self) {
		self.0.mul_assign(rhs.0);
	}
}

impl FromPrimitive for Decimal {
	fn from_i8(n: i8) -> Option<Self> {
		RustDecimal::from_i8(n).map(Decimal)
	}

	fn from_u8(n: u8) -> Option<Self> {
		RustDecimal::from_u8(n).map(Decimal)
	}

	fn from_i16(n: i16) -> Option<Self> {
		RustDecimal::from_i16(n).map(Decimal)
	}

	fn from_u16(n: u16) -> Option<Self> {
		RustDecimal::from_u16(n).map(Decimal)
	}

	fn from_i32(n: i32) -> Option<Self> {
		RustDecimal::from_i32(n).map(Decimal)
	}

	fn from_u32(n: u32) -> Option<Self> {
		RustDecimal::from_u32(n).map(Decimal)
	}

	fn from_i64(n: i64) -> Option<Self> {
		RustDecimal::from_i64(n).map(Decimal)
	}

	fn from_u64(n: u64) -> Option<Self> {
		RustDecimal::from_u64(n).map(Decimal)
	}

	fn from_i128(n: i128) -> Option<Self> {
		RustDecimal::from_i128(n).map(Decimal)
	}

	fn from_u128(n: u128) -> Option<Self> {
		RustDecimal::from_u128(n).map(Decimal)
	}

	fn from_isize(n: isize) -> Option<Self> {
		RustDecimal::from_isize(n).map(Decimal)
	}

	fn from_usize(n: usize) -> Option<Self> {
		RustDecimal::from_usize(n).map(Decimal)
	}

	fn from_f32(n: f32) -> Option<Self> {
		RustDecimal::from_f32(n).map(|v| Decimal(v.normalize()))
	}

	fn from_f64(n: f64) -> Option<Self> {
		RustDecimal::from_f64(n).map(|v| Decimal(v.normalize()))
	}
}

impl ToPrimitive for Decimal {
	fn to_u8(&self) -> Option<u8> {
		self.0.to_u8()
	}

	fn to_i8(&self) -> Option<i8> {
		self.0.to_i8()
	}

	fn to_u16(&self) -> Option<u16> {
		self.0.to_u16()
	}

	fn to_i16(&self) -> Option<i16> {
		self.0.to_i16()
	}

	fn to_u32(&self) -> Option<u32> {
		self.0.to_u32()
	}

	fn to_i32(&self) -> Option<i32> {
		self.0.to_i32()
	}

	fn to_i64(&self) -> Option<i64> {
		self.0.to_i64()
	}

	fn to_u64(&self) -> Option<u64> {
		self.0.to_u64()
	}

	fn to_i128(&self) -> Option<i128> {
		self.0.to_i128()
	}

	fn to_u128(&self) -> Option<u128> {
		self.0.to_u128()
	}

	fn to_isize(&self) -> Option<isize> {
		self.0.to_isize()
	}

	fn to_usize(&self) -> Option<usize> {
		self.0.to_usize()
	}

	fn to_f32(&self) -> Option<f32> {
		self.0.to_f32()
	}

	fn to_f64(&self) -> Option<f64> {
		self.0.to_f64()
	}
}

impl Zero for Decimal {
	fn zero() -> Self {
		Decimal(RustDecimal::zero())
	}

	fn is_zero(&self) -> bool {
		self.0.is_zero()
	}
}

impl One for Decimal {
	fn one() -> Self {
		Decimal(RustDecimal::one())
	}
}

impl Neg for Decimal {
	type Output = Self;

	fn neg(self) -> Self::Output {
		Decimal(self.0.neg())
	}
}

impl Signed for Decimal {
	fn abs(&self) -> Self {
		Decimal(self.0.abs())
	}

	fn abs_sub(&self, other: &Self) -> Self {
		Decimal(self.0.abs_sub(&other.0))
	}

	fn signum(&self) -> Self {
		Decimal(self.0.signum())
	}

	fn is_positive(&self) -> bool {
		self.0.is_sign_positive()
	}

	fn is_negative(&self) -> bool {
		self.0.is_sign_negative()
	}
}

impl Num for Decimal {
	type FromStrRadixErr = rust_decimal::Error;

	fn from_str_radix(src: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
		RustDecimal::from_str_radix(src, radix).map(|d| Decimal(d.normalize()))
	}
}

impl FromStr for Decimal {
	type Err = rust_decimal::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		RustDecimal::from_str(s).map(|d| Decimal(d.normalize()))
	}
}

impl From<i32> for Decimal {
	fn from(n: i32) -> Self {
		Decimal(RustDecimal::from(n))
	}
}

impl From<i64> for Decimal {
	fn from(n: i64) -> Self {
		Decimal(RustDecimal::from(n))
	}
}

impl From<u64> for Decimal {
	fn from(n: u64) -> Self {
		Decimal(RustDecimal::from(n))
	}
}

impl From<i128> for Decimal {
	fn from(n: i128) -> Self {
		Decimal(RustDecimal::from(n))
	}
}

impl From<u128> for Decimal {
	fn from(n: u128) -> Self {
		Decimal(RustDecimal::from(n))
	}
}

impl From<usize> for Decimal {
	fn from(value: usize) -> Self {
		Decimal(RustDecimal::from(value))
	}
}

impl TryFrom<f64> for Decimal {
	type Error = rust_decimal::Error;

	fn try_from(value: f64) -> Result<Self, Self::Error> {
		RustDecimal::try_from(value).map(|v| Decimal(v.normalize()))
	}
}

impl TryFrom<Decimal> for i64 {
	type Error = rust_decimal::Error;

	fn try_from(value: Decimal) -> Result<Self, Self::Error> {
		value.0.try_into()
	}
}

impl TryFrom<Decimal> for i128 {
	type Error = rust_decimal::Error;

	fn try_from(value: Decimal) -> Result<Self, Self::Error> {
		value.0.try_into()
	}
}

impl TryFrom<Decimal> for u64 {
	type Error = rust_decimal::Error;

	fn try_from(value: Decimal) -> Result<Self, Self::Error> {
		value.0.try_into()
	}
}

impl TryFrom<Decimal> for u128 {
	type Error = rust_decimal::Error;

	fn try_from(value: Decimal) -> Result<Self, Self::Error> {
		value.0.try_into()
	}
}

impl TryFrom<Decimal> for usize {
	type Error = rust_decimal::Error;

	fn try_from(value: Decimal) -> Result<Self, Self::Error> {
		value.0.try_into()
	}
}

impl TryFrom<Decimal> for f64 {
	type Error = rust_decimal::Error;

	fn try_from(value: Decimal) -> Result<Self, Self::Error> {
		value.0.try_into()
	}
}

impl Revisioned for Decimal {
	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), revision::Error> {
		self.0.serialize_revisioned(w)
	}

	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, revision::Error>
	where
		Self: Sized,
	{
		RustDecimal::deserialize_revisioned(r).map(Decimal)
	}

	fn revision() -> u16 {
		RustDecimal::revision()
	}
}
