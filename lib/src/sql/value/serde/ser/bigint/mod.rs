use crate::err::Error;
use crate::sql::value::serde::ser;
use revision::Error as RevisionError;
use revision::Revisioned;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::{
	Deserialize, 
	Serialize, 
	Deserializer, 
	Serializer as SerdeSerializer, 
};
use serde::de::{self, Visitor};
use std::fmt::{Display, Formatter};
use std::iter::Product;
use std::iter::Sum;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;
use rust_decimal::prelude::*;

pub(super) struct Serializer;

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq, Hash)]
pub struct I256(alloy_primitives::I256);

impl Serialize for I256 {
	fn serialize<S: SerdeSerializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut hex = self.0.to_hex_string();
		if hex.starts_with('-') {
			hex = "-0x".to_owned() + hex[3..].trim_start_matches('0');
		} else {
			hex = "0x".to_owned() + hex[2..].trim_start_matches('0');
		}
		serializer.serialize_str(hex.as_str())
	}
}

impl <'de> Deserialize<'de> for I256 {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(I256Visitor)
	}
}

struct I256Visitor;

impl<'de> Visitor<'de> for I256Visitor {
	type Value = I256;
	
	fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
		formatter.write_str("I256")
	}

	fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
	where
		E: de::Error
	{
		match alloy_primitives::I256::from_str(v) {
			Ok(v) => Ok(I256(v)),
			Err(_) => Err(de::Error::custom("I256")),
		}
	}
}

macro_rules! impl_prim_conversions {
	($($int: ty),*) => {
		$(
			impl From<$int> for I256 {
				fn from(i: $int) -> Self {
					Self(alloy_primitives::I256::try_from(i).unwrap())
				}
			}
		)*
	};
}

impl_prim_conversions!(i8, i16, i32, i64, isize, u8, u16, u32, u64);

impl From<alloy_primitives::I256> for I256 {
	fn from(v: alloy_primitives::I256) -> Self {
		Self(v)
	}
}

impl From<usize> for I256 {
	fn from(v: usize)-> Self {
		Self(alloy_primitives::I256::from_str(v.to_string().as_str()).unwrap())
	}
}

impl From<i128> for I256 {
	fn from(v: i128)-> Self {
		Self(alloy_primitives::I256::from_str(v.to_string().as_str()).unwrap())
	}
}

impl From<u128> for I256 {
	fn from(v: u128)-> Self {
		Self(alloy_primitives::I256::from_str(v.to_string().as_str()).unwrap())
	}
}

impl TryFrom<f64> for I256 {
	// todo: [zyre] add support for f64
	type Error = Error;
	fn try_from(v: f64) ->  Result<Self, Self::Error> {
		Err(Error::TryFrom(v.to_string(), "I256"))
	}
}

impl TryFrom<Decimal> for I256 {
	// todo: [zyre] properly handle conversions
	type Error = Error;
	fn try_from(v: Decimal) ->  Result<Self, Self::Error> {
		match v.to_i128() {
			Some(v) => Ok(I256::from(v)),
			None => Err(Error::TryFrom(v.to_string(), "I256")),
		}
	}
}

impl TryFrom<&str> for I256 {
	// todo: [zyre] properly handle conversions
	type Error = Error;
	fn try_from(v: &str) ->  Result<Self, Self::Error> {
		info!("TryFrom<&str> I256: {}", v);
		match I256::from_str(v) {
			Ok(v) => Ok(v),
			Err(_) => Err(Error::TryFrom(v.to_string(), "I256")),
		}
	}
}

impl TryFrom<String> for I256 {
	// todo: [zyre] properly handle conversions
	type Error = Error;
	fn try_from(v: String) ->  Result<Self, Self::Error> {
		info!("TryFrom<String> I256: {}", v);
		match I256::from_str(v.as_str()) {
			Ok(v) => Ok(v),
			Err(_) => Err(Error::TryFrom(v.to_string(), "I256")),
		}
	}
}


impl TryFrom<&[u8]> for I256 {
	type Error = Error;
	fn try_from(v: &[u8]) ->  Result<Self, Self::Error> {
		
		let s = String::from_utf8_lossy(v);
		info!("TryFrom<&[u8]> I256: {}", s);
		match I256::from_str(s.as_ref()) {
			Ok(v) => Ok(v),
			Err(_) => Err(Error::TryFrom(s.to_string(), "I256")),
		}
	}
}


impl I256 {
	// Satisfy `try_into_prim` macro
	#[inline]
	pub fn to_i8(self) -> Option<i8> {
		Option::from(self.0.as_i8())
	}
	#[inline]
	pub fn to_i16(self) -> Option<i16> {
		Option::from(self.0.as_i16())
	}
	#[inline]
	pub fn to_i32(self) -> Option<i32> {
		Option::from(self.0.as_i32())
	}
	#[inline]
	pub fn to_i64(self) -> Option<i64> {
		Option::from(self.0.as_i64())
	}
	#[inline]
	pub fn to_i128(self) -> Option<i128> {
		None
	}
	#[inline]
	pub fn to_u8(self) -> Option<u8> {
		Option::from(self.0.as_u8())
	}
	#[inline]
	pub fn to_u16(self) -> Option<u16> {
		Option::from(self.0.as_u16())
	}
	#[inline]
	pub fn to_u32(self) -> Option<u32> {
		Option::from(self.0.as_u32())
	}
	#[inline]
	pub fn to_u64(self) -> Option<u64> {
		Option::from(self.0.as_u64())
	}
	#[inline]
	pub fn to_u128(self) -> Option<u128> {
		None
	}
	#[inline]
	pub fn to_f32(self) -> Option<f32> {
		Option::from(self.0.as_i32() as f32)
	}
	#[inline]
	pub fn to_f64(self) -> Option<f64> {
		Option::from(self.0.as_i64() as f64)
	}
	#[inline]
	pub fn to_usize(self) -> Option<usize> {
		Option::from(self.0.as_usize())
	}

	pub fn from_str(s: &str) -> Result<Self, alloy_primitives::ParseSignedError> {
		let v = alloy_primitives::I256::from_str(s)?;
		Ok(I256(v))
	}

	// Forward arithmetic operations
	#[inline]
	pub fn is_zero(&self) -> bool {
		self.0.is_zero()
	}
	#[inline]
	pub fn is_negative(&self) -> bool {
		self.0.is_negative()
	}
	#[inline]
	pub fn is_positive(&self) -> bool {
		self.0.is_positive()
	}
	#[inline]
	pub fn abs(&self) -> Self {
		I256(self.0.abs())
	}
	#[inline]
	pub fn pow(&self, exp: u32) -> Self {
		I256(self.0.pow(alloy_primitives::Uint::from(exp)))
	}
	#[inline]
	pub fn cmp(&self, other: Self) -> std::cmp::Ordering {
		self.0.cmp(&other.0)
	}
	#[inline]
	pub fn eq(&self, other: &Self) -> bool {
		self.0.eq(&other.0)
	}
	#[inline]
	pub fn is_zero_or_positive(&self) -> bool {
		self.0.is_zero() || self.0.is_positive()
	}
	#[inline]
	pub fn is_zero_or_negative(&self) -> bool {
		self.0.is_zero() || self.0.is_negative()
	}
	#[inline]
	pub fn zero() -> Self {
		I256(alloy_primitives::I256::ZERO)
	}
  #[inline]
  pub fn one() -> Self {
    I256(alloy_primitives::I256::ONE)
  }

	// checked arithmetic
	pub fn checked_add(self, rhs: Self) -> Option<Self> {
		self.0.checked_add(rhs.0).map(I256)
	}

	pub fn checked_sub(self, rhs: Self) -> Option<Self> {
		self.0.checked_sub(rhs.0).map(I256)
	}

	pub fn checked_mul(self, rhs: Self) -> Option<Self> {
		self.0.checked_mul(rhs.0).map(I256)
	}

	pub fn checked_div(self, rhs: Self) -> Option<Self> {
		self.0.checked_div(rhs.0).map(I256)
	}

	pub fn checked_rem(self, rhs: Self) -> Option<Self> {
		self.0.checked_rem(rhs.0).map(I256)
	}
}

impl Neg for I256 {
	type Output = Self;
	#[inline]
	fn neg(self) -> Self {
		self.0.overflowing_neg().0.into()
	}
}

impl Add<Self> for I256 {
	type Output = Self;
	#[inline]
	fn add(self, rhs: Self) -> Self {
		self.0.overflowing_add(rhs.0).0.into()
	}
}

impl <'a, 'b> Add<&'b I256> for &'a I256 {
	type Output = I256;
	#[inline]
	fn add(self, rhs: &'b I256) -> I256 {
		self.0.overflowing_add(rhs.0).0.into()
	}
}

impl Sub<Self> for I256 {
	type Output = Self;
	#[inline]
	fn sub(self, rhs: Self) -> Self {
		self.0.overflowing_sub(rhs.0).0.into()
	}
}

impl <'a, 'b> Sub<&'b I256> for &'a I256 {
	type Output = I256;
	#[inline]
	fn sub(self, rhs: &'b I256) -> I256 {
		self.0.overflowing_sub(rhs.0).0.into()
	}
}

impl Mul<Self> for I256 {
	type Output = Self;
	#[inline]
	fn mul(self, rhs: Self) -> Self {
		self.0.mul(rhs.0).into()
	}
}

impl <'a, 'b> Mul<&'b I256> for &'a I256 {
	type Output = I256;
	#[inline]
	fn mul(self, rhs: &'b I256) -> I256 {
		self.0.mul(rhs.0).into()
	}
}

impl Div<Self> for I256 {
	type Output = Self;
	#[inline]
	fn div(self, rhs: Self) -> Self {
		self.0.div(rhs.0).into()
	}
}

impl <'a, 'b> Div<&'b I256> for &'a I256 {
	type Output = I256;
	#[inline]
	fn div(self, rhs: &'b I256) -> I256 {
		self.0.div(rhs.0).into()
	}
}

impl Rem<Self> for I256 {
	type Output = Self;
  #[inline]
	fn rem(self, rhs: Self) -> Self {
		self.0.rem(rhs.0).into()
	}
}

impl Sum<Self> for I256 {
	fn sum<I>(iter: I) -> I256
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(I256::zero(), |acc, x| acc + x)
	}
}

impl <'a> Sum<&'a Self> for I256 {
	fn sum<I>(iter: I) -> I256
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(I256::zero(), |acc, x| acc + *x)
	}
}

impl Product<Self> for I256 {
	fn product<I>(iter: I) -> Self
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(I256::one(), |acc, x| acc * x)
	}
}

impl <'a> Product<&'a Self> for I256 {
	fn product<I>(iter: I) -> I256
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(I256::one(), |acc, x| acc * *x)
	}
}

impl Display for I256 {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}

fn unsafe_u64_to_u8_slice(slice: &[u64]) -> &[u8] {
	unsafe {
			std::slice::from_raw_parts(
					slice.as_ptr() as *const u8,
					slice.len() * std::mem::size_of::<u64>(),
			)
	}
}

// fn u64_to_u8_slice(slice: &[u64]) -> &[u8] {
// 		let mut bytes = [0u8; 32];
// 		for (i, limb) in slice.iter().enumerate() {
// 			let lbytes = limb.to_le_bytes();
// 			for (j, b) in lbytes.iter().enumerate() {
// 				bytes[(i*8)+j] = *b;
// 			}
// 		}
// 		&bytes
// }

impl Revisioned for I256 {
	fn revision() -> u16 {
		1
	}
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), RevisionError> {
		let limbs = self.0.as_limbs();
		let bytes = unsafe_u64_to_u8_slice(limbs);
		w.write_all(bytes)
		.map_err(|e| RevisionError::Io(e.raw_os_error().unwrap_or(0)))
	}
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, RevisionError> {
		let mut v = [0u8; 32];
		r
			.read_exact(v.as_mut_slice())
			.map_err(|e| RevisionError::Io(e.raw_os_error().unwrap_or(0)))?;
		Ok(I256(alloy_primitives::I256::from_le_bytes(v)))
	}
}

impl ser::Serializer for Serializer {
	type Ok = I256;
	type Error = Error;

	type SerializeSeq = Impossible<I256, Error>;
	type SerializeTuple = Impossible<I256, Error>;
	type SerializeTupleStruct = Impossible<I256, Error>;
	type SerializeTupleVariant = Impossible<I256, Error>;
	type SerializeMap = Impossible<I256, Error>;
	type SerializeStruct = Impossible<I256, Error>;
	type SerializeStructVariant = Impossible<I256, Error>;

	const EXPECTED: &'static str = "a struct `I256`";

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Self::Ok, Error> {
		I256::from_str(value).map_err(Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn u256() {
		let number = I256::default();
		let serialized = Serialize::serialize(&number, Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}
}
