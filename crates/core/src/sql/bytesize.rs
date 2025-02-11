use crate::err::Error;
use crate::sql::statements::info::InfoStructure;
use crate::sql::Value;
use num_traits::CheckedAdd;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::{Peekable, Sum};
use std::ops;
use std::str::{Chars, FromStr};

use super::value::{TryAdd, TrySub};
use super::Strand;

#[revisioned(revision = 1)]
#[derive(
	Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Ord,
)]
#[serde(rename = "$surrealdb::private::sql::Bytesize")]
#[non_exhaustive]
pub struct Bytesize(pub u64);

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const TIB: u64 = GIB * 1024;
const PIB: u64 = TIB * 1024;

impl FromStr for Bytesize {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Bytesize {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Bytesize {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Bytesize {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match Bytesize::parse(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl Bytesize {
	pub const ZERO: Bytesize = Bytesize(0);
	pub const MAX: Bytesize = Bytesize(u64::MAX);

	pub fn new(b: u64) -> Self {
		Bytesize(b)
	}

	pub fn parse(input: impl Into<String>) -> Result<Self, Error> {
		let input = input.into().trim().to_lowercase();
		if input.is_empty() {
			return Err(Error::InvalidBytesize);
		}

		let mut chars: Peekable<Chars> = input.chars().peekable();
		let mut total = Bytesize::new(0);

		while chars.peek().is_some() {
			// Parse number
			let mut number = String::new();
			while let Some(&c) = chars.peek() {
				if !c.is_ascii_digit() {
					break;
				}
				number.push(
					chars
						.next()
						.ok_or(Error::Unreachable("Char was previously peekable".into()))?,
				);
			}

			let value = number.parse::<u64>().map_err(|_| Error::InvalidBytesize)?;

			// Parse unit
			let unit = chars.next().ok_or(Error::InvalidBytesize)?.to_ascii_lowercase();

			// Handle optional 'b' suffix
			if unit != 'b' {
				match chars.next() {
					Some('b') => (),
					_ => return Err(Error::InvalidBytesize),
				}
			}

			let bytesize = match unit {
				'b' => Bytesize::b(value),
				'k' => Bytesize::kb(value),
				'm' => Bytesize::mb(value),
				'g' => Bytesize::gb(value),
				't' => Bytesize::tb(value),
				'p' => Bytesize::pb(value),
				_ => return Err(Error::InvalidBytesize),
			};

			total = total.try_add(bytesize)?;
		}

		if total == Bytesize::new(0) {
			return Err(Error::InvalidBytesize);
		}

		Ok(total)
	}

	pub fn b(b: u64) -> Self {
		Bytesize(b)
	}

	pub fn kb(kb: u64) -> Self {
		Bytesize(kb * KIB)
	}

	pub fn mb(mb: u64) -> Self {
		Bytesize(mb * MIB)
	}

	pub fn gb(gb: u64) -> Self {
		Bytesize(gb * GIB)
	}

	pub fn tb(tb: u64) -> Self {
		Bytesize(tb * TIB)
	}

	pub fn pb(pb: u64) -> Self {
		Bytesize(pb * PIB)
	}
}

impl fmt::Display for Bytesize {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let b = self.0;
		let pb = b / PIB;
		let b = b % PIB;
		let tb = b / TIB;
		let b = b % TIB;
		let gb = b / GIB;
		let b = b % GIB;
		let mb = b / MIB;
		let b = b % MIB;
		let kb = b / KIB;
		let b = b % KIB;

		if pb > 0 {
			write!(f, "{pb}pb")?;
		}
		if tb > 0 {
			write!(f, "{tb}tb")?;
		}
		if gb > 0 {
			write!(f, "{gb}gb")?;
		}
		if mb > 0 {
			write!(f, "{mb}mb")?;
		}
		if kb > 0 {
			write!(f, "{kb}kb")?;
		}
		if b > 0 {
			write!(f, "{b}b")?;
		}
		Ok(())
	}
}

impl ops::Add for Bytesize {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		// checked to make sure it doesn't overflow
		match self.0.checked_add(other.0) {
			Some(v) => Bytesize::new(v),
			None => Bytesize::new(u64::MAX),
		}
	}
}

impl TryAdd for Bytesize {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self, Error> {
		self.0
			.checked_add(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map(Bytesize::new)
	}
}

impl CheckedAdd for Bytesize {
	fn checked_add(&self, other: &Self) -> Option<Self> {
		self.0.checked_add(other.0).map(Bytesize::new)
	}
}

impl<'b> ops::Add<&'b Bytesize> for &Bytesize {
	type Output = Bytesize;
	fn add(self, other: &'b Bytesize) -> Bytesize {
		match self.0.checked_add(other.0) {
			Some(v) => Bytesize::new(v),
			None => Bytesize::new(u64::MAX),
		}
	}
}

impl<'b> TryAdd<&'b Bytesize> for &Bytesize {
	type Output = Bytesize;
	fn try_add(self, other: &'b Bytesize) -> Result<Bytesize, Error> {
		self.0
			.checked_add(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map(Bytesize::new)
	}
}

impl ops::Sub for Bytesize {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		match self.0.checked_sub(other.0) {
			Some(v) => Bytesize::new(v),
			None => Bytesize::default(),
		}
	}
}

impl TrySub for Bytesize {
	type Output = Self;
	fn try_sub(self, other: Self) -> Result<Self, Error> {
		self.0
			.checked_sub(other.0)
			.ok_or_else(|| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map(Bytesize::new)
	}
}

impl<'b> ops::Sub<&'b Bytesize> for &Bytesize {
	type Output = Bytesize;
	fn sub(self, other: &'b Bytesize) -> Bytesize {
		match self.0.checked_sub(other.0) {
			Some(v) => Bytesize::new(v),
			None => Bytesize::default(),
		}
	}
}

impl<'b> TrySub<&'b Bytesize> for &Bytesize {
	type Output = Bytesize;
	fn try_sub(self, other: &'b Bytesize) -> Result<Bytesize, Error> {
		self.0
			.checked_sub(other.0)
			.ok_or_else(|| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map(Bytesize::new)
	}
}

impl Sum<Self> for Bytesize {
	fn sum<I>(iter: I) -> Bytesize
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(Bytesize::default(), |a, b| a + b)
	}
}

impl<'a> Sum<&'a Self> for Bytesize {
	fn sum<I>(iter: I) -> Bytesize
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(Bytesize::default(), |a, b| &a + b)
	}
}

impl InfoStructure for Bytesize {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[cfg(test)]
mod tests {
	use super::Bytesize;

	#[test]
	fn parse_bytesize() {
		let str = "1tb8mb2b";
		let bytesize = Bytesize::parse(str).unwrap();
		assert_eq!(bytesize, Bytesize::new(1_099_520_016_386));
		assert_eq!(str, bytesize.to_string());
	}
}
