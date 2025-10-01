use std::iter::Sum;
use std::str::FromStr;
use std::{fmt, ops};

use anyhow::{Result, bail, ensure};

use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::val::{TryAdd, TrySub, Value};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Ord)]
pub struct Bytesize(pub u64);

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const TIB: u64 = GIB * 1024;
const PIB: u64 = TIB * 1024;

impl FromStr for Bytesize {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Bytesize::parse(s)
	}
}

impl Bytesize {
	pub const ZERO: Bytesize = Bytesize(0);
	pub const MAX: Bytesize = Bytesize(u64::MAX);

	pub fn new(b: u64) -> Self {
		Bytesize(b)
	}

	pub fn parse(input: &str) -> Result<Self> {
		let input = input.trim();
		ensure!(!input.is_empty(), Error::InvalidBytesize);

		let mut chars = input.as_bytes().iter().peekable();
		let mut total = Bytesize::new(0);

		while chars.peek().is_some() {
			// Parse number
			let mut value = 0u64;
			while let Some(&c) = chars.peek() {
				if !c.is_ascii_digit() {
					break;
				}

				chars.next();
				value = value.checked_mul(10).ok_or(Error::InvalidBytesize)?;
				value = value.checked_add((c - b'0') as u64).ok_or(Error::InvalidBytesize)?;
			}

			// Parse unit
			let unit = chars.next().ok_or(Error::InvalidBytesize)?.to_ascii_lowercase();

			// Handle optional 'b' suffix
			if unit != b'b' {
				match chars.next().map(|x| x.to_ascii_lowercase()) {
					Some(b'b') => (),
					_ => bail!(Error::InvalidBytesize),
				}
			}

			let bytesize = match unit {
				b'b' => Bytesize::b(value),
				b'k' => Bytesize::kb(value),
				b'm' => Bytesize::mb(value),
				b'g' => Bytesize::gb(value),
				b't' => Bytesize::tb(value),
				b'p' => Bytesize::pb(value),
				_ => bail!(Error::InvalidBytesize),
			};

			total = total.try_add(bytesize)?;
		}

		ensure!(total != Bytesize::new(0), Error::InvalidBytesize);

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
	fn try_add(self, other: Self) -> Result<Self> {
		self.0
			.checked_add(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map_err(anyhow::Error::new)
			.map(Bytesize::new)
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
	fn try_add(self, other: &'b Bytesize) -> Result<Bytesize> {
		self.0
			.checked_add(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map_err(anyhow::Error::new)
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
	fn try_sub(self, other: Self) -> Result<Self> {
		self.0
			.checked_sub(other.0)
			.ok_or_else(|| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map_err(anyhow::Error::new)
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
	fn try_sub(self, other: &'b Bytesize) -> Result<Bytesize> {
		self.0
			.checked_sub(other.0)
			.ok_or_else(|| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map_err(anyhow::Error::new)
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
