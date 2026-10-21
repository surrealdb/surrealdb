//! Decimal helpers shared by the parser and the value layer.

use std::str::FromStr;

use rust_decimal::Decimal;

/// A trait to extend the Decimal type with additional functionality.
pub trait DecimalExt {
	/// Converts a string to a Decimal, normalizing it in the process.
	///
	/// This method is a convenience wrapper around
	/// `rust_decimal::Decimal::from_str` which can parse a string into a
	/// Decimal and normalize it. If the value has higher precision than the
	/// Decimal type can handle, it will be rounded to the
	/// nearest representable value.
	fn from_str_normalized(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;

	/// Converts a string to a Decimal only if the value is representable
	/// exactly: a scale beyond the type's maximum precision, or a magnitude
	/// beyond its range, is an error rather than a rounded value. The result
	/// keeps the written scale.
	fn from_str_exact_scale(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;

	/// [`DecimalExt::from_str_exact_scale`], normalized.
	fn from_str_normalized_exact(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;
}

impl DecimalExt for Decimal {
	fn from_str_normalized(s: &str) -> Result<Decimal, rust_decimal::Error> {
		#[allow(clippy::disallowed_methods)]
		Ok(Decimal::from_str(s)?.normalize())
	}

	fn from_str_exact_scale(s: &str) -> Result<Decimal, rust_decimal::Error> {
		#[allow(clippy::disallowed_methods)]
		Decimal::from_str_exact(s)
	}

	fn from_str_normalized_exact(s: &str) -> Result<Decimal, rust_decimal::Error> {
		Ok(Self::from_str_exact_scale(s)?.normalize())
	}
}
