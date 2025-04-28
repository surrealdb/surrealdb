//! Decimal functionality and extension traits.

use rust_decimal::Decimal;

/// A trait to extend the Decimal type with additional functionality.
pub trait DecimalExt {
	/// Converts a string to a Decimal, normalizing it in the process.
	fn from_str_normalized(s: &str) -> Result<Self, rust_decimal::Error>
	where
		Self: Sized;
}

impl DecimalExt for Decimal {
	fn from_str_normalized(s: &str) -> Result<Decimal, rust_decimal::Error> {
		// #[allow(clippy::disallowed_methods)]
		Ok(Decimal::from_str_exact(s)?.normalize())
	}
}
