use core::fmt;
use std::fmt::{Display, Formatter};

/// Struct which implements surrealql formatting for floats
pub struct F(pub f64);

impl Display for F {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		if !self.0.is_finite() {
			if self.0.is_nan() {
				f.write_str("NaN")?;
			} else if self.0.is_sign_positive() {
				f.write_str("Infinity")?;
			} else {
				f.write_str("-Infinity")?;
			}
		}
		self.0.fmt(f)?;
		f.write_str("f")?;
		Ok(())
	}
}
