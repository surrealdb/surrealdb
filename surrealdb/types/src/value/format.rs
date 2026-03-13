use core::fmt;
use std::fmt::{Display, Formatter};

use crate::sql::fmt_non_finite_f64;

/// Struct which implements surrealql formatting for floats
pub struct F(pub f64);

impl Display for F {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		if !self.0.is_finite() {
			f.write_str(fmt_non_finite_f64(self.0))?
		} else {
			self.0.fmt(f)?;
			f.write_str("f")?;
		}
		Ok(())
	}
}
