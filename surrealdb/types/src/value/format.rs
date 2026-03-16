use core::fmt;
use std::fmt::{Display, Formatter};

use crate::sql::fmt_non_finite_f64;

/// Struct which implements surrealql formatting for floats
pub struct F(pub f64);

impl Display for F {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match fmt_non_finite_f64(self.0) {
			// Special case: Infinity, -Infinity or NaN
			Some(special) => f.write_str(special),
			// Regular float: add f to distinguish between int and float
			None => {
				self.0.fmt(f)?;
				f.write_str("f")
			}
		}
	}
}
