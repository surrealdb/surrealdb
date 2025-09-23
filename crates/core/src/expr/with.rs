use std::fmt::{Display, Formatter, Result};

use crate::fmt::{EscapeIdent, Fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl Display for With {
	fn fmt(&self, f: &mut Formatter) -> Result {
		f.write_str("WITH")?;
		match self {
			With::NoIndex => f.write_str(" NOINDEX"),
			With::Index(i) => {
				f.write_str(" INDEX ")?;
				Fmt::comma_separated(i.iter().map(EscapeIdent)).fmt(f)
			}
		}
	}
}
