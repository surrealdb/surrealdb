use crate::expr::Expr;
use crate::expr::field::Fields;
use std::fmt::{self, Display};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Output {
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}

impl Output {
	pub(crate) fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		if let Self::Fields(f) = self {
			f.visit(visitor);
		}
	}
}

impl Default for Output {
	fn default() -> Self {
		Self::None
	}
}

impl Display for Output {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("RETURN ")?;
		match self {
			Self::None => f.write_str("NONE"),
			Self::Null => f.write_str("NULL"),
			Self::Diff => f.write_str("DIFF"),
			Self::After => f.write_str("AFTER"),
			Self::Before => f.write_str("BEFORE"),
			Self::Fields(v) => Display::fmt(v, f),
		}
	}
}
