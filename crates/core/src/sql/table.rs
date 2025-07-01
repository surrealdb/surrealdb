use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::sql::escape::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Ord)]
pub struct Table(pub String);

impl From<Table> for crate::expr::Table {
	fn from(v: Table) -> Self {
		crate::expr::Table(v.0)
	}
}

impl From<crate::expr::Table> for Table {
	fn from(v: crate::expr::Table) -> Self {
		Self(v.0)
	}
}

impl fmt::Display for Table {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		EscapeIdent(&self.0).fmt(f)
	}
}
