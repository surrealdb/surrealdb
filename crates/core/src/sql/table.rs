use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

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
